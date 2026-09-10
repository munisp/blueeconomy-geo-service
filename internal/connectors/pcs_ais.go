package connectors

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/munisp/blueeconomy-geo-service/internal/sign"
	"github.com/munisp/blueeconomy-geo-service/internal/store"
)

// PCSImporter closes the AIS silo (G2): it consumes validated AIS positions
// from the port-interoperability pcs_ais_positions plane (migration 0024)
// and feeds them through the SAME decode→validate→dedup→store→evaluate→
// publish pipeline as every other connector — no parallel ingestion path,
// no schema drift. The source rows carry double-precision degrees; they are
// converted once at this boundary into the fixed-point micro-degree
// integers the geo plane stores (round-half-away-from-zero).
//
// Config-gated and fail-closed: GEO_PCS_AIS_IMPORT_DSN unset disables the
// importer entirely (capabilities report configured:false); set but
// unreachable/invalid aborts startup like every other connector.
// Ingestion is idempotent: the watermark is the greatest consumed
// message_ts, and the pipeline dedup window absorbs replays inside it.
type PCSImporter struct {
	DSN          string
	PollInterval time.Duration
	// BatchSize bounds one poll (default 1000, max 1000).
	BatchSize int
	Pipeline  *Pipeline
	Logger    *log.Logger
	Metrics   interface {
		Inc(name string, labels map[string]string)
	}
}

// pcsAISRow mirrors db/migrations/0024_pcs_ais_positions.sql (port-interop).
type pcsAISRow struct {
	MMSI          string
	IMO           *string
	Latitude      float64
	Longitude     float64
	SpeedKnots    float64
	CourseDegrees float64
	Heading       *int32
	MessageTS     time.Time
}

// microsFromDegrees converts double degrees to fixed-point micro-degrees
// (round to nearest, ties to even).
func microsFromDegrees(degrees float64) int32 {
	return int32(math.RoundToEven(degrees * 1e6))
}

// milliknotsFromKnots converts knots to milli-knots.
func milliknotsFromKnots(knots float64) uint32 {
	return uint32(math.RoundToEven(knots * 1000))
}

// millidegreesFromDegrees converts degrees to milli-degrees.
func millidegreesFromDegrees(degrees float64) uint32 {
	return uint32(math.RoundToEven(degrees * 1000))
}

// Validate fails closed on an enabled-but-misconfigured importer.
func (importer *PCSImporter) Validate() error {
	if importer.DSN == "" {
		return errors.New("pcs ais importer DSN is required")
	}
	if importer.Pipeline == nil {
		return errors.New("pcs ais importer pipeline is required")
	}
	if importer.PollInterval <= 0 {
		return errors.New("pcs ais importer poll interval must be positive")
	}
	return nil
}

// Run polls until ctx is cancelled. The first poll starts from
// now()-5min (the PCS feed is near-realtime; historical backfill is a
// deliberate operator action via replay, not an implicit full-table scan).
func (importer *PCSImporter) Run(ctx context.Context) error {
	if err := importer.Validate(); err != nil {
		return err
	}
	pool, err := pgxpool.New(ctx, importer.DSN)
	if err != nil {
		return fmt.Errorf("pcs ais connect: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("pcs ais ping: %w", err)
	}
	batch := importer.BatchSize
	if batch <= 0 || batch > 1000 {
		batch = 1000
	}
	logger := importer.Logger
	if logger == nil {
		logger = log.Default()
	}
	logger.Printf("pcs-ais importer: polling every %s (batch %d)", importer.PollInterval, batch)
	watermark := time.Now().UTC().Add(-5 * time.Minute)
	ticker := time.NewTicker(importer.PollInterval)
	defer ticker.Stop()
	for {
		advanced, err := importer.poll(ctx, pool, watermark, batch)
		if err != nil {
			if importer.Metrics != nil {
				importer.Metrics.Inc("geo_pcs_ais_import_errors_total", nil)
			}
			logger.Printf("pcs-ais importer poll: %v", err)
		} else if !advanced.IsZero() {
			watermark = advanced
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// poll consumes one batch at-or-after the watermark and returns the new
// watermark (zero when no rows were consumed).
func (importer *PCSImporter) poll(ctx context.Context, pool *pgxpool.Pool, watermark time.Time, batch int) (time.Time, error) {
	rows, err := pool.Query(ctx, `SELECT mmsi, imo, latitude, longitude, speed_knots,
		course_degrees, heading, message_ts
		FROM pcs_ais_positions WHERE message_ts > $1
		ORDER BY message_ts ASC LIMIT $2`, watermark, batch)
	if err != nil {
		return time.Time{}, fmt.Errorf("pcs ais query: %w", err)
	}
	defer rows.Close()
	batchRows := make([]pcsAISRow, 0, batch)
	for rows.Next() {
		var row pcsAISRow
		if err := rows.Scan(&row.MMSI, &row.IMO, &row.Latitude, &row.Longitude,
			&row.SpeedKnots, &row.CourseDegrees, &row.Heading, &row.MessageTS); err != nil {
			return time.Time{}, fmt.Errorf("pcs ais scan: %w", err)
		}
		batchRows = append(batchRows, row)
	}
	if err := rows.Err(); err != nil {
		return time.Time{}, err
	}
	for _, row := range batchRows {
		sog := milliknotsFromKnots(row.SpeedKnots)
		cog := millidegreesFromDegrees(row.CourseDegrees)
		position := store.Position{
			MMSI:                         row.MMSI,
			SourceClass:                  sign.SourceAIS,
			LatitudeMicros:               microsFromDegrees(row.Latitude),
			LongitudeMicros:              microsFromDegrees(row.Longitude),
			SpeedOverGroundMilliknots:    &sog,
			CourseOverGroundMillidegrees: &cog,
			ReceiverID:                   "pcs-ais-import",
			ObservedAt:                   row.MessageTS.UTC(),
			Classification:               "INTERNAL",
		}
		if row.IMO != nil {
			position.IMO = *row.IMO
		}
		// Dedup payload key ties to the PCS idempotency key (mmsi, message_ts)
		// so feed replays are absorbed by the pipeline dedup window.
		if err := importer.Pipeline.HandlePosition(ctx, IngestPosition{
			Position:   position,
			PayloadKey: fmt.Sprintf("pcs-ais:%s:%d", row.MMSI, row.MessageTS.UTC().Unix()),
		}); err != nil {
			return time.Time{}, fmt.Errorf("pcs ais ingest mmsi %s: %w", row.MMSI, err)
		}
		if importer.Metrics != nil {
			importer.Metrics.Inc("geo_pcs_ais_imported_total", nil)
		}
	}
	if len(batchRows) == 0 {
		return time.Time{}, nil
	}
	return batchRows[len(batchRows)-1].MessageTS.UTC(), nil
}
