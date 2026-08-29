// Package store is the PostGIS writer/reader of the hot path: batched
// position inserts into the daily RANGE-partitioned ais_positions table,
// latest_positions upserts, SCD-2 vessels_static upserts, geofence zone
// administration and evaluation, and the REST read model. All coordinates
// and speeds are stored as fixed-point integers per the geo.*.v1 contracts;
// the geography columns are derived from the integer columns at the storage
// boundary (integer-decimal rendering, no float round-trip).
package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store wraps the Postgres pools: the tenant-scoped application pool and
// the dedicated ingest pool (geo_ingest role) used solely by the platform-
// wide geofence evaluator. The two are separate CONNECTIONS, not a SET
// ROLE — permissive RLS policies OR across role memberships, so the app
// role must never hold geo_ingest membership (0008_rls_ingest_login.sql).
type Store struct {
	pool   *pgxpool.Pool
	ingest *pgxpool.Pool
}

// New connects and verifies both pools; it fails closed when either the
// application database or the ingest role connection is unreachable at
// startup. ingestDSN authenticates as the geo_ingest role (least
// privilege: SELECT geofence_zones + INSERT geofence_events only).
func New(ctx context.Context, dsn, ingestDSN string) (*Store, error) {
	if strings.TrimSpace(dsn) == "" {
		return nil, errors.New("postgres DSN is required")
	}
	if strings.TrimSpace(ingestDSN) == "" {
		return nil, errors.New("ingest postgres DSN (geo_ingest role) is required")
	}
	pool, err := connectPool(ctx, dsn, "postgres")
	if err != nil {
		return nil, err
	}
	ingest, err := connectPool(ctx, ingestDSN, "ingest postgres")
	if err != nil {
		pool.Close()
		return nil, err
	}
	return &Store{pool: pool, ingest: ingest}, nil
}

func connectPool(ctx context.Context, dsn, label string) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse %s DSN: %w", label, err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("connect %s: %w", label, err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping %s: %w", label, err)
	}
	return pool, nil
}

// Close releases both pools.
func (store *Store) Close() {
	store.pool.Close()
	store.ingest.Close()
}

// Pool exposes the pool for the migration runner and integration tests.
func (store *Store) Pool() *pgxpool.Pool {
	return store.pool
}

// Position is one validated fixed-point position report to persist.
type Position struct {
	PositionReportID             string
	MMSI                         string // empty only for APP_REPORT
	VesselRef                    string
	SourceClass                  string
	LatitudeMicros               int32
	LongitudeMicros              int32
	SpeedOverGroundMilliknots    *uint32
	CourseOverGroundMillidegrees *uint32
	HeadingMillidegrees          *uint32
	NavStatus                    *int32
	PositionAccuracy             string
	ReceiverID                   string
	AISMessageType               *int32
	IMO                          string
	Callsign                     string
	ShipName                     string
	Classification               string
	ObservedAt                   time.Time
}

// renderMicros renders a micro-degree fixed-point value as an exact decimal
// string (sign, integer part, 6-digit fraction) for PostGIS WKT. No binary
// floating point is involved.
func renderMicros(micros int32) string {
	negative := micros < 0
	absolute := int64(micros)
	if negative {
		absolute = -absolute
	}
	whole := absolute / 1_000_000
	fraction := absolute % 1_000_000
	text := fmt.Sprintf("%d.%06d", whole, fraction)
	if negative {
		return "-" + text
	}
	return text
}

// pointWKT renders the WGS84 point (longitude first) for ST_GeogFromText.
func pointWKT(latitudeMicros, longitudeMicros int32) string {
	return "POINT(" + renderMicros(longitudeMicros) + " " + renderMicros(latitudeMicros) + ")"
}

// InsertPositions batch-inserts validated positions (500–1000 rows per
// statement) into the partitioned ais_positions table. The geography column
// is derived from the fixed-point columns via ST_GeogFromText.
func (store *Store) InsertPositions(ctx context.Context, positions []Position) error {
	if len(positions) == 0 {
		return nil
	}
	if len(positions) > 1000 {
		return fmt.Errorf("position batch %d exceeds the 1000-row insert bound", len(positions))
	}
	var builder strings.Builder
	builder.WriteString(`INSERT INTO ais_positions (
		position_report_id, mmsi, vessel_ref, source_class, geom,
		latitude_micros, longitude_micros,
		speed_over_ground_milliknots, course_over_ground_millidegrees, heading_millidegrees,
		nav_status, position_accuracy, receiver_id, ais_message_type,
		imo, callsign, ship_name, classification, observed_at) VALUES `)
	args := make([]any, 0, len(positions)*19)
	for i, position := range positions {
		if i > 0 {
			builder.WriteString(", ")
		}
		base := i*19 + 1
		fmt.Fprintf(&builder, "($%d, $%d, $%d, $%d, ST_GeogFromText($%d), $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
			base, base+1, base+2, base+3, base+4, base+5, base+6, base+7, base+8, base+9,
			base+10, base+11, base+12, base+13, base+14, base+15, base+16, base+17, base+18)
		var mmsi, vesselRef *string
		if position.MMSI != "" {
			mmsi = &position.MMSI
		}
		if position.VesselRef != "" {
			vesselRef = &position.VesselRef
		}
		accuracy := position.PositionAccuracy
		if accuracy == "" {
			accuracy = "UNSPECIFIED"
		}
		args = append(args,
			position.PositionReportID, mmsi, vesselRef, position.SourceClass,
			pointWKT(position.LatitudeMicros, position.LongitudeMicros),
			position.LatitudeMicros, position.LongitudeMicros,
			position.SpeedOverGroundMilliknots, position.CourseOverGroundMillidegrees, position.HeadingMillidegrees,
			position.NavStatus, accuracy, position.ReceiverID, position.AISMessageType,
			position.IMO, position.Callsign, position.ShipName, position.Classification, position.ObservedAt.UTC())
	}
	if _, err := store.pool.Exec(ctx, builder.String(), args...); err != nil {
		return fmt.Errorf("insert %d positions: %w", len(positions), err)
	}
	return nil
}

// UpsertLatestPosition maintains the single hot row per vessel identity
// (mmsi or vessel_ref), moving only forward in observed time.
func (store *Store) UpsertLatestPosition(ctx context.Context, position Position) error {
	accuracy := position.PositionAccuracy
	if accuracy == "" {
		accuracy = "UNSPECIFIED"
	}
	wkt := pointWKT(position.LatitudeMicros, position.LongitudeMicros)
	if position.MMSI != "" {
		_, err := store.pool.Exec(ctx, `INSERT INTO latest_positions (
			mmsi, position_report_id, source_class, geom, latitude_micros, longitude_micros,
			speed_over_ground_milliknots, course_over_ground_millidegrees, heading_millidegrees,
			nav_status, classification, observed_at)
			VALUES ($1, $2, $3, ST_GeogFromText($4), $5, $6, $7, $8, $9, $10, $11, $12)
			ON CONFLICT (mmsi) WHERE mmsi IS NOT NULL DO UPDATE SET
				position_report_id = EXCLUDED.position_report_id,
				source_class = EXCLUDED.source_class,
				geom = EXCLUDED.geom,
				latitude_micros = EXCLUDED.latitude_micros,
				longitude_micros = EXCLUDED.longitude_micros,
				speed_over_ground_milliknots = EXCLUDED.speed_over_ground_milliknots,
				course_over_ground_millidegrees = EXCLUDED.course_over_ground_millidegrees,
				heading_millidegrees = EXCLUDED.heading_millidegrees,
				nav_status = EXCLUDED.nav_status,
				classification = EXCLUDED.classification,
				observed_at = EXCLUDED.observed_at,
				ingested_at = now()
			WHERE EXCLUDED.observed_at > latest_positions.observed_at`,
			position.MMSI, position.PositionReportID, position.SourceClass, wkt,
			position.LatitudeMicros, position.LongitudeMicros,
			position.SpeedOverGroundMilliknots, position.CourseOverGroundMillidegrees, position.HeadingMillidegrees,
			position.NavStatus, position.Classification, position.ObservedAt.UTC())
		if err != nil {
			return fmt.Errorf("upsert latest position mmsi %s: %w", position.MMSI, err)
		}
		return nil
	}
	if position.VesselRef == "" {
		return errors.New("latest position upsert requires mmsi or vessel_ref")
	}
	_, err := store.pool.Exec(ctx, `INSERT INTO latest_positions (
		vessel_ref, position_report_id, source_class, geom, latitude_micros, longitude_micros,
		speed_over_ground_milliknots, course_over_ground_millidegrees, heading_millidegrees,
		nav_status, classification, observed_at)
		VALUES ($1, $2, $3, ST_GeogFromText($4), $5, $6, $7, $8, $9, $10, $11, $12)
		ON CONFLICT (vessel_ref) WHERE vessel_ref IS NOT NULL DO UPDATE SET
			position_report_id = EXCLUDED.position_report_id,
			source_class = EXCLUDED.source_class,
			geom = EXCLUDED.geom,
			latitude_micros = EXCLUDED.latitude_micros,
			longitude_micros = EXCLUDED.longitude_micros,
			speed_over_ground_milliknots = EXCLUDED.speed_over_ground_milliknots,
			course_over_ground_millidegrees = EXCLUDED.course_over_ground_millidegrees,
			heading_millidegrees = EXCLUDED.heading_millidegrees,
			nav_status = EXCLUDED.nav_status,
			classification = EXCLUDED.classification,
			observed_at = EXCLUDED.observed_at,
			ingested_at = now()
		WHERE EXCLUDED.observed_at > latest_positions.observed_at`,
		position.VesselRef, position.PositionReportID, position.SourceClass, wkt,
		position.LatitudeMicros, position.LongitudeMicros,
		position.SpeedOverGroundMilliknots, position.CourseOverGroundMillidegrees, position.HeadingMillidegrees,
		position.NavStatus, position.Classification, position.ObservedAt.UTC())
	if err != nil {
		return fmt.Errorf("upsert latest position vessel_ref %s: %w", position.VesselRef, err)
	}
	return nil
}

// EnsurePositionPartitions provisions daily partitions for the given days
// (idempotent; inserts into an unprovisioned day fail closed).
func (store *Store) EnsurePositionPartitions(ctx context.Context, days ...time.Time) error {
	for _, day := range days {
		utc := day.UTC()
		midnight := time.Date(utc.Year(), utc.Month(), utc.Day(), 0, 0, 0, 0, time.UTC)
		if _, err := store.pool.Exec(ctx, `SELECT geo_ensure_position_partition($1::date)`, midnight); err != nil {
			return fmt.Errorf("ensure position partition %s: %w", midnight.Format("2006-01-02"), err)
		}
	}
	return nil
}

// WithTenant runs fn inside a transaction with app.tenant_id bound (RLS).
func (store *Store) WithTenant(ctx context.Context, tenantID string, fn func(tx pgx.Tx) error) error {
	if strings.TrimSpace(tenantID) == "" {
		return errors.New("tenant id is required for tenant-scoped access")
	}
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tenant transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT set_config('app.tenant_id', $1, true)`, tenantID); err != nil {
		return fmt.Errorf("bind tenant: %w", err)
	}
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
