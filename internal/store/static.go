package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// StaticReport is vessel static/voyage data for the SCD-2 vessels_static
// dimension.
type StaticReport struct {
	StaticReportID      string
	MMSI                string
	IMO                 string
	Callsign            string
	ShipName            string
	ShipTypeCode        int32
	DimensionBowM       uint32
	DimensionSternM     uint32
	DimensionPortM      uint32
	DimensionStarboardM uint32
	DraughtMillimetres  uint32
	Destination         string
	ETA                 *time.Time
	EpfsType            string
	SourceClass         string
	Classification      string
	ObservedAt          time.Time
}

// UpsertVesselStatic applies SCD-2 semantics: when the incoming report
// changes the current row for the MMSI, the current row is closed
// (valid_to = observed_at) and the new row becomes current. Identical
// re-reports are absorbed without opening a new version.
//
// Out-of-order delivery (a delayed AIS type-5/24 whose observed_at predates
// the current row's valid_from) is dropped, not applied: closing the
// current row backwards would violate CHECK (valid_to > valid_from) and
// reject the whole upsert, and rewriting history would corrupt the open
// row. The caller observes applied=false and logs/metrics the drop.
func (store *Store) UpsertVesselStatic(ctx context.Context, report StaticReport) (applied bool, err error) {
	if report.MMSI == "" {
		return false, errors.New("static report requires mmsi")
	}
	if report.EpfsType == "" {
		report.EpfsType = "UNSPECIFIED"
	}
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin static upsert: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var currentID string
	var unchanged bool
	var currentValidFrom time.Time
	err = tx.QueryRow(ctx, `SELECT static_report_id,
		imo = $2 AND callsign = $3 AND ship_name = $4 AND ship_type_code = $5
		AND dimension_bow_m = $6 AND dimension_stern_m = $7 AND dimension_port_m = $8 AND dimension_starboard_m = $9
		AND draught_millimetres = $10 AND destination = $11 AND epfs_type = $12,
		valid_from
		FROM vessels_static WHERE mmsi = $1 AND valid_to IS NULL
		ORDER BY valid_from DESC LIMIT 1 FOR UPDATE`,
		report.MMSI, report.IMO, report.Callsign, report.ShipName, report.ShipTypeCode,
		report.DimensionBowM, report.DimensionSternM, report.DimensionPortM, report.DimensionStarboardM,
		report.DraughtMillimetres, report.Destination, report.EpfsType).Scan(&currentID, &unchanged, &currentValidFrom)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return false, fmt.Errorf("read current static row mmsi %s: %w", report.MMSI, err)
	}
	if unchanged {
		return true, tx.Commit(ctx)
	}
	observed := report.ObservedAt.UTC()
	if currentID != "" && !observed.After(currentValidFrom) {
		// Stale or equal-time report: leave the current open row untouched.
		return false, tx.Commit(ctx)
	}
	if currentID != "" {
		if _, err := tx.Exec(ctx, `UPDATE vessels_static SET valid_to = $2
			WHERE static_report_id = $1 AND valid_to IS NULL`, currentID, observed); err != nil {
			return false, fmt.Errorf("close static row %s: %w", currentID, err)
		}
	}
	if _, err := tx.Exec(ctx, `INSERT INTO vessels_static (
		static_report_id, mmsi, imo, callsign, ship_name, ship_type_code,
		dimension_bow_m, dimension_stern_m, dimension_port_m, dimension_starboard_m,
		draught_millimetres, destination, eta, epfs_type, source_class, classification,
		observed_at, valid_from)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)`,
		report.StaticReportID, report.MMSI, report.IMO, report.Callsign, report.ShipName, report.ShipTypeCode,
		report.DimensionBowM, report.DimensionSternM, report.DimensionPortM, report.DimensionStarboardM,
		report.DraughtMillimetres, report.Destination, report.ETA, report.EpfsType, report.SourceClass,
		report.Classification, observed, observed); err != nil {
		return false, fmt.Errorf("insert static row mmsi %s: %w", report.MMSI, err)
	}
	return true, tx.Commit(ctx)
}
