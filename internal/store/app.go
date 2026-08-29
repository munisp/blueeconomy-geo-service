package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// AppReport is a Tier-0 mobile position report with device-outbox
// idempotency.
type AppReport struct {
	PositionReportID          string
	ReporterID                string
	VesselReference           string
	LatitudeMicros            int32
	LongitudeMicros           int32
	AccuracyM                 uint32
	SpeedMillimetresPerSecond *uint32
	RecordedAt                time.Time
	OutboxID                  string
	Classification            string
}

// SOSAlert is a mobile-raised distress alert (classification floor
// RESTRICTED).
type SOSAlert struct {
	SosAlertID      string
	ReporterID      string
	VesselReference string
	LatitudeMicros  int32
	LongitudeMicros int32
	RecordedAt      time.Time
	OutboxID        string
	FreeText        string
	Classification  string
}

// ErrDuplicateOutbox reports an outbox_id replay with a *different* payload
// under the same (reporter_id, outbox_id) — a 409 conflict.
var ErrDuplicateOutbox = errors.New("outbox_id already used with a different payload")

// InsertAppReport applies the (reporter_id, outbox_id) idempotency contract:
// an exact replay is absorbed (inserted=false, no error); a conflicting
// reuse of outbox_id fails with ErrDuplicateOutbox.
func (store *Store) InsertAppReport(ctx context.Context, report AppReport) (inserted bool, err error) {
	classification := report.Classification
	if classification == "" {
		classification = "PUBLIC"
	}
	tag, err := store.pool.Exec(ctx, `INSERT INTO app_position_reports (
		position_report_id, reporter_id, vessel_reference, latitude_micros, longitude_micros,
		accuracy_m, speed_millimetres_per_second, recorded_at, outbox_id, classification)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		ON CONFLICT (reporter_id, outbox_id) DO NOTHING`,
		report.PositionReportID, report.ReporterID, report.VesselReference,
		report.LatitudeMicros, report.LongitudeMicros, report.AccuracyM,
		report.SpeedMillimetresPerSecond, report.RecordedAt.UTC(), report.OutboxID, classification)
	if err != nil {
		return false, fmt.Errorf("insert app report: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return true, nil
	}
	// Existing row: identical payload → idempotent replay; otherwise 409.
	var identical bool
	err = store.pool.QueryRow(ctx, `SELECT position_report_id = $3 AND vessel_reference = $4
		AND latitude_micros = $5 AND longitude_micros = $6 AND recorded_at = $7
		FROM app_position_reports WHERE reporter_id = $1 AND outbox_id = $2`,
		report.ReporterID, report.OutboxID, report.PositionReportID, report.VesselReference,
		report.LatitudeMicros, report.LongitudeMicros, report.RecordedAt.UTC()).Scan(&identical)
	if err != nil {
		return false, fmt.Errorf("read existing app report: %w", err)
	}
	if !identical {
		return false, ErrDuplicateOutbox
	}
	return false, nil
}

// InsertSOSAlert applies the same idempotency contract to SOS alerts and
// enforces the RESTRICTED classification floor (fail closed).
func (store *Store) InsertSOSAlert(ctx context.Context, alert SOSAlert) (inserted bool, err error) {
	switch alert.Classification {
	case "RESTRICTED", "CONFIDENTIAL", "SECRET":
	default:
		return false, errors.New("sos alert classification floor is RESTRICTED")
	}
	if len(alert.FreeText) > 280 {
		return false, errors.New("sos free_text exceeds 280 characters")
	}
	tag, err := store.pool.Exec(ctx, `INSERT INTO sos_alerts (
		sos_alert_id, reporter_id, vessel_reference, latitude_micros, longitude_micros,
		recorded_at, outbox_id, free_text, classification)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		ON CONFLICT (reporter_id, outbox_id) DO NOTHING`,
		alert.SosAlertID, alert.ReporterID, alert.VesselReference,
		alert.LatitudeMicros, alert.LongitudeMicros, alert.RecordedAt.UTC(),
		alert.OutboxID, alert.FreeText, alert.Classification)
	if err != nil {
		return false, fmt.Errorf("insert sos alert: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return true, nil
	}
	var identical bool
	err = store.pool.QueryRow(ctx, `SELECT sos_alert_id = $3 AND vessel_reference = $4
		AND latitude_micros = $5 AND longitude_micros = $6
		FROM sos_alerts WHERE reporter_id = $1 AND outbox_id = $2`,
		alert.ReporterID, alert.OutboxID, alert.SosAlertID, alert.VesselReference,
		alert.LatitudeMicros, alert.LongitudeMicros).Scan(&identical)
	if err != nil {
		return false, fmt.Errorf("read existing sos alert: %w", err)
	}
	if !identical {
		return false, ErrDuplicateOutbox
	}
	return false, nil
}

// SOSRow is the REST read model for sos_alerts.
type SOSRow struct {
	SosAlertID      string    `json:"sosAlertId"`
	ReporterID      string    `json:"reporterId"`
	VesselReference string    `json:"vesselReference"`
	LatitudeMicros  int32     `json:"latitudeMicros"`
	LongitudeMicros int32     `json:"longitudeMicros"`
	RecordedAt      time.Time `json:"recordedAt"`
	FreeText        string    `json:"freeText"`
	Classification  string    `json:"classification"`
	State           string    `json:"state"`
	ReceivedAt      time.Time `json:"receivedAt"`
}

// ListSOS returns alerts whose classification is covered by the caller's
// clearance (the API enforces the RESTRICTED floor before calling).
func (store *Store) ListSOS(ctx context.Context, clearedLabels []string, limit int) ([]SOSRow, error) {
	rows, err := store.pool.Query(ctx, `SELECT sos_alert_id, reporter_id, vessel_reference,
		latitude_micros, longitude_micros, recorded_at, free_text, classification, state, received_at
		FROM sos_alerts WHERE classification = ANY($1)
		ORDER BY received_at DESC LIMIT $2`, clearedLabels, limit)
	if err != nil {
		return nil, fmt.Errorf("list sos alerts: %w", err)
	}
	defer rows.Close()
	alerts := make([]SOSRow, 0)
	for rows.Next() {
		var alert SOSRow
		if err := rows.Scan(&alert.SosAlertID, &alert.ReporterID, &alert.VesselReference,
			&alert.LatitudeMicros, &alert.LongitudeMicros, &alert.RecordedAt,
			&alert.FreeText, &alert.Classification, &alert.State, &alert.ReceivedAt); err != nil {
			return nil, fmt.Errorf("scan sos alert: %w", err)
		}
		alerts = append(alerts, alert)
	}
	return alerts, rows.Err()
}

// LatestPositionForVesselRef resolves an app vessel reference for the
// app-position → latest_positions projection.
func (store *Store) LatestPositionForVesselRef(ctx context.Context, vesselRef string) (*Position, error) {
	var position Position
	var mmsi, vesselRefOut *string
	err := store.pool.QueryRow(ctx, `SELECT mmsi, vessel_ref, position_report_id, source_class,
		latitude_micros, longitude_micros, classification, observed_at
		FROM latest_positions WHERE vessel_ref = $1`, vesselRef).
		Scan(&mmsi, &vesselRefOut, &position.PositionReportID, &position.SourceClass,
			&position.LatitudeMicros, &position.LongitudeMicros, &position.Classification, &position.ObservedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if mmsi != nil {
		position.MMSI = *mmsi
	}
	if vesselRefOut != nil {
		position.VesselRef = *vesselRefOut
	}
	return &position, nil
}
