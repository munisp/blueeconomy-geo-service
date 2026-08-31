package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
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

// SOSRow is the REST read model for sos_alerts. The lifecycle ledger
// columns are exposed so clients stop treating resolved history as live.
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
	// Lifecycle ledger (0006_sos_lifecycle.sql); empty while RAISED.
	AcknowledgedBy string     `json:"acknowledgedBy,omitempty"`
	AcknowledgedAt *time.Time `json:"acknowledgedAt,omitempty"`
	ResolvedBy     string     `json:"resolvedBy,omitempty"`
	ResolvedAt     *time.Time `json:"resolvedAt,omitempty"`
}

// sosColumns is the shared SELECT column list for the SOS read model.
const sosColumns = `sos_alert_id, reporter_id, vessel_reference,
	latitude_micros, longitude_micros, recorded_at, free_text, classification, state, received_at,
	COALESCE(acknowledged_by, ''), acknowledged_at, COALESCE(resolved_by, ''), resolved_at`

func scanSOSRow(scanner interface{ Scan(...any) error }) (SOSRow, error) {
	var alert SOSRow
	err := scanner.Scan(&alert.SosAlertID, &alert.ReporterID, &alert.VesselReference,
		&alert.LatitudeMicros, &alert.LongitudeMicros, &alert.RecordedAt,
		&alert.FreeText, &alert.Classification, &alert.State, &alert.ReceivedAt,
		&alert.AcknowledgedBy, &alert.AcknowledgedAt, &alert.ResolvedBy, &alert.ResolvedAt)
	return alert, err
}

// ListSOS returns alerts whose classification is covered by the caller's
// clearance (the API enforces the RESTRICTED floor before calling).
func (store *Store) ListSOS(ctx context.Context, clearedLabels []string, limit int) ([]SOSRow, error) {
	rows, err := store.pool.Query(ctx, `SELECT `+sosColumns+`
		FROM sos_alerts WHERE classification = ANY($1)
		ORDER BY received_at DESC LIMIT $2`, clearedLabels, limit)
	if err != nil {
		return nil, fmt.Errorf("list sos alerts: %w", err)
	}
	defer rows.Close()
	alerts := make([]SOSRow, 0)
	for rows.Next() {
		alert, err := scanSOSRow(rows)
		if err != nil {
			return nil, fmt.Errorf("scan sos alert: %w", err)
		}
		alerts = append(alerts, alert)
	}
	return alerts, rows.Err()
}

// ErrSOSNotFound is returned when the alert id is unknown.
var ErrSOSNotFound = errors.New("sos alert not found")

// ErrSOSInvalidTransition is returned when the requested lifecycle step is
// not legal from the current state (409 semantics).
var ErrSOSInvalidTransition = errors.New("sos alert lifecycle transition is not legal from the current state")

// TransitionSOSAlert applies one lifecycle step under SELECT ... FOR UPDATE:
// acknowledge requires RAISED; resolve requires RAISED or ACKNOWLEDGED
// (direct RAISED -> RESOLVED is a legal shortcut). The acting principal
// (from verified token claims), the server timestamp and an optional note
// are persisted on the ledger columns; the updated row is returned.
func (store *Store) TransitionSOSAlert(ctx context.Context, sosAlertID, actor, action, note string) (SOSRow, error) {
	if strings.TrimSpace(actor) == "" {
		return SOSRow{}, errors.New("lifecycle actor is required")
	}
	if action != "acknowledge" && action != "resolve" {
		return SOSRow{}, fmt.Errorf("sos lifecycle action %q is not acknowledge or resolve", action)
	}
	if len(note) > 500 {
		return SOSRow{}, errors.New("lifecycle note exceeds 500 characters")
	}
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return SOSRow{}, fmt.Errorf("begin sos transition: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var state string
	err = tx.QueryRow(ctx, `SELECT state FROM sos_alerts WHERE sos_alert_id = $1 FOR UPDATE`, sosAlertID).Scan(&state)
	if errors.Is(err, pgx.ErrNoRows) {
		return SOSRow{}, ErrSOSNotFound
	}
	if err != nil {
		return SOSRow{}, fmt.Errorf("lock sos alert %s: %w", sosAlertID, err)
	}
	var update string
	switch action {
	case "acknowledge":
		if state != "RAISED" {
			return SOSRow{}, fmt.Errorf("%w: %s -> ACKNOWLEDGED", ErrSOSInvalidTransition, state)
		}
		update = `UPDATE sos_alerts SET state = 'ACKNOWLEDGED',
			acknowledged_by = $2, acknowledged_at = now(), acknowledge_note = $3
			WHERE sos_alert_id = $1`
	case "resolve":
		if state != "RAISED" && state != "ACKNOWLEDGED" {
			return SOSRow{}, fmt.Errorf("%w: %s -> RESOLVED", ErrSOSInvalidTransition, state)
		}
		update = `UPDATE sos_alerts SET state = 'RESOLVED',
			resolved_by = $2, resolved_at = now(), resolve_note = $3
			WHERE sos_alert_id = $1`
	}
	if _, err := tx.Exec(ctx, update, sosAlertID, actor, note); err != nil {
		return SOSRow{}, fmt.Errorf("transition sos alert %s to %s: %w", sosAlertID, action, err)
	}
	row, err := scanSOSRow(tx.QueryRow(ctx, `SELECT `+sosColumns+` FROM sos_alerts WHERE sos_alert_id = $1`, sosAlertID))
	if err != nil {
		return SOSRow{}, fmt.Errorf("read transitioned sos alert %s: %w", sosAlertID, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return SOSRow{}, fmt.Errorf("commit sos transition: %w", err)
	}
	return row, nil
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
