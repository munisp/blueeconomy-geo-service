package store

// Phase-12 safety-compliance persistence: FSC/PSC inspections (with
// detention maker-checker), SAR coordination (phase ladder, resource
// tasking, append-only comms log) and marine accident investigation (state
// machine, hash-chained evidence). Every method runs inside WithTenant
// (RLS bound); transitions are applied under SELECT ... FOR UPDATE exactly
// like the SOS lifecycle (0006 doctrine).

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// ─── Shared errors ──────────────────────────────────────────────────────────

var ErrSafetyNotFound = errors.New("safety resource not found")
var ErrSafetyInvalidTransition = errors.New("safety lifecycle transition is not legal from the current state")
var ErrSafetyMakerChecker = errors.New("maker-checker violation: the acting principal must differ from the maker")

// ─── FSC/PSC inspection ─────────────────────────────────────────────────────

type ChecklistTemplateRow struct {
	TemplateID string
	TenantID   string
	Regime     string
	Version    int
	Title      string
	Items      string // JSONB array, raw
	CreatedBy  string
	CreatedAt  time.Time
}

type InspectionRow struct {
	InspectionID          string
	TenantID              string
	Regime                string
	TemplateID            string
	VesselReference       string
	PortCode              string
	Classification        string
	State                 string
	InspectorPrincipalID  string
	CreatedAt             time.Time
	StartedAt             *time.Time
	CompletedAt           *time.Time
	DetainedBy            *string
	DetainedAt            *time.Time
	DetentionGrounds      *string
	RectificationStartedBy *string
	RectificationStartedAt *time.Time
	ReleasedBy            *string
	ReleasedAt            *time.Time
	ClosedBy              *string
	ClosedAt              *time.Time
}

type DeficiencyRow struct {
	DeficiencyID         string
	InspectionID         string
	Code                 string
	Description          string
	Severity             string
	State                string
	RectificationDeadline *time.Time
	RecordedBy           string
	RecordedAt           time.Time
	RectifiedBy          *string
	RectifiedAt          *time.Time
	VerifiedBy           *string
	VerifiedAt           *time.Time
}

const inspectionColumns = `inspection_id, tenant_id, regime, template_id, vessel_reference, port_code,
	classification, state, inspector_principal_id, created_at, started_at, completed_at,
	detained_by, detained_at, detention_grounds, rectification_started_by, rectification_started_at,
	released_by, released_at, closed_by, closed_at`

func scanInspection(row pgx.Row) (InspectionRow, error) {
	var r InspectionRow
	err := row.Scan(&r.InspectionID, &r.TenantID, &r.Regime, &r.TemplateID, &r.VesselReference,
		&r.PortCode, &r.Classification, &r.State, &r.InspectorPrincipalID, &r.CreatedAt,
		&r.StartedAt, &r.CompletedAt, &r.DetainedBy, &r.DetainedAt, &r.DetentionGrounds,
		&r.RectificationStartedBy, &r.RectificationStartedAt, &r.ReleasedBy, &r.ReleasedAt,
		&r.ClosedBy, &r.ClosedAt)
	return r, err
}

func (store *Store) CreateChecklistTemplate(ctx context.Context, row ChecklistTemplateRow) error {
	return store.WithTenant(ctx, row.TenantID, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO safety_checklist_templates
			(template_id, tenant_id, regime, version, title, items, created_by)
			VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7)`,
			row.TemplateID, row.TenantID, row.Regime, row.Version, row.Title, row.Items, row.CreatedBy)
		if err != nil {
			return fmt.Errorf("insert checklist template: %w", err)
		}
		return nil
	})
}

func (store *Store) ListChecklistTemplates(ctx context.Context, tenantID, regime string) ([]ChecklistTemplateRow, error) {
	out := []ChecklistTemplateRow{}
	err := store.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		query := `SELECT template_id, tenant_id, regime, version, title, items::text, created_by, created_at
			FROM safety_checklist_templates`
		args := []any{}
		if regime != "" {
			query += ` WHERE regime = $1`
			args = append(args, regime)
		}
		query += ` ORDER BY regime, version`
		rows, err := tx.Query(ctx, query, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var r ChecklistTemplateRow
			if err := rows.Scan(&r.TemplateID, &r.TenantID, &r.Regime, &r.Version, &r.Title,
				&r.Items, &r.CreatedBy, &r.CreatedAt); err != nil {
				return err
			}
			out = append(out, r)
		}
		return rows.Err()
	})
	return out, err
}

func (store *Store) CreateInspection(ctx context.Context, row InspectionRow) (InspectionRow, error) {
	var out InspectionRow
	err := store.WithTenant(ctx, row.TenantID, func(tx pgx.Tx) error {
		// Fail closed: the referenced template must exist and match the
		// declared regime (visible only within the same tenant under RLS).
		var templateRegime string
		err := tx.QueryRow(ctx, `SELECT regime FROM safety_checklist_templates WHERE template_id = $1`,
			row.TemplateID).Scan(&templateRegime)
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("%w: checklist template %s", ErrSafetyNotFound, row.TemplateID)
		}
		if err != nil {
			return err
		}
		if templateRegime != row.Regime {
			return fmt.Errorf("%w: template regime %s does not match inspection regime %s",
				ErrSafetyInvalidTransition, templateRegime, row.Regime)
		}
		return tx.QueryRow(ctx, `INSERT INTO safety_inspections
			(inspection_id, tenant_id, regime, template_id, vessel_reference, port_code,
			 classification, inspector_principal_id)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING `+inspectionColumns,
			row.InspectionID, row.TenantID, row.Regime, row.TemplateID, row.VesselReference,
			row.PortCode, row.Classification, row.InspectorPrincipalID).Scan(
			&out.InspectionID, &out.TenantID, &out.Regime, &out.TemplateID, &out.VesselReference,
			&out.PortCode, &out.Classification, &out.State, &out.InspectorPrincipalID, &out.CreatedAt,
			&out.StartedAt, &out.CompletedAt, &out.DetainedBy, &out.DetainedAt, &out.DetentionGrounds,
			&out.RectificationStartedBy, &out.RectificationStartedAt, &out.ReleasedBy, &out.ReleasedAt,
			&out.ClosedBy, &out.ClosedAt)
	})
	return out, err
}

func (store *Store) GetInspection(ctx context.Context, tenantID, inspectionID string) (InspectionRow, error) {
	var out InspectionRow
	err := store.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		row, err := scanInspection(tx.QueryRow(ctx,
			`SELECT `+inspectionColumns+` FROM safety_inspections WHERE inspection_id = $1`, inspectionID))
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrSafetyNotFound
		}
		out = row
		return err
	})
	return out, err
}

func (store *Store) ListInspections(ctx context.Context, tenantID string, limit int) ([]InspectionRow, error) {
	out := []InspectionRow{}
	err := store.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT `+inspectionColumns+` FROM safety_inspections
			ORDER BY created_at DESC LIMIT $1`, limit)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			row, err := scanInspection(rows)
			if err != nil {
				return err
			}
			out = append(out, row)
		}
		return rows.Err()
	})
	return out, err
}

// TransitionInspection applies one inspection lifecycle step under
// SELECT ... FOR UPDATE:
//   start       SCHEDULED -> IN_PROGRESS
//   complete    IN_PROGRESS -> COMPLETED
//   detain      IN_PROGRESS|COMPLETED -> DETAINED (grounds required)
//   rectify     DETAINED -> RECTIFICATION
//   release     RECTIFICATION -> RELEASED (checker: actor must differ from
//               the detaining maker, and every CRITICAL deficiency must be
//               VERIFIED before release)
//   close       COMPLETED|RELEASED -> CLOSED
func (store *Store) TransitionInspection(ctx context.Context, tenantID, inspectionID, actor, action, note string) (InspectionRow, error) {
	var out InspectionRow
	err := store.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		row, err := scanInspection(tx.QueryRow(ctx,
			`SELECT `+inspectionColumns+` FROM safety_inspections WHERE inspection_id = $1 FOR UPDATE`,
			inspectionID))
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrSafetyNotFound
		}
		if err != nil {
			return err
		}
		var query string
		switch action {
		case "start":
			if row.State != "SCHEDULED" {
				return fmt.Errorf("%w: %s -> IN_PROGRESS", ErrSafetyInvalidTransition, row.State)
			}
			query = `UPDATE safety_inspections SET state = 'IN_PROGRESS', started_at = now()
				WHERE inspection_id = $1`
		case "complete":
			if row.State != "IN_PROGRESS" {
				return fmt.Errorf("%w: %s -> COMPLETED", ErrSafetyInvalidTransition, row.State)
			}
			query = `UPDATE safety_inspections SET state = 'COMPLETED', completed_at = now()
				WHERE inspection_id = $1`
		case "detain":
			if row.State != "IN_PROGRESS" && row.State != "COMPLETED" {
				return fmt.Errorf("%w: %s -> DETAINED", ErrSafetyInvalidTransition, row.State)
			}
			if strings.TrimSpace(note) == "" {
				return fmt.Errorf("%w: detention grounds are required", ErrSafetyInvalidTransition)
			}
			query = `UPDATE safety_inspections SET state = 'DETAINED', detained_by = $2,
				detained_at = now(), detention_grounds = $3 WHERE inspection_id = $1`
		case "rectify":
			if row.State != "DETAINED" {
				return fmt.Errorf("%w: %s -> RECTIFICATION", ErrSafetyInvalidTransition, row.State)
			}
			query = `UPDATE safety_inspections SET state = 'RECTIFICATION',
				rectification_started_by = $2, rectification_started_at = now() WHERE inspection_id = $1`
		case "release":
			if row.State != "RECTIFICATION" {
				return fmt.Errorf("%w: %s -> RELEASED", ErrSafetyInvalidTransition, row.State)
			}
			if row.DetainedBy != nil && *row.DetainedBy == actor {
				return ErrSafetyMakerChecker
			}
			var openCritical int
			if err := tx.QueryRow(ctx, `SELECT count(*) FROM safety_inspection_deficiencies
				WHERE inspection_id = $1 AND severity = 'CRITICAL' AND state <> 'VERIFIED'`,
				inspectionID).Scan(&openCritical); err != nil {
				return err
			}
			if openCritical > 0 {
				return fmt.Errorf("%w: %d CRITICAL deficiencies are not VERIFIED",
					ErrSafetyInvalidTransition, openCritical)
			}
			query = `UPDATE safety_inspections SET state = 'RELEASED', released_by = $2,
				released_at = now() WHERE inspection_id = $1`
		case "close":
			if row.State != "COMPLETED" && row.State != "RELEASED" {
				return fmt.Errorf("%w: %s -> CLOSED", ErrSafetyInvalidTransition, row.State)
			}
			query = `UPDATE safety_inspections SET state = 'CLOSED', closed_by = $2, closed_at = now()
				WHERE inspection_id = $1`
		default:
			return fmt.Errorf("%w: unknown action %q", ErrSafetyInvalidTransition, action)
		}
		var execErr error
		if action == "start" || action == "complete" {
			_, execErr = tx.Exec(ctx, query, inspectionID)
		} else if action == "detain" {
			_, execErr = tx.Exec(ctx, query, inspectionID, actor, note)
		} else {
			_, execErr = tx.Exec(ctx, query, inspectionID, actor)
		}
		if execErr != nil {
			return execErr
		}
		out, err = scanInspection(tx.QueryRow(ctx,
			`SELECT `+inspectionColumns+` FROM safety_inspections WHERE inspection_id = $1`, inspectionID))
		return err
	})
	return out, err
}

// RecordDeficiency appends one deficiency to an inspection. The deficiency
// code must reference an item of the inspection's checklist template (fail
// closed against free-text codes).
func (store *Store) RecordDeficiency(ctx context.Context, tenantID string, row DeficiencyRow) (DeficiencyRow, error) {
	var out DeficiencyRow
	err := store.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		var items string
		err := tx.QueryRow(ctx, `SELECT t.items::text FROM safety_inspections i
			JOIN safety_checklist_templates t ON t.template_id = i.template_id
			WHERE i.inspection_id = $1 FOR UPDATE OF i`, row.InspectionID).Scan(&items)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrSafetyNotFound
		}
		if err != nil {
			return err
		}
		var known bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (
			SELECT 1 FROM jsonb_array_elements($1::jsonb) item WHERE item->>'code' = $2)`,
			items, row.Code).Scan(&known); err != nil {
			return err
		}
		if !known {
			return fmt.Errorf("%w: deficiency code %q is not an item of the inspection checklist template",
				ErrSafetyInvalidTransition, row.Code)
		}
		return tx.QueryRow(ctx, `INSERT INTO safety_inspection_deficiencies
			(deficiency_id, inspection_id, code, description, severity, rectification_deadline, recorded_by)
			VALUES ($1,$2,$3,$4,$5,$6,$7)
			RETURNING deficiency_id, inspection_id, code, description, severity, state,
				rectification_deadline, recorded_by, recorded_at, rectified_by, rectified_at, verified_by, verified_at`,
			row.DeficiencyID, row.InspectionID, row.Code, row.Description, row.Severity,
			row.RectificationDeadline, row.RecordedBy).Scan(
			&out.DeficiencyID, &out.InspectionID, &out.Code, &out.Description, &out.Severity, &out.State,
			&out.RectificationDeadline, &out.RecordedBy, &out.RecordedAt, &out.RectifiedBy,
			&out.RectifiedAt, &out.VerifiedBy, &out.VerifiedAt)
	})
	return out, err
}

func (store *Store) ListDeficiencies(ctx context.Context, tenantID, inspectionID string) ([]DeficiencyRow, error) {
	out := []DeficiencyRow{}
	err := store.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT deficiency_id, inspection_id, code, description, severity,
			state, rectification_deadline, recorded_by, recorded_at, rectified_by, rectified_at,
			verified_by, verified_at
			FROM safety_inspection_deficiencies WHERE inspection_id = $1 ORDER BY recorded_at`, inspectionID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var r DeficiencyRow
			if err := rows.Scan(&r.DeficiencyID, &r.InspectionID, &r.Code, &r.Description, &r.Severity,
				&r.State, &r.RectificationDeadline, &r.RecordedBy, &r.RecordedAt, &r.RectifiedBy,
				&r.RectifiedAt, &r.VerifiedBy, &r.VerifiedAt); err != nil {
				return err
			}
			out = append(out, r)
		}
		return rows.Err()
	})
	return out, err
}

// TransitionDeficiency: rectify OPEN -> RECTIFIED; verify RECTIFIED ->
// VERIFIED (checker: verifier must differ from the rectifier).
func (store *Store) TransitionDeficiency(ctx context.Context, tenantID, deficiencyID, actor, action string) (DeficiencyRow, error) {
	var out DeficiencyRow
	err := store.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		var state, rectifiedBy *string
		var severity string
		err := tx.QueryRow(ctx, `SELECT state, severity, rectified_by FROM safety_inspection_deficiencies
			WHERE deficiency_id = $1 FOR UPDATE`, deficiencyID).
			Scan(&state, &severity, &rectifiedBy)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrSafetyNotFound
		}
		if err != nil {
			return err
		}
		var query string
		switch {
		case action == "rectify" && *state == "OPEN":
			query = `UPDATE safety_inspection_deficiencies SET state = 'RECTIFIED',
				rectified_by = $2, rectified_at = now() WHERE deficiency_id = $1`
		case action == "verify" && *state == "RECTIFIED":
			if rectifiedBy != nil && *rectifiedBy == actor {
				return ErrSafetyMakerChecker
			}
			query = `UPDATE safety_inspection_deficiencies SET state = 'VERIFIED',
				verified_by = $2, verified_at = now() WHERE deficiency_id = $1`
		default:
			return fmt.Errorf("%w: %s -> %s", ErrSafetyInvalidTransition, *state, action)
		}
		if _, err := tx.Exec(ctx, query, deficiencyID, actor); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `SELECT deficiency_id, inspection_id, code, description, severity,
			state, rectification_deadline, recorded_by, recorded_at, rectified_by, rectified_at,
			verified_by, verified_at FROM safety_inspection_deficiencies WHERE deficiency_id = $1`,
			deficiencyID).Scan(&out.DeficiencyID, &out.InspectionID, &out.Code, &out.Description,
			&out.Severity, &out.State, &out.RectificationDeadline, &out.RecordedBy, &out.RecordedAt,
			&out.RectifiedBy, &out.RectifiedAt, &out.VerifiedBy, &out.VerifiedAt)
	})
	return out, err
}

// ─── SAR coordination ───────────────────────────────────────────────────────

type SarIncidentRow struct {
	IncidentID         string
	TenantID           string
	SosAlertID         *string
	Title              string
	Classification     string
	Phase              string
	OpenedBy           string
	OpenedAt           time.Time
	AlertedBy          *string
	AlertedAt          *time.Time
	DistressDeclaredBy *string
	DistressDeclaredAt *time.Time
	RescueStartedBy    *string
	RescueStartedAt    *time.Time
	ClosedBy           *string
	ClosedAt           *time.Time
	ClosureReason      *string
}

type SarTaskingRow struct {
	TaskingID    string
	IncidentID   string
	ResourceType string
	ResourceName string
	State        string
	TaskedBy     string
	TaskedAt     time.Time
	ReleasedBy   *string
	ReleasedAt   *time.Time
}

type SarCommsRow struct {
	EntryID    int64
	IncidentID string
	Direction  string
	Channel    string
	Message    string
	LoggedBy   string
	LoggedAt   time.Time
}

const sarColumns = `incident_id, tenant_id, sos_alert_id, title, classification, phase,
	opened_by, opened_at, alerted_by, alerted_at, distress_declared_by, distress_declared_at,
	rescue_started_by, rescue_started_at, closed_by, closed_at, closure_reason`

func scanSarIncident(row pgx.Row) (SarIncidentRow, error) {
	var r SarIncidentRow
	err := row.Scan(&r.IncidentID, &r.TenantID, &r.SosAlertID, &r.Title, &r.Classification, &r.Phase,
		&r.OpenedBy, &r.OpenedAt, &r.AlertedBy, &r.AlertedAt, &r.DistressDeclaredBy, &r.DistressDeclaredAt,
		&r.RescueStartedBy, &r.RescueStartedAt, &r.ClosedBy, &r.ClosedAt, &r.ClosureReason)
	return r, err
}

// CreateSarIncident opens an incident in UNCERTAINTY, optionally linked to a
// persisted SOS alert and an optional WGS-84 position (microdegrees).
func (store *Store) CreateSarIncident(ctx context.Context, row SarIncidentRow, latMicros, lonMicros *int32) (SarIncidentRow, error) {
	var out SarIncidentRow
	err := store.WithTenant(ctx, row.TenantID, func(tx pgx.Tx) error {
		var positionArg any
		if latMicros != nil && lonMicros != nil {
			positionArg = fmt.Sprintf("SRID=4326;POINT(%s %s)", microdegText(*lonMicros), microdegText(*latMicros))
		}
		return tx.QueryRow(ctx, `INSERT INTO sar_incidents
			(incident_id, tenant_id, sos_alert_id, title, position, classification, opened_by)
			VALUES ($1,$2,$3,$4,$5::geography,$6,$7) RETURNING `+sarColumns,
			row.IncidentID, row.TenantID, row.SosAlertID, row.Title, positionArg,
			row.Classification, row.OpenedBy).Scan(
			&out.IncidentID, &out.TenantID, &out.SosAlertID, &out.Title, &out.Classification, &out.Phase,
			&out.OpenedBy, &out.OpenedAt, &out.AlertedBy, &out.AlertedAt, &out.DistressDeclaredBy,
			&out.DistressDeclaredAt, &out.RescueStartedBy, &out.RescueStartedAt, &out.ClosedBy,
			&out.ClosedAt, &out.ClosureReason)
	})
	return out, err
}

func microdegText(micros int32) string {
	neg := micros < 0
	abs := micros
	if neg {
		abs = -abs
	}
	text := fmt.Sprintf("%d.%06d", abs/1_000_000, abs%1_000_000)
	if neg {
		return "-" + text
	}
	return text
}

func (store *Store) GetSarIncident(ctx context.Context, tenantID, incidentID string) (SarIncidentRow, error) {
	var out SarIncidentRow
	err := store.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		row, err := scanSarIncident(tx.QueryRow(ctx,
			`SELECT `+sarColumns+` FROM sar_incidents WHERE incident_id = $1`, incidentID))
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrSafetyNotFound
		}
		out = row
		return err
	})
	return out, err
}

func (store *Store) ListSarIncidents(ctx context.Context, tenantID string, limit int) ([]SarIncidentRow, error) {
	out := []SarIncidentRow{}
	err := store.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT `+sarColumns+` FROM sar_incidents
			ORDER BY opened_at DESC LIMIT $1`, limit)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			row, err := scanSarIncident(rows)
			if err != nil {
				return err
			}
			out = append(out, row)
		}
		return rows.Err()
	})
	return out, err
}

// TransitionSarIncident advances the IAMSAR phase ladder. alert / distress /
// rescue require the immediately preceding phase; close is legal from any
// non-terminal phase (false alarm / stood down) and requires a reason.
func (store *Store) TransitionSarIncident(ctx context.Context, tenantID, incidentID, actor, action, reason string) (SarIncidentRow, error) {
	var out SarIncidentRow
	err := store.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		row, err := scanSarIncident(tx.QueryRow(ctx,
			`SELECT `+sarColumns+` FROM sar_incidents WHERE incident_id = $1 FOR UPDATE`, incidentID))
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrSafetyNotFound
		}
		if err != nil {
			return err
		}
		var query string
		switch action {
		case "alert":
			if row.Phase != "UNCERTAINTY" {
				return fmt.Errorf("%w: %s -> ALERT", ErrSafetyInvalidTransition, row.Phase)
			}
			query = `UPDATE sar_incidents SET phase = 'ALERT', alerted_by = $2, alerted_at = now()
				WHERE incident_id = $1`
		case "distress":
			if row.Phase != "ALERT" {
				return fmt.Errorf("%w: %s -> DISTRESS", ErrSafetyInvalidTransition, row.Phase)
			}
			query = `UPDATE sar_incidents SET phase = 'DISTRESS', distress_declared_by = $2,
				distress_declared_at = now() WHERE incident_id = $1`
		case "rescue":
			if row.Phase != "DISTRESS" {
				return fmt.Errorf("%w: %s -> RESCUE", ErrSafetyInvalidTransition, row.Phase)
			}
			query = `UPDATE sar_incidents SET phase = 'RESCUE', rescue_started_by = $2,
				rescue_started_at = now() WHERE incident_id = $1`
		case "close":
			if row.Phase == "CLOSED" {
				return fmt.Errorf("%w: CLOSED is terminal", ErrSafetyInvalidTransition)
			}
			if strings.TrimSpace(reason) == "" {
				return fmt.Errorf("%w: closure reason is required", ErrSafetyInvalidTransition)
			}
			query = `UPDATE sar_incidents SET phase = 'CLOSED', closed_by = $2, closed_at = now(),
				closure_reason = $3 WHERE incident_id = $1`
		default:
			return fmt.Errorf("%w: unknown action %q", ErrSafetyInvalidTransition, action)
		}
		var execErr error
		if action == "close" {
			_, execErr = tx.Exec(ctx, query, incidentID, actor, reason)
		} else {
			_, execErr = tx.Exec(ctx, query, incidentID, actor)
		}
		if execErr != nil {
			return execErr
		}
		out, err = scanSarIncident(tx.QueryRow(ctx,
			`SELECT `+sarColumns+` FROM sar_incidents WHERE incident_id = $1`, incidentID))
		return err
	})
	return out, err
}

// TaskSarResource records one resource tasking against a non-closed
// incident; ReleaseSarTasking marks it released.
func (store *Store) TaskSarResource(ctx context.Context, tenantID string, row SarTaskingRow) (SarTaskingRow, error) {
	var out SarTaskingRow
	err := store.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		var phase string
		err := tx.QueryRow(ctx, `SELECT phase FROM sar_incidents WHERE incident_id = $1 FOR UPDATE`,
			row.IncidentID).Scan(&phase)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrSafetyNotFound
		}
		if err != nil {
			return err
		}
		if phase == "CLOSED" {
			return fmt.Errorf("%w: cannot task resources on a CLOSED incident", ErrSafetyInvalidTransition)
		}
		return tx.QueryRow(ctx, `INSERT INTO sar_resource_taskings
			(tasking_id, incident_id, resource_type, resource_name, tasked_by)
			VALUES ($1,$2,$3,$4,$5) RETURNING tasking_id, incident_id, resource_type, resource_name,
				state, tasked_by, tasked_at, released_by, released_at`,
			row.TaskingID, row.IncidentID, row.ResourceType, row.ResourceName, row.TaskedBy).Scan(
			&out.TaskingID, &out.IncidentID, &out.ResourceType, &out.ResourceName, &out.State,
			&out.TaskedBy, &out.TaskedAt, &out.ReleasedBy, &out.ReleasedAt)
	})
	return out, err
}

// TransitionSarTasking: advance EN_ROUTE -> ON_SCENE, or release from any
// non-released state.
func (store *Store) TransitionSarTasking(ctx context.Context, tenantID, taskingID, actor, action string) (SarTaskingRow, error) {
	var out SarTaskingRow
	err := store.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		var state string
		err := tx.QueryRow(ctx, `SELECT state FROM sar_resource_taskings WHERE tasking_id = $1 FOR UPDATE`,
			taskingID).Scan(&state)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrSafetyNotFound
		}
		if err != nil {
			return err
		}
		var query string
		switch {
		case action == "enroute" && state == "TASKED":
			query = `UPDATE sar_resource_taskings SET state = 'EN_ROUTE' WHERE tasking_id = $1`
		case action == "onscene" && (state == "TASKED" || state == "EN_ROUTE"):
			query = `UPDATE sar_resource_taskings SET state = 'ON_SCENE' WHERE tasking_id = $1`
		case action == "release" && state != "RELEASED":
			query = `UPDATE sar_resource_taskings SET state = 'RELEASED', released_by = $2,
				released_at = now() WHERE tasking_id = $1`
		default:
			return fmt.Errorf("%w: %s -> %s", ErrSafetyInvalidTransition, state, action)
		}
		var execErr error
		if action == "release" {
			_, execErr = tx.Exec(ctx, query, taskingID, actor)
		} else {
			_, execErr = tx.Exec(ctx, query, taskingID)
		}
		if execErr != nil {
			return execErr
		}
		return tx.QueryRow(ctx, `SELECT tasking_id, incident_id, resource_type, resource_name, state,
			tasked_by, tasked_at, released_by, released_at FROM sar_resource_taskings WHERE tasking_id = $1`,
			taskingID).Scan(&out.TaskingID, &out.IncidentID, &out.ResourceType, &out.ResourceName,
			&out.State, &out.TaskedBy, &out.TaskedAt, &out.ReleasedBy, &out.ReleasedAt)
	})
	return out, err
}

// AppendSarComms appends one entry to the incident comms ledger (no
// UPDATE/DELETE exists on this table for the app role).
func (store *Store) AppendSarComms(ctx context.Context, tenantID string, row SarCommsRow) (SarCommsRow, error) {
	var out SarCommsRow
	err := store.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		var exists bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM sar_incidents WHERE incident_id = $1)`,
			row.IncidentID).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			return ErrSafetyNotFound
		}
		return tx.QueryRow(ctx, `INSERT INTO sar_comms_log (incident_id, direction, channel, message, logged_by)
			VALUES ($1,$2,$3,$4,$5) RETURNING entry_id, incident_id, direction, channel, message, logged_by, logged_at`,
			row.IncidentID, row.Direction, row.Channel, row.Message, row.LoggedBy).Scan(
			&out.EntryID, &out.IncidentID, &out.Direction, &out.Channel, &out.Message, &out.LoggedBy, &out.LoggedAt)
	})
	return out, err
}

func (store *Store) ListSarComms(ctx context.Context, tenantID, incidentID string, limit int) ([]SarCommsRow, error) {
	out := []SarCommsRow{}
	err := store.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT entry_id, incident_id, direction, channel, message, logged_by, logged_at
			FROM sar_comms_log WHERE incident_id = $1 ORDER BY entry_id LIMIT $2`, incidentID, limit)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var r SarCommsRow
			if err := rows.Scan(&r.EntryID, &r.IncidentID, &r.Direction, &r.Channel, &r.Message,
				&r.LoggedBy, &r.LoggedAt); err != nil {
				return err
			}
			out = append(out, r)
		}
		return rows.Err()
	})
	return out, err
}

// ─── Marine accident investigation ──────────────────────────────────────────

type InvestigationCaseRow struct {
	CaseID           string
	TenantID         string
	CasualtyType     string
	Severity         string
	VesselReference  string
	OccurredAt       time.Time
	Classification   string
	State            string
	LeadInvestigator string
	OpenedAt         time.Time
	ReportedBy       *string
	ReportedAt       *time.Time
	ClosedBy         *string
	ClosedAt         *time.Time
}

type EvidenceRow struct {
	EvidenceID    string
	CaseID        string
	Description   string
	ContentHash   string
	PrevChainHash string
	ChainHash     string
	CollectedBy   string
	CollectedAt   time.Time
}

type FindingRow struct {
	FindingID string
	CaseID    string
	Finding   string
	CreatedBy string
	CreatedAt time.Time
}

type RecommendationRow struct {
	RecommendationID string
	CaseID           string
	Recommendation   string
	Status           string
	CreatedBy        string
	CreatedAt        time.Time
	DecidedBy        *string
	DecidedAt        *time.Time
}

const caseColumns = `case_id, tenant_id, casualty_type, severity, vessel_reference, occurred_at,
	classification, state, lead_investigator, opened_at, reported_by, reported_at, closed_by, closed_at`

func scanCase(row pgx.Row) (InvestigationCaseRow, error) {
	var r InvestigationCaseRow
	err := row.Scan(&r.CaseID, &r.TenantID, &r.CasualtyType, &r.Severity, &r.VesselReference,
		&r.OccurredAt, &r.Classification, &r.State, &r.LeadInvestigator, &r.OpenedAt,
		&r.ReportedBy, &r.ReportedAt, &r.ClosedBy, &r.ClosedAt)
	return r, err
}

// EvidenceGenesisHash is the prev_chain_hash of the first evidence item of a
// case (64 zero hex digits).
const EvidenceGenesisHash = "0000000000000000000000000000000000000000000000000000000000000000"

// EvidenceChainHash computes sha256(prev_chain_hash || content_hash) over
// the concatenated lowercase-hex encodings. Pure function — unit-testable
// and mirrored exactly by the verifier query.
func EvidenceChainHash(prevChainHash, contentHash string) string {
	sum := sha256.Sum256([]byte(prevChainHash + contentHash))
	return hex.EncodeToString(sum[:])
}

func (store *Store) CreateInvestigationCase(ctx context.Context, row InvestigationCaseRow, latMicros, lonMicros *int32) (InvestigationCaseRow, error) {
	var out InvestigationCaseRow
	err := store.WithTenant(ctx, row.TenantID, func(tx pgx.Tx) error {
		var locationArg any
		if latMicros != nil && lonMicros != nil {
			locationArg = fmt.Sprintf("SRID=4326;POINT(%s %s)", microdegText(*lonMicros), microdegText(*latMicros))
		}
		return tx.QueryRow(ctx, `INSERT INTO investigation_cases
			(case_id, tenant_id, casualty_type, severity, vessel_reference, occurred_at, location,
			 classification, lead_investigator)
			VALUES ($1,$2,$3,$4,$5,$6,$7::geography,$8,$9) RETURNING `+caseColumns,
			row.CaseID, row.TenantID, row.CasualtyType, row.Severity, row.VesselReference,
			row.OccurredAt, locationArg, row.Classification, row.LeadInvestigator).Scan(
			&out.CaseID, &out.TenantID, &out.CasualtyType, &out.Severity, &out.VesselReference,
			&out.OccurredAt, &out.Classification, &out.State, &out.LeadInvestigator, &out.OpenedAt,
			&out.ReportedBy, &out.ReportedAt, &out.ClosedBy, &out.ClosedAt)
	})
	return out, err
}

func (store *Store) GetInvestigationCase(ctx context.Context, tenantID, caseID string) (InvestigationCaseRow, error) {
	var out InvestigationCaseRow
	err := store.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		row, err := scanCase(tx.QueryRow(ctx,
			`SELECT `+caseColumns+` FROM investigation_cases WHERE case_id = $1`, caseID))
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrSafetyNotFound
		}
		out = row
		return err
	})
	return out, err
}

func (store *Store) ListInvestigationCases(ctx context.Context, tenantID string, limit int) ([]InvestigationCaseRow, error) {
	out := []InvestigationCaseRow{}
	err := store.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT `+caseColumns+` FROM investigation_cases
			ORDER BY opened_at DESC LIMIT $1`, limit)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			row, err := scanCase(rows)
			if err != nil {
				return err
			}
			out = append(out, row)
		}
		return rows.Err()
	})
	return out, err
}

// TransitionInvestigationCase advances the state machine:
//   evidence OPEN -> EVIDENCE; analysis EVIDENCE -> ANALYSIS;
//   report ANALYSIS -> REPORTED; close REPORTED -> CLOSED.
func (store *Store) TransitionInvestigationCase(ctx context.Context, tenantID, caseID, actor, action string) (InvestigationCaseRow, error) {
	var out InvestigationCaseRow
	err := store.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		row, err := scanCase(tx.QueryRow(ctx,
			`SELECT `+caseColumns+` FROM investigation_cases WHERE case_id = $1 FOR UPDATE`, caseID))
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrSafetyNotFound
		}
		if err != nil {
			return err
		}
		var query string
		switch action {
		case "evidence":
			if row.State != "OPEN" {
				return fmt.Errorf("%w: %s -> EVIDENCE", ErrSafetyInvalidTransition, row.State)
			}
			query = `UPDATE investigation_cases SET state = 'EVIDENCE' WHERE case_id = $1`
		case "analysis":
			if row.State != "EVIDENCE" {
				return fmt.Errorf("%w: %s -> ANALYSIS", ErrSafetyInvalidTransition, row.State)
			}
			query = `UPDATE investigation_cases SET state = 'ANALYSIS' WHERE case_id = $1`
		case "report":
			if row.State != "ANALYSIS" {
				return fmt.Errorf("%w: %s -> REPORTED", ErrSafetyInvalidTransition, row.State)
			}
			query = `UPDATE investigation_cases SET state = 'REPORTED', reported_by = $2,
				reported_at = now() WHERE case_id = $1`
		case "close":
			if row.State != "REPORTED" {
				return fmt.Errorf("%w: %s -> CLOSED", ErrSafetyInvalidTransition, row.State)
			}
			query = `UPDATE investigation_cases SET state = 'CLOSED', closed_by = $2, closed_at = now()
				WHERE case_id = $1`
		default:
			return fmt.Errorf("%w: unknown action %q", ErrSafetyInvalidTransition, action)
		}
		var execErr error
		if action == "report" || action == "close" {
			_, execErr = tx.Exec(ctx, query, caseID, actor)
		} else {
			_, execErr = tx.Exec(ctx, query, caseID)
		}
		if execErr != nil {
			return execErr
		}
		out, err = scanCase(tx.QueryRow(ctx,
			`SELECT `+caseColumns+` FROM investigation_cases WHERE case_id = $1`, caseID))
		return err
	})
	return out, err
}

// AppendEvidence appends one hash-chained evidence item. The case row is
// locked FOR UPDATE so chain heads serialize; contentHash must be the
// lowercase hex SHA-256 of the evidence artefact (computed by the caller —
// the store never sees the artefact itself).
func (store *Store) AppendEvidence(ctx context.Context, tenantID string, row EvidenceRow) (EvidenceRow, error) {
	var out EvidenceRow
	err := store.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		var state string
		err := tx.QueryRow(ctx, `SELECT state FROM investigation_cases WHERE case_id = $1 FOR UPDATE`,
			row.CaseID).Scan(&state)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrSafetyNotFound
		}
		if err != nil {
			return err
		}
		if state != "EVIDENCE" {
			return fmt.Errorf("%w: evidence may only be appended in EVIDENCE state (now %s)",
				ErrSafetyInvalidTransition, state)
		}
		prev := EvidenceGenesisHash
		var head *string
		err = tx.QueryRow(ctx, `SELECT chain_hash FROM investigation_evidence
			WHERE case_id = $1 ORDER BY collected_at DESC, evidence_id DESC LIMIT 1`, row.CaseID).Scan(&head)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		if head != nil {
			prev = *head
		}
		chainHash := EvidenceChainHash(prev, row.ContentHash)
		return tx.QueryRow(ctx, `INSERT INTO investigation_evidence
			(evidence_id, case_id, description, content_hash, prev_chain_hash, chain_hash, collected_by)
			VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING evidence_id, case_id, description, content_hash,
				prev_chain_hash, chain_hash, collected_by, collected_at`,
			row.EvidenceID, row.CaseID, row.Description, row.ContentHash, prev, chainHash,
			row.CollectedBy).Scan(&out.EvidenceID, &out.CaseID, &out.Description, &out.ContentHash,
			&out.PrevChainHash, &out.ChainHash, &out.CollectedBy, &out.CollectedAt)
	})
	return out, err
}

func (store *Store) ListEvidence(ctx context.Context, tenantID, caseID string) ([]EvidenceRow, error) {
	out := []EvidenceRow{}
	err := store.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT evidence_id, case_id, description, content_hash,
			prev_chain_hash, chain_hash, collected_by, collected_at
			FROM investigation_evidence WHERE case_id = $1 ORDER BY collected_at, evidence_id`, caseID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var r EvidenceRow
			if err := rows.Scan(&r.EvidenceID, &r.CaseID, &r.Description, &r.ContentHash,
				&r.PrevChainHash, &r.ChainHash, &r.CollectedBy, &r.CollectedAt); err != nil {
				return err
			}
			out = append(out, r)
		}
		return rows.Err()
	})
	return out, err
}

// VerifyEvidenceChain recomputes the hash chain for one case and returns
// false at the first broken link (genesis expected on the oldest item).
func (store *Store) VerifyEvidenceChain(ctx context.Context, tenantID, caseID string) (bool, error) {
	items, err := store.ListEvidence(ctx, tenantID, caseID)
	if err != nil {
		return false, err
	}
	prev := EvidenceGenesisHash
	for _, item := range items {
		if item.PrevChainHash != prev || item.ChainHash != EvidenceChainHash(prev, item.ContentHash) {
			return false, nil
		}
		prev = item.ChainHash
	}
	return true, nil
}

func (store *Store) AddFinding(ctx context.Context, tenantID string, row FindingRow) (FindingRow, error) {
	var out FindingRow
	err := store.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		var state string
		err := tx.QueryRow(ctx, `SELECT state FROM investigation_cases WHERE case_id = $1 FOR UPDATE`,
			row.CaseID).Scan(&state)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrSafetyNotFound
		}
		if err != nil {
			return err
		}
		if state != "ANALYSIS" && state != "REPORTED" {
			return fmt.Errorf("%w: findings may only be recorded in ANALYSIS or REPORTED state (now %s)",
				ErrSafetyInvalidTransition, state)
		}
		return tx.QueryRow(ctx, `INSERT INTO investigation_findings (finding_id, case_id, finding, created_by)
			VALUES ($1,$2,$3,$4) RETURNING finding_id, case_id, finding, created_by, created_at`,
			row.FindingID, row.CaseID, row.Finding, row.CreatedBy).Scan(
			&out.FindingID, &out.CaseID, &out.Finding, &out.CreatedBy, &out.CreatedAt)
	})
	return out, err
}

func (store *Store) AddRecommendation(ctx context.Context, tenantID string, row RecommendationRow) (RecommendationRow, error) {
	var out RecommendationRow
	err := store.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		var state string
		err := tx.QueryRow(ctx, `SELECT state FROM investigation_cases WHERE case_id = $1 FOR UPDATE`,
			row.CaseID).Scan(&state)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrSafetyNotFound
		}
		if err != nil {
			return err
		}
		if state != "ANALYSIS" && state != "REPORTED" {
			return fmt.Errorf("%w: recommendations may only be recorded in ANALYSIS or REPORTED state (now %s)",
				ErrSafetyInvalidTransition, state)
		}
		return tx.QueryRow(ctx, `INSERT INTO investigation_recommendations
			(recommendation_id, case_id, recommendation, created_by)
			VALUES ($1,$2,$3,$4) RETURNING recommendation_id, case_id, recommendation, status,
				created_by, created_at, decided_by, decided_at`,
			row.RecommendationID, row.CaseID, row.Recommendation, row.CreatedBy).Scan(
			&out.RecommendationID, &out.CaseID, &out.Recommendation, &out.Status, &out.CreatedBy,
			&out.CreatedAt, &out.DecidedBy, &out.DecidedAt)
	})
	return out, err
}

// DecideRecommendation: PROPOSED -> ACCEPTED|REJECTED (decision ledger);
// ACCEPTED -> IMPLEMENTED.
func (store *Store) DecideRecommendation(ctx context.Context, tenantID, recommendationID, actor, action string) (RecommendationRow, error) {
	var out RecommendationRow
	err := store.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		var status string
		err := tx.QueryRow(ctx, `SELECT status FROM investigation_recommendations
			WHERE recommendation_id = $1 FOR UPDATE`, recommendationID).Scan(&status)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrSafetyNotFound
		}
		if err != nil {
			return err
		}
		var query string
		switch {
		case (action == "accept" || action == "reject") && status == "PROPOSED":
			next := "ACCEPTED"
			if action == "reject" {
				next = "REJECTED"
			}
			query = `UPDATE investigation_recommendations SET status = '` + next + `',
				decided_by = $2, decided_at = now() WHERE recommendation_id = $1`
		case action == "implement" && status == "ACCEPTED":
			query = `UPDATE investigation_recommendations SET status = 'IMPLEMENTED'
				WHERE recommendation_id = $1`
		default:
			return fmt.Errorf("%w: %s -> %s", ErrSafetyInvalidTransition, status, action)
		}
		var execErr error
		if action == "implement" {
			_, execErr = tx.Exec(ctx, query, recommendationID)
		} else {
			_, execErr = tx.Exec(ctx, query, recommendationID, actor)
		}
		if execErr != nil {
			return execErr
		}
		return tx.QueryRow(ctx, `SELECT recommendation_id, case_id, recommendation, status,
			created_by, created_at, decided_by, decided_at FROM investigation_recommendations
			WHERE recommendation_id = $1`, recommendationID).Scan(
			&out.RecommendationID, &out.CaseID, &out.Recommendation, &out.Status, &out.CreatedBy,
			&out.CreatedAt, &out.DecidedBy, &out.DecidedAt)
	})
	return out, err
}

func (store *Store) ListFindings(ctx context.Context, tenantID, caseID string) ([]FindingRow, error) {
	out := []FindingRow{}
	err := store.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT finding_id, case_id, finding, created_by, created_at
			FROM investigation_findings WHERE case_id = $1 ORDER BY created_at`, caseID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var r FindingRow
			if err := rows.Scan(&r.FindingID, &r.CaseID, &r.Finding, &r.CreatedBy, &r.CreatedAt); err != nil {
				return err
			}
			out = append(out, r)
		}
		return rows.Err()
	})
	return out, err
}

func (store *Store) ListRecommendations(ctx context.Context, tenantID, caseID string) ([]RecommendationRow, error) {
	out := []RecommendationRow{}
	err := store.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT recommendation_id, case_id, recommendation, status,
			created_by, created_at, decided_by, decided_at
			FROM investigation_recommendations WHERE case_id = $1 ORDER BY created_at`, caseID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var r RecommendationRow
			if err := rows.Scan(&r.RecommendationID, &r.CaseID, &r.Recommendation, &r.Status,
				&r.CreatedBy, &r.CreatedAt, &r.DecidedBy, &r.DecidedAt); err != nil {
				return err
			}
			out = append(out, r)
		}
		return rows.Err()
	})
	return out, err
}
