// MRV domain operations. Every mutation persists the domain row and the
// signed outbox envelope in one transaction and fails closed: unknown fuel
// grade (no source-cited factor), missing confirmed monitoring plan,
// idempotency divergence, four-eyes conflicts and illegal state transitions
// all abort the transaction.
package mrv

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

var (
	imoPattern  = regexp.MustCompile(`^[0-9]{7}$`)
	mmsiPattern = regexp.MustCompile(`^[0-9]{9}$`)
)

// RegisterShip registers one in-scope ship (mrv-flag-admin). dcs_scope is
// computed from the configured GT threshold and the declared international
// voyage flag; the MMSI link is optional until the AIS link is confirmed.
func (service *Service) RegisterShip(ctx context.Context, actor string, ship Ship) (Ship, error) {
	if !imoPattern.MatchString(ship.ImoNumber) {
		return Ship{}, errors.New("imoNumber must be exactly 7 digits")
	}
	if ship.MMSI != "" && !mmsiPattern.MatchString(ship.MMSI) {
		return Ship{}, errors.New("mmsi must be exactly 9 digits")
	}
	if strings.TrimSpace(ship.ShipName) == "" || ship.GT == 0 {
		return Ship{}, errors.New("shipName and gt are required")
	}
	ship.DcsScope = uint64(ship.GT) >= uint64(service.DcsGTThreshold) && ship.InternationalVoyages
	if ship.FlagState == "" {
		ship.FlagState = "NG"
	}
	ship.RegisteredBy = actor
	err := service.withActor(ctx, actor, func(tx pgx.Tx) error {
		var mmsi *string
		if ship.MMSI != "" {
			mmsi = &ship.MMSI
		}
		_, err := tx.Exec(ctx, `INSERT INTO mrv_ships
			(imo_number, mmsi, ship_name, gt, dwt, ship_type, flag_state, international_voyages, dcs_scope, registered_by)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
			ship.ImoNumber, mmsi, ship.ShipName, ship.GT, ship.DWT, ship.ShipType, ship.FlagState,
			ship.InternationalVoyages, ship.DcsScope, actor)
		return err
	})
	if err != nil {
		return Ship{}, fmt.Errorf("register ship: %w", err)
	}
	ship.CreatedAt = time.Now().UTC()
	return ship, nil
}

// GetShip returns one registered ship.
func (service *Service) GetShip(ctx context.Context, actor, imoNumber string) (Ship, error) {
	var ship Ship
	var mmsi, registeredBy *string
	var dwt *uint32
	err := service.withActor(ctx, actor, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT imo_number, mmsi, ship_name, gt, dwt, ship_type, flag_state,
			international_voyages, dcs_scope, registered_by, created_at
			FROM mrv_ships WHERE imo_number = $1`, imoNumber).
			Scan(&ship.ImoNumber, &mmsi, &ship.ShipName, &ship.GT, &dwt, &ship.ShipType, &ship.FlagState,
				&ship.InternationalVoyages, &ship.DcsScope, &registeredBy, &ship.CreatedAt)
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return Ship{}, ErrShipNotFound
	}
	if err != nil {
		return Ship{}, fmt.Errorf("get ship: %w", err)
	}
	if mmsi != nil {
		ship.MMSI = *mmsi
	}
	ship.DWT = dwt
	if registeredBy != nil {
		ship.RegisteredBy = *registeredBy
	}
	return ship, nil
}

// PutMonitoringPlan registers a new plan version for the ship (DRAFT).
// methods maps consumer type -> EU MRV Annex I method ("A".."D").
func (service *Service) PutMonitoringPlan(ctx context.Context, actor, imoNumber string, methods map[string]string, fuelGrades []string) (MonitoringPlan, error) {
	if len(methods) == 0 || len(fuelGrades) == 0 {
		return MonitoringPlan{}, errors.New("methods and fuelGrades are required")
	}
	for consumer, method := range methods {
		switch consumer {
		case ConsumerMainEngine, ConsumerAuxEngine, ConsumerBoiler, ConsumerInertGas, ConsumerNotUnderWay:
		default:
			return MonitoringPlan{}, fmt.Errorf("methods key %q is not a DCS consumer type", consumer)
		}
		switch method {
		case MethodA, MethodB, MethodC, MethodD:
		default:
			return MonitoringPlan{}, fmt.Errorf("method %q is not an EU MRV Annex I method", method)
		}
	}
	for _, grade := range fuelGrades {
		if strings.TrimSpace(grade) == "" {
			return MonitoringPlan{}, errors.New("fuelGrades must not contain empty grades")
		}
	}
	plan := MonitoringPlan{PlanID: uuid.NewString(), ImoNumber: imoNumber, State: PlanStateDraft, CreatedBy: actor}
	methodsJSON, err := json.Marshal(methods)
	if err != nil {
		return MonitoringPlan{}, err
	}
	plan.Methods = methodsJSON
	plan.FuelGrades = fuelGrades
	err = service.withActor(ctx, actor, func(tx pgx.Tx) error {
		var exists bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM mrv_ships WHERE imo_number = $1)`, imoNumber).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			return ErrShipNotFound
		}
		if err := tx.QueryRow(ctx, `SELECT COALESCE(MAX(version), 0) + 1 FROM mrv_monitoring_plans WHERE imo_number = $1`,
			imoNumber).Scan(&plan.Version); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `INSERT INTO mrv_monitoring_plans
			(plan_id, imo_number, version, methods, fuel_grades, state, created_by)
			VALUES ($1,$2,$3,$4,$5,'DRAFT',$6)`,
			plan.PlanID, imoNumber, plan.Version, methodsJSON, fuelGrades, actor)
		return err
	})
	if err != nil {
		return MonitoringPlan{}, err
	}
	plan.CreatedAt = time.Now().UTC()
	return plan, nil
}

// ConfirmMonitoringPlan confirms a plan (mrv-verifier; maker <> checker).
// Confirmation supersedes any previously CONFIRMED plan of the ship.
func (service *Service) ConfirmMonitoringPlan(ctx context.Context, actor, planID string) (MonitoringPlan, error) {
	var plan MonitoringPlan
	err := service.withActor(ctx, actor, func(tx pgx.Tx) error {
		var createdBy, state string
		err := tx.QueryRow(ctx, `SELECT imo_number, version, methods, fuel_grades, state, created_by
			FROM mrv_monitoring_plans WHERE plan_id = $1 FOR UPDATE`, planID).
			Scan(&plan.ImoNumber, &plan.Version, &plan.Methods, &plan.FuelGrades, &state, &createdBy)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrPlanNotFound
		}
		if err != nil {
			return err
		}
		if createdBy == actor {
			return ErrMakerCheckerConflict
		}
		if state != PlanStateDraft && state != PlanStateSubmitted {
			return fmt.Errorf("%w: %s -> CONFIRMED", ErrPlanState, state)
		}
		now := time.Now().UTC().Truncate(time.Microsecond)
		if _, err := tx.Exec(ctx, `UPDATE mrv_monitoring_plans SET state = 'SUPERSEDED'
			WHERE imo_number = $1 AND state = 'CONFIRMED'`, plan.ImoNumber); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE mrv_monitoring_plans
			SET state = 'CONFIRMED', confirmed_by = $2, confirmed_at = $3 WHERE plan_id = $1`,
			planID, actor, now); err != nil {
			return err
		}
		plan.PlanID = planID
		plan.State = PlanStateConfirmed
		plan.CreatedBy = createdBy
		plan.ConfirmedBy = actor
		plan.ConfirmedAt = &now
		return nil
	})
	if err != nil {
		return MonitoringPlan{}, err
	}
	return plan, nil
}

// SubmitFuelReport records one operator fuel/activity report (DCS record
// unit). Idempotent on the Idempotency-Key (external_ref): an identical
// replay returns the stored report; a divergent replay conflicts. Fails
// closed when no source-cited CO2 factor resolves for the grade at the
// reporting period, and when no CONFIRMED monitoring plan covers the
// reported method and grade.
func (service *Service) SubmitFuelReport(ctx context.Context, actor, imoNumber, idempotencyKey string, report FuelReport) (FuelReport, bool, error) {
	if strings.TrimSpace(idempotencyKey) == "" {
		return FuelReport{}, false, ErrIdempotencyKeyNeeded
	}
	if err := validateFuelReport(&report); err != nil {
		return FuelReport{}, false, err
	}
	report.ImoNumber = imoNumber
	report.ExternalRef = idempotencyKey
	report.ReportedBy = actor
	digest, err := digestEvidence(report.Evidence)
	if err != nil {
		return FuelReport{}, false, err
	}
	report.EvidenceDigestSha256 = digest
	replayed := false
	err = service.withActor(ctx, actor, func(tx pgx.Tx) error {
		var exists bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM mrv_ships WHERE imo_number = $1)`, imoNumber).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			return ErrShipNotFound
		}
		// Collection requires a CONFIRMED monitoring plan covering the
		// reported consumer method and fuel grade (SEEMP Part II analog).
		var planOK bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (
			SELECT 1 FROM mrv_monitoring_plans
			WHERE imo_number = $1 AND state = 'CONFIRMED'
			  AND methods ->> $2 = $3 AND $4 = ANY (fuel_grades))`,
			imoNumber, report.Consumer, report.Method, report.FuelGrade).Scan(&planOK); err != nil {
			return err
		}
		if !planOK {
			return ErrNoConfirmedPlan
		}
		// Fail-closed factor resolution: the grade must resolve to a
		// source-cited CO2 factor at the reporting period, else no record.
		if _, err := ResolveFactor(ctx, tx, report.FuelGrade, "CO2", report.PeriodTo); err != nil {
			return err
		}
		// Idempotent replay on external_ref.
		var existingID string
		var existing FuelReport
		scanErr := tx.QueryRow(ctx, `SELECT report_id, period_from, period_to, consumer, fuel_grade, method,
			fuel_tonnes_milli, distance_nm_milli, hours_underway_minutes, COALESCE(bdn_ref, ''),
			evidence_digest_sha256, reported_by, created_at
			FROM mrv_fuel_reports WHERE external_ref = $1`, idempotencyKey).
			Scan(&existingID, &existing.PeriodFrom, &existing.PeriodTo, &existing.Consumer, &existing.FuelGrade,
				&existing.Method, &existing.FuelTonnesMilli, &existing.DistanceNmMilli, &existing.HoursUnderwayMinutes,
				&existing.BdnRef, &existing.EvidenceDigestSha256, &existing.ReportedBy, &existing.CreatedAt)
		if scanErr == nil {
			if existing.PeriodFrom.Equal(report.PeriodFrom) && existing.PeriodTo.Equal(report.PeriodTo) &&
				existing.Consumer == report.Consumer && existing.FuelGrade == report.FuelGrade &&
				existing.Method == report.Method && existing.FuelTonnesMilli == report.FuelTonnesMilli &&
				uint64PtrEqual(existing.DistanceNmMilli, report.DistanceNmMilli) &&
				uint64PtrEqual(existing.HoursUnderwayMinutes, report.HoursUnderwayMinutes) &&
				existing.BdnRef == report.BdnRef {
				existing.ReportID = existingID
				existing.ImoNumber = imoNumber
				existing.ExternalRef = idempotencyKey
				report = existing
				replayed = true
				return nil
			}
			return ErrIdempotencyConflict
		}
		if !errors.Is(scanErr, pgx.ErrNoRows) {
			return scanErr
		}
		report.ReportID = uuid.NewString()
		now := time.Now().UTC().Truncate(time.Microsecond)
		report.CreatedAt = now
		if len(report.Evidence) == 0 {
			report.Evidence = json.RawMessage(`{}`)
		}
		var bdn *string
		if report.BdnRef != "" {
			bdn = &report.BdnRef
		}
		if _, err := tx.Exec(ctx, `INSERT INTO mrv_fuel_reports
			(report_id, imo_number, external_ref, period_from, period_to, consumer, fuel_grade, method,
			 fuel_tonnes_milli, distance_nm_milli, hours_underway_minutes, bdn_ref, evidence,
			 evidence_digest_sha256, reported_by, created_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)`,
			report.ReportID, imoNumber, idempotencyKey, report.PeriodFrom, report.PeriodTo, report.Consumer,
			report.FuelGrade, report.Method, report.FuelTonnesMilli, report.DistanceNmMilli,
			report.HoursUnderwayMinutes, bdn, report.Evidence, report.EvidenceDigestSha256, actor, now); err != nil {
			return err
		}
		resource := FuelReportResource{
			ReportID: report.ReportID, ImoNumber: imoNumber, ExternalReference: idempotencyKey,
			PeriodFrom: report.PeriodFrom, PeriodTo: report.PeriodTo, Consumer: report.Consumer,
			FuelGrade: report.FuelGrade, Method: report.Method, FuelTonnesMilli: U64(report.FuelTonnesMilli),
			BdnReference: report.BdnRef, EvidenceDigestSha256: report.EvidenceDigestSha256, ReportedAt: now,
		}
		if report.DistanceNmMilli != nil {
			value := U64(*report.DistanceNmMilli)
			resource.DistanceNmMilli = &value
		}
		if report.HoursUnderwayMinutes != nil {
			value := U64(*report.HoursUnderwayMinutes)
			resource.HoursUnderwayMinutes = &value
		}
		_, err := service.enqueueOutbox(ctx, tx, EventFuelReport, imoNumber, report.ReportID, resource, now, "")
		return err
	})
	if err != nil {
		return FuelReport{}, false, err
	}
	return report, replayed, nil
}

func validateFuelReport(report *FuelReport) error {
	if report.PeriodFrom.IsZero() || report.PeriodTo.IsZero() || !report.PeriodTo.After(report.PeriodFrom) {
		return errors.New("periodFrom/periodTo are required and periodTo must be after periodFrom")
	}
	switch report.Consumer {
	case ConsumerMainEngine, ConsumerAuxEngine, ConsumerBoiler, ConsumerInertGas, ConsumerNotUnderWay:
	default:
		return fmt.Errorf("consumer %q is not a DCS consumer type", report.Consumer)
	}
	switch report.Method {
	case MethodA, MethodB, MethodC, MethodD:
	default:
		return fmt.Errorf("method %q is not an EU MRV Annex I method", report.Method)
	}
	if report.Method == MethodA && strings.TrimSpace(report.BdnRef) == "" {
		return errors.New("bdnReference is mandatory for method A (bunker delivery note grade capture)")
	}
	if strings.TrimSpace(report.FuelGrade) == "" {
		return errors.New("fuelGrade (ISO 8217 viscosity grade key) is required")
	}
	return nil
}

func uint64PtrEqual(a, b *uint64) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}
