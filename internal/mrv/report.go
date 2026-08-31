// Annual report compile/submit, the verification decision workflow and
// Statement of Compliance issuance with the canonical signed artifact.
package mrv

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	jsoncanonicalizer "github.com/cyberphone/json-canonicalization/go/src/webpki.org/jsoncanonicalizer"
)

// reportTotals is the annual aggregation document persisted in
// mrv_annual_reports.totals and carried into the signed artifact.
type reportTotals struct {
	PerGrade             []gradeTotal `json:"perGrade"`
	TotalFuelTonnesMilli U64          `json:"totalFuelTonnesMilli"`
	CO2TonnesMilli       U64          `json:"co2TonnesMilli"`
	DistanceNmMilli      U64          `json:"distanceNmMilli"`
	HoursUnderwayMinutes U64          `json:"hoursUnderwayMinutes"`
	FuelReportCount      int          `json:"fuelReportCount"`
}

type gradeTotal struct {
	FuelGrade       string `json:"fuelGrade"`
	FuelTonnesMilli U64    `json:"fuelTonnesMilli"`
	CO2TonnesMilli  U64    `json:"co2TonnesMilli"`
	FactorNano      U64    `json:"factorNano"`
	FactorCitation  string `json:"factorCitation"`
}

// CompileAnnualReport aggregates the ship's fuel reports for the calendar
// year into a DRAFT annual report (recompile replaces a DRAFT; any other
// state conflicts). CO2 = fuel x factor per report row, factors resolved
// per row at its own period end from the source-cited registry — an
// unresolvable grade aborts the compile (fail closed, no estimate).
func (service *Service) CompileAnnualReport(ctx context.Context, actor, imoNumber string, year int) (AnnualReport, error) {
	if year < 2019 || year > 2200 {
		return AnnualReport{}, errors.New("calendar year out of range")
	}
	var report AnnualReport
	err := service.withActor(ctx, actor, func(tx pgx.Tx) error {
		var ship Ship
		var dwt *uint32
		err := tx.QueryRow(ctx, `SELECT ship_type, gt, dwt FROM mrv_ships WHERE imo_number = $1`, imoNumber).
			Scan(&ship.ShipType, &ship.GT, &dwt)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrShipNotFound
		}
		if err != nil {
			return err
		}
		yearFrom := time.Date(year, 1, 1, 0, 0, 0, 0, time.UTC)
		yearTo := time.Date(year+1, 1, 1, 0, 0, 0, 0, time.UTC)
		rows, err := tx.Query(ctx, `SELECT fuel_grade, fuel_tonnes_milli,
			COALESCE(distance_nm_milli, 0), COALESCE(hours_underway_minutes, 0), period_to
			FROM mrv_fuel_reports
			WHERE imo_number = $1 AND period_from >= $2 AND period_to <= $3
			ORDER BY fuel_grade, period_from`, imoNumber, yearFrom, yearTo)
		if err != nil {
			return fmt.Errorf("aggregate fuel reports: %w", err)
		}
		type row struct {
			grade     string
			fuelMilli uint64
			distance  uint64
			hours     uint64
			periodTo  time.Time
		}
		reportRows := []row{}
		defer rows.Close()
		for rows.Next() {
			var r row
			if err := rows.Scan(&r.grade, &r.fuelMilli, &r.distance, &r.hours, &r.periodTo); err != nil {
				return err
			}
			reportRows = append(reportRows, r)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		// CO2 per grade with per-row factor resolution; every resolved row
		// joins the factor set that factor_set_hash binds.
		perGrade := map[string]*gradeTotal{}
		gradeOrder := []string{}
		factorRows := map[string]FactorRow{}
		totals := reportTotals{PerGrade: []gradeTotal{}}
		for _, r := range reportRows {
			factor, err := ResolveFactor(ctx, tx, r.grade, "CO2", r.periodTo)
			if err != nil {
				return err // fail closed: no source-cited factor, no report
			}
			co2Milli, err := CO2MilliTonnes(r.fuelMilli, factor.FactorNano)
			if err != nil {
				return err
			}
			agg, ok := perGrade[r.grade]
			if !ok {
				agg = &gradeTotal{FuelGrade: r.grade, FactorNano: U64(factor.FactorNano), FactorCitation: factor.SourceCitation}
				perGrade[r.grade] = agg
				gradeOrder = append(gradeOrder, r.grade)
			}
			agg.FuelTonnesMilli += U64(r.fuelMilli)
			agg.CO2TonnesMilli += U64(co2Milli)
			totals.TotalFuelTonnesMilli += U64(r.fuelMilli)
			totals.CO2TonnesMilli += U64(co2Milli)
			totals.DistanceNmMilli += U64(r.distance)
			totals.HoursUnderwayMinutes += U64(r.hours)
			totals.FuelReportCount++
			factorRows[factor.FactorKey+"|"+factor.Gas+"|"+factor.ValidFrom.Format("2006-01-02")] = factor
		}
		for _, grade := range gradeOrder {
			totals.PerGrade = append(totals.PerGrade, *perGrade[grade])
		}
		factorSet := make([]FactorRow, 0, len(factorRows))
		for _, factor := range factorRows {
			factorSet = append(factorSet, factor)
		}
		factorSetHash := FactorSetHash(factorSet)
		totalsJSON, err := json.Marshal(totals)
		if err != nil {
			return err
		}
		// CII from operator-approved configuration only; absent config or
		// coverage yields an honest NOT_COMPUTABLE (NULLs), never an estimate.
		var dwtValue uint64
		if dwt != nil {
			dwtValue = uint64(*dwt)
		}
		cii := service.CII.ComputeCII(ship.ShipType, uint64(ship.GT), dwtValue, year,
			uint64(totals.CO2TonnesMilli), uint64(totals.DistanceNmMilli))
		var attained, required *uint64
		var rating *string
		if !cii.NotComputable {
			a := uint64(*cii.AttainedNano)
			r := uint64(*cii.RequiredNano)
			attained, required = &a, &r
			rating = &cii.Rating
		}
		// Upsert the DRAFT; any other state conflicts.
		var existingID, existingState string
		scanErr := tx.QueryRow(ctx, `SELECT report_id, state FROM mrv_annual_reports
			WHERE imo_number = $1 AND calendar_year = $2 FOR UPDATE`, imoNumber, year).Scan(&existingID, &existingState)
		now := time.Now().UTC().Truncate(time.Microsecond)
		switch {
		case errors.Is(scanErr, pgx.ErrNoRows):
			report = AnnualReport{ReportID: uuid.NewString(), ImoNumber: imoNumber, CalendarYear: year,
				Totals: totalsJSON, FactorSetHash: factorSetHash, State: ReportStateDraft,
				CompiledBy: actor, CreatedAt: now}
			_, err = tx.Exec(ctx, `INSERT INTO mrv_annual_reports
				(report_id, imo_number, calendar_year, totals, attained_cii_nano, required_cii_nano,
				 cii_rating, factor_set_hash, state, compiled_by, created_at)
				VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'DRAFT',$9,$10)`,
				report.ReportID, imoNumber, year, totalsJSON, attained, required, rating, factorSetHash, actor, now)
		case scanErr != nil:
			err = scanErr
		case existingState != ReportStateDraft:
			err = fmt.Errorf("%w: recompile requires DRAFT (state is %s)", ErrReportState, existingState)
		default:
			report = AnnualReport{ReportID: existingID, ImoNumber: imoNumber, CalendarYear: year,
				Totals: totalsJSON, FactorSetHash: factorSetHash, State: ReportStateDraft,
				CompiledBy: actor, CreatedAt: now}
			_, err = tx.Exec(ctx, `UPDATE mrv_annual_reports SET totals = $3, attained_cii_nano = $4,
				required_cii_nano = $5, cii_rating = $6, factor_set_hash = $7, compiled_by = $8
				WHERE report_id = $1 AND state = 'DRAFT'`,
				existingID, year, totalsJSON, attained, required, rating, factorSetHash, actor)
		}
		if err != nil {
			return err
		}
		report.AttainedCiiNano = attained
		report.RequiredCiiNano = required
		if rating != nil {
			report.CiiRating = *rating
		}
		return service.enqueueAnnualEvent(ctx, tx, report, now)
	})
	if err != nil {
		return AnnualReport{}, err
	}
	return report, nil
}

// enqueueAnnualEvent emits the mrv.emissions-annual.v1 envelope.
func (service *Service) enqueueAnnualEvent(ctx context.Context, tx pgx.Tx, report AnnualReport, at time.Time) error {
	var totals struct {
		CO2TonnesMilli       U64 `json:"co2TonnesMilli"`
		DistanceNmMilli      U64 `json:"distanceNmMilli"`
		HoursUnderwayMinutes U64 `json:"hoursUnderwayMinutes"`
	}
	if err := json.Unmarshal(report.Totals, &totals); err != nil {
		return fmt.Errorf("decode report totals: %w", err)
	}
	submittedAt := at
	if report.SubmittedAt != nil {
		submittedAt = *report.SubmittedAt
	}
	resource := EmissionsAnnualResource{
		AnnualReportID: report.ReportID, ImoNumber: report.ImoNumber, CalendarYear: uint32(report.CalendarYear),
		State: report.State, CO2TonnesMilli: totals.CO2TonnesMilli, DistanceNmMilli: totals.DistanceNmMilli,
		HoursUnderwayMinutes: totals.HoursUnderwayMinutes, FactorSetDigestSha256: report.FactorSetHash,
		SubmittedAt: submittedAt,
	}
	if report.AttainedCiiNano != nil {
		value := U64(*report.AttainedCiiNano)
		resource.AttainedCiiNano = &value
	}
	if report.RequiredCiiNano != nil {
		value := U64(*report.RequiredCiiNano)
		resource.RequiredCiiNano = &value
	}
	resource.CiiRating = report.CiiRating
	_, err := service.enqueueOutbox(ctx, tx, EventEmissionsAnnual, report.ReportID, report.ReportID, resource, at, "")
	return err
}

// SubmitAnnualReport transitions DRAFT -> SUBMITTED (the compiling reporter
// is the maker; only the maker submits their own compile).
func (service *Service) SubmitAnnualReport(ctx context.Context, actor, reportID string) (AnnualReport, error) {
	var report AnnualReport
	err := service.withActor(ctx, actor, func(tx pgx.Tx) error {
		loaded, err := lockReport(ctx, tx, reportID)
		if err != nil {
			return err
		}
		if loaded.State != ReportStateDraft {
			return fmt.Errorf("%w: submit requires DRAFT (state is %s)", ErrReportState, loaded.State)
		}
		if loaded.CompiledBy != actor {
			return errors.New("only the compiling reporter submits the report")
		}
		now := time.Now().UTC().Truncate(time.Microsecond)
		if _, err := tx.Exec(ctx, `UPDATE mrv_annual_reports SET state = 'SUBMITTED', submitted_by = $2,
			submitted_at = $3 WHERE report_id = $1`, reportID, actor, now); err != nil {
			return err
		}
		loaded.State = ReportStateSubmitted
		loaded.SubmittedBy = actor
		loaded.SubmittedAt = &now
		report = loaded
		return service.enqueueAnnualEvent(ctx, tx, report, now)
	})
	if err != nil {
		return AnnualReport{}, err
	}
	return report, nil
}

// lockReport loads and locks one annual report.
func lockReport(ctx context.Context, tx pgx.Tx, reportID string) (AnnualReport, error) {
	var report AnnualReport
	var submittedBy, ciiRating *string
	err := tx.QueryRow(ctx, `SELECT report_id, imo_number, calendar_year, totals, attained_cii_nano,
		required_cii_nano, cii_rating, factor_set_hash, state, compiled_by, submitted_by, created_at, submitted_at
		FROM mrv_annual_reports WHERE report_id = $1 FOR UPDATE`, reportID).
		Scan(&report.ReportID, &report.ImoNumber, &report.CalendarYear, &report.Totals, &report.AttainedCiiNano,
			&report.RequiredCiiNano, &ciiRating, &report.FactorSetHash, &report.State, &report.CompiledBy,
			&submittedBy, &report.CreatedAt, &report.SubmittedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return AnnualReport{}, ErrReportNotFound
	}
	if err != nil {
		return AnnualReport{}, err
	}
	if submittedBy != nil {
		report.SubmittedBy = *submittedBy
	}
	if ciiRating != nil {
		report.CiiRating = *ciiRating
	}
	return report, nil
}

// RecordDecision records one immutable verifier decision with the AIS
// cross-check outcome (computed from the position plane when the ship has
// an AIS link). VERIFY -> VERIFIED; REJECT -> REJECTED (terminal);
// REQUEST_CLARIFICATION -> back to SUBMITTED. Maker <> checker is enforced
// against the submitting principal at both the application and the
// database boundary (trigger).
func (service *Service) RecordDecision(ctx context.Context, actor, reportID, decision, reason string) (Verification, CrosscheckResult, error) {
	switch decision {
	case DecisionVerify, DecisionReject, DecisionClarify:
	default:
		return Verification{}, "", fmt.Errorf("decision %q is not a contract decision", decision)
	}
	if decision != DecisionVerify && strings.TrimSpace(reason) == "" {
		return Verification{}, "", errors.New("reason is mandatory for REJECT and REQUEST_CLARIFICATION")
	}
	var verification Verification
	crosscheckResult := CrosscheckNoReportedValues
	err := service.withActor(ctx, actor, func(tx pgx.Tx) error {
		report, err := lockReport(ctx, tx, reportID)
		if err != nil {
			return err
		}
		if report.State != ReportStateSubmitted && report.State != ReportStateVerifierReview {
			return fmt.Errorf("%w: decisions require SUBMITTED or VERIFIER_REVIEW (state is %s)", ErrReportState, report.State)
		}
		if report.SubmittedBy == actor {
			return ErrMakerCheckerConflict
		}
		// AIS cross-check against the report year (cross-check only; the
		// outcome informs the verifier and never mutates reported values).
		crosscheck, crosscheckDigest, result := service.aisCrosscheck(ctx, tx, report)
		crosscheckResult = result
		// State transition: enter VERIFIER_REVIEW, then apply the decision.
		if report.State == ReportStateSubmitted {
			if _, err := tx.Exec(ctx, `UPDATE mrv_annual_reports SET state = 'VERIFIER_REVIEW'
				WHERE report_id = $1`, reportID); err != nil {
				return err
			}
		}
		var final string
		switch decision {
		case DecisionVerify:
			final = ReportStateVerified
		case DecisionReject:
			final = ReportStateRejected
		default:
			final = ReportStateSubmitted
		}
		if _, err := tx.Exec(ctx, `UPDATE mrv_annual_reports SET state = $2 WHERE report_id = $1`, reportID, final); err != nil {
			return err
		}
		now := time.Now().UTC().Truncate(time.Microsecond)
		verification = Verification{
			VerificationID: uuid.NewString(), ReportID: reportID, Decision: decision,
			Verifier: actor, Reason: reason, AISCrosscheck: crosscheck, DecidedAt: now,
		}
		var crosscheckArg any
		if len(crosscheck) > 0 {
			crosscheckArg = crosscheck
		}
		if _, err := tx.Exec(ctx, `INSERT INTO mrv_verifications
			(verification_id, report_id, decision, verifier_principal, reason, ais_crosscheck, decided_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7)`,
			verification.VerificationID, reportID, decision, actor, reason, crosscheckArg, now); err != nil {
			return err
		}
		reasonCode := ""
		if decision != DecisionVerify {
			reasonCode = "OPERATOR_CLARIFICATION_REQUIRED"
			if decision == DecisionReject {
				reasonCode = "VERIFICATION_REJECTED"
			}
		}
		resource := VerificationResource{
			VerificationID: verification.VerificationID, AnnualReportID: reportID, Decision: decision,
			ReasonCode: reasonCode, AISCrosscheckDigestSha256: crosscheckDigest, DecidedAt: now,
		}
		_, err = service.enqueueOutbox(ctx, tx, EventVerification, reportID, verification.VerificationID, resource, now, "")
		return err
	})
	if err != nil {
		return Verification{}, "", err
	}
	return verification, crosscheckResult, nil
}

// aisCrosscheck computes the AIS-derived estimate for the report year and
// classifies it against the reported totals. It returns the cross-check
// record (persisted as JSONB), its digest (carried by the event) and the
// classified result for metrics.
func (service *Service) aisCrosscheck(ctx context.Context, tx pgx.Tx, report AnnualReport) (json.RawMessage, string, CrosscheckResult) {
	record := map[string]any{}
	var mmsi *string
	if err := tx.QueryRow(ctx, `SELECT mmsi FROM mrv_ships WHERE imo_number = $1`, report.ImoNumber).Scan(&mmsi); err != nil || mmsi == nil {
		record["result"] = string(CrosscheckNoReportedValues)
		record["note"] = "no confirmed AIS link"
		return marshalCrosscheck(record)
	}
	from := time.Date(report.CalendarYear, 1, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(report.CalendarYear+1, 1, 1, 0, 0, 0, 0, time.UTC)
	rows, err := tx.Query(ctx, `SELECT observed_at, latitude_micros, longitude_micros,
		COALESCE(speed_over_ground_milliknots, 0)
		FROM ais_positions WHERE mmsi = $1 AND observed_at >= $2 AND observed_at < $3
		ORDER BY observed_at`, *mmsi, from, to)
	if err != nil {
		record["result"] = string(CrosscheckInsufficientCoverage)
		record["note"] = "position plane query failed"
		return marshalCrosscheck(record)
	}
	fixes := []ActivityFix{}
	for rows.Next() {
		var fix ActivityFix
		if err := rows.Scan(&fix.ObservedAt, &fix.LatMicros, &fix.LonMicros, &fix.SogMilliknots); err != nil {
			rows.Close()
			record["result"] = string(CrosscheckInsufficientCoverage)
			return marshalCrosscheck(record)
		}
		fixes = append(fixes, fix)
	}
	rows.Close()
	estimate, err := EstimateActivity(fixes, from, to, service.ActivityParams)
	if err != nil {
		record["result"] = string(CrosscheckInsufficientCoverage)
		return marshalCrosscheck(record)
	}
	var totals struct {
		DistanceNmMilli      U64 `json:"distanceNmMilli"`
		HoursUnderwayMinutes U64 `json:"hoursUnderwayMinutes"`
	}
	_ = json.Unmarshal(report.Totals, &totals)
	result := Crosscheck(estimate, uint64(totals.DistanceNmMilli), uint64(totals.HoursUnderwayMinutes), service.CrosscheckTolerancePermille)
	record["result"] = string(result)
	record["estimatedDistanceNmMilli"] = fmt.Sprintf("%d", estimate.DistanceNmMilli)
	record["estimatedHoursUnderwayMinutes"] = fmt.Sprintf("%d", estimate.HoursUnderwayMinutes)
	record["reportedDistanceNmMilli"] = fmt.Sprintf("%d", uint64(totals.DistanceNmMilli))
	record["reportedHoursUnderwayMinutes"] = fmt.Sprintf("%d", uint64(totals.HoursUnderwayMinutes))
	record["fixCount"] = estimate.FixCount
	record["inputDigestSha256"] = estimate.InputDigestSha256
	return marshalCrosscheck(record)
}

func marshalCrosscheck(record map[string]any) (json.RawMessage, string, CrosscheckResult) {
	raw, _ := json.Marshal(record)
	canonical, err := jsoncanonicalizer.Transform(raw)
	if err != nil {
		canonical = raw
	}
	sum := sha256.Sum256(canonical)
	result, _ := record["result"].(string)
	return raw, "sha256:" + hex.EncodeToString(sum[:]), CrosscheckResult(result)
}

// IssueSoC issues the Statement of Compliance for a VERIFIED annual report
// (mrv-flag-admin): builds the canonical signed JSON artifact (JCS +
// JWS-EdDSA, envelope-signature scheme), anchors its sha256 in
// mrv.soc.v1 provenance.ledgerCommitHash, and fails closed on any other
// state (also trigger-enforced at the database boundary).
func (service *Service) IssueSoC(ctx context.Context, actor, reportID string) (socID string, artifactSha256 string, err error) {
	err = service.withActor(ctx, actor, func(tx pgx.Tx) error {
		report, err := lockReport(ctx, tx, reportID)
		if err != nil {
			return err
		}
		if report.State != ReportStateVerified {
			return fmt.Errorf("%w: a Statement of Compliance requires VERIFIED (state is %s)", ErrReportState, report.State)
		}
		var existing bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM mrv_statements_of_compliance WHERE report_id = $1)`,
			reportID).Scan(&existing); err != nil {
			return err
		}
		if existing {
			return ErrSoCExists
		}
		now := time.Now().UTC().Truncate(time.Microsecond)
		artifact, artifactSum, err := service.buildArtifact(report, now)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO mrv_report_artifacts (report_id, artifact_json, artifact_sha256)
			VALUES ($1,$2,$3)`, reportID, artifact, artifactSum); err != nil {
			return err
		}
		socID = uuid.NewString()
		if _, err := tx.Exec(ctx, `INSERT INTO mrv_statements_of_compliance
			(soc_id, report_id, issued_by, issued_at, artifact_sha256) VALUES ($1,$2,$3,$4,$5)`,
			socID, reportID, actor, now, artifactSum); err != nil {
			return err
		}
		artifactDigest := "sha256:" + artifactSum
		resource := StatementOfComplianceResource{
			SocID: socID, AnnualReportID: reportID, ImoNumber: report.ImoNumber,
			CalendarYear: uint32(report.CalendarYear), ArtifactDigestSha256: artifactDigest, IssuedAt: now,
		}
		// The artifact digest anchors provenance.ledgerCommitHash (gold
		// layer commitment), per docs/mrv-events.md.
		_, err = service.enqueueOutbox(ctx, tx, EventSoC, socID, reportID, resource, now, artifactDigest)
		artifactSha256 = artifactSum
		return err
	})
	if err != nil {
		return "", "", err
	}
	return socID, artifactSha256, nil
}

// buildArtifact renders the canonical signed JSON annual-report artifact:
// the JCS-canonical document signed with the envelope-signature scheme
// (JWS-EdDSA over the JCS payload), sha256-anchored.
func (service *Service) buildArtifact(report AnnualReport, at time.Time) (json.RawMessage, string, error) {
	document := map[string]any{
		"artifactType":          "mrv-annual-report-artifact/v1",
		"annualReportId":        report.ReportID,
		"imoNumber":             report.ImoNumber,
		"calendarYear":          report.CalendarYear,
		"state":                 report.State,
		"factorSetDigestSha256": report.FactorSetHash,
		"issuedAt":              at.Format(time.RFC3339),
		"producer":              ProducerName,
	}
	var totals map[string]any
	if err := json.Unmarshal(report.Totals, &totals); err != nil {
		return nil, "", fmt.Errorf("decode report totals: %w", err)
	}
	document["totals"] = totals
	if report.AttainedCiiNano != nil {
		document["attainedCiiNano"] = fmt.Sprintf("%d", *report.AttainedCiiNano)
		document["requiredCiiNano"] = fmt.Sprintf("%d", *report.RequiredCiiNano)
		document["ciiRating"] = report.CiiRating
	} else {
		document["cii"] = "NOT_COMPUTABLE"
	}
	raw, err := json.Marshal(document)
	if err != nil {
		return nil, "", err
	}
	canonical, err := jsoncanonicalizer.Transform(raw)
	if err != nil {
		return nil, "", fmt.Errorf("canonicalize artifact: %w", err)
	}
	signature, err := service.Signer.SignCanonical(canonical)
	if err != nil {
		return nil, "", fmt.Errorf("sign artifact: %w", err)
	}
	document["signature"] = signature
	full, err := json.Marshal(document)
	if err != nil {
		return nil, "", err
	}
	sum := sha256.Sum256(full)
	return full, hex.EncodeToString(sum[:]), nil
}

// GetAnnualReport returns one annual report.
func (service *Service) GetAnnualReport(ctx context.Context, actor, reportID string) (AnnualReport, error) {
	var report AnnualReport
	err := service.withActor(ctx, actor, func(tx pgx.Tx) error {
		loaded, err := lockReport(ctx, tx, reportID)
		if err != nil {
			return err
		}
		report = loaded
		return nil
	})
	return report, err
}

// GetArtifact returns the canonical signed artifact for a report.
func (service *Service) GetArtifact(ctx context.Context, actor, reportID string) (json.RawMessage, string, error) {
	var artifact json.RawMessage
	var sum string
	err := service.withActor(ctx, actor, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT artifact_json, artifact_sha256 FROM mrv_report_artifacts WHERE report_id = $1`,
			reportID).Scan(&artifact, &sum)
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, "", ErrReportNotFound
	}
	return artifact, sum, err
}
