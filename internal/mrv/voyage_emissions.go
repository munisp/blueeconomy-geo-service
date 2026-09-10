// Per-voyage emissions (G8, MRV dashboard API #20): CO2 allocated to one
// BOSP/EOSP voyage window from the operator fuel reports overlapping it,
// plus the AIS-derived activity estimate for the same window as the
// cross-check. Allocation is time-proportional over the overlap (integer
// nano-second arithmetic; fixed-point milli-tonnes in/out), factors resolve
// per row from the source-cited registry — an unresolvable grade fails
// closed, no estimate is produced. Every computation is announced on the
// signed mrv.voyage-emissions.v1 outbox event, so the read is as auditable
// as the annual compile.
package mrv

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// EventVoyageEmissions is the per-voyage emissions computation event.
const EventVoyageEmissions = "mrv.voyage-emissions.v1"

// ErrVoyageWindowIncomplete marks voyages without both BOSP and EOSP: the
// window is unknown, so per-voyage CO2 is honestly not computable yet.
var ErrVoyageWindowIncomplete = errors.New("voyage has no complete BOSP/EOSP window: per-voyage emissions are not computable until both bounds are recorded")

// VoyageGradeEmissions is one fuel grade's allocated share of the voyage.
type VoyageGradeEmissions struct {
	FuelGrade               string `json:"fuelGrade"`
	AllocatedFuelTonnesMilli U64   `json:"allocatedFuelTonnesMilli"`
	CO2TonnesMilli          U64    `json:"co2TonnesMilli"`
	FactorNano              U64    `json:"factorNano"`
	FactorCitation          string `json:"factorCitation"`
}

// VoyageEmissionsResource is the mrv.voyage-emissions.v1 payload and the
// REST response document.
type VoyageEmissionsResource struct {
	ComputationID    string                  `json:"computationId"`
	VoyageID         string                  `json:"voyageId"`
	ImoNumber        string                  `json:"imoNumber"`
	BospAt           time.Time               `json:"bospAt"`
	EospAt           time.Time               `json:"eospAt"`
	PerGrade         []VoyageGradeEmissions  `json:"perGrade"`
	TotalCO2TonnesMilli U64                  `json:"totalCo2TonnesMilli"`
	AllocatedFuelTonnesMilli U64             `json:"allocatedFuelTonnesMilli"`
	FuelReportCount  int                     `json:"fuelReportCount"`
	// AllocationMethod documents the estimator honestly on the wire.
	AllocationMethod string                  `json:"allocationMethod"`
	// ActivityCrosscheck is the AIS-derived window estimate (never a
	// substitute for reported fuel).
	ActivityCrosscheck *ActivityEstimateResource `json:"activityCrosscheck,omitempty"`
	ComputedAt       time.Time               `json:"computedAt"`
}

// allocateOverlap returns the fuel share of [periodFrom, periodTo) falling
// inside [windowFrom, windowTo), time-proportional in integer nanoseconds.
// Degenerate (non-positive) periods allocate zero rather than dividing by
// zero (fail-closed).
func allocateOverlap(periodFrom, periodTo, windowFrom, windowTo time.Time, fuelTonnesMilli uint64) uint64 {
	start := periodFrom
	if windowFrom.After(start) {
		start = windowFrom
	}
	end := periodTo
	if windowTo.Before(end) {
		end = windowTo
	}
	overlap := end.Sub(start)
	period := periodTo.Sub(periodFrom)
	if overlap <= 0 || period <= 0 {
		return 0
	}
	// fuel * overlap / period; the intermediate product (milli-tonnes x
	// nanoseconds) exceeds uint64 for real windows, so the multiply-divide
	// runs in big integers and the result (<= fuel) fits uint64 by
	// construction.
	numerator := new(big.Int).Mul(new(big.Int).SetUint64(fuelTonnesMilli), big.NewInt(overlap.Nanoseconds()))
	numerator.Div(numerator, big.NewInt(period.Nanoseconds()))
	return numerator.Uint64()
}

// VoyageEmissions computes the per-voyage CO2 allocation for one recorded
// voyage and enqueues the signed mrv.voyage-emissions.v1 outbox event.
func (service *Service) VoyageEmissions(ctx context.Context, actor, imoNumber, voyageID string, clearedLabels []string) (VoyageEmissionsResource, error) {
	var result VoyageEmissionsResource
	err := service.withActor(ctx, actor, func(tx pgx.Tx) error {
		var voyage Voyage
		var mmsi *string
		err := tx.QueryRow(ctx, `SELECT v.voyage_id, v.imo_number, v.bosp_at, v.eosp_at, s.mmsi
			FROM mrv_voyages v JOIN mrv_ships s ON s.imo_number = v.imo_number
			WHERE v.voyage_id = $1 AND v.imo_number = $2`, voyageID, imoNumber).
			Scan(&voyage.VoyageID, &voyage.ImoNumber, &voyage.BospAt, &voyage.EospAt, &mmsi)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrVoyageNotFound
		}
		if err != nil {
			return err
		}
		if voyage.BospAt == nil || voyage.EospAt == nil {
			return ErrVoyageWindowIncomplete
		}
		from, to := voyage.BospAt.UTC(), voyage.EospAt.UTC()
		rows, err := tx.Query(ctx, `SELECT fuel_grade, fuel_tonnes_milli, period_from, period_to
			FROM mrv_fuel_reports
			WHERE imo_number = $1 AND period_from < $3 AND period_to > $2
			ORDER BY fuel_grade, period_from`, imoNumber, from, to)
		if err != nil {
			return fmt.Errorf("voyage fuel overlap query: %w", err)
		}
		defer rows.Close()
		perGrade := map[string]*VoyageGradeEmissions{}
		gradeOrder := []string{}
		result = VoyageEmissionsResource{
			ComputationID: uuid.NewString(), VoyageID: voyageID, ImoNumber: imoNumber,
			BospAt: from, EospAt: to,
			PerGrade: []VoyageGradeEmissions{},
			AllocationMethod: "time-proportional overlap of each fuel report period with the BOSP/EOSP voyage window; factors resolved per row from the source-cited registry",
		}
		for rows.Next() {
			var grade string
			var fuelMilli uint64
			var periodFrom, periodTo time.Time
			if err := rows.Scan(&grade, &fuelMilli, &periodFrom, &periodTo); err != nil {
				return err
			}
			allocated := allocateOverlap(periodFrom, periodTo, from, to, fuelMilli)
			if allocated == 0 {
				continue
			}
			factor, err := ResolveFactor(ctx, tx, grade, "CO2", periodTo)
			if err != nil {
				return err // fail closed: no source-cited factor, no estimate
			}
			co2Milli, err := CO2MilliTonnes(allocated, factor.FactorNano)
			if err != nil {
				return err
			}
			agg, ok := perGrade[grade]
			if !ok {
				agg = &VoyageGradeEmissions{FuelGrade: grade, FactorNano: U64(factor.FactorNano), FactorCitation: factor.SourceCitation}
				perGrade[grade] = agg
				gradeOrder = append(gradeOrder, grade)
			}
			agg.AllocatedFuelTonnesMilli += U64(allocated)
			agg.CO2TonnesMilli += U64(co2Milli)
			result.AllocatedFuelTonnesMilli += U64(allocated)
			result.TotalCO2TonnesMilli += U64(co2Milli)
			result.FuelReportCount++
		}
		if err := rows.Err(); err != nil {
			return err
		}
		for _, grade := range gradeOrder {
			result.PerGrade = append(result.PerGrade, *perGrade[grade])
		}
		// AIS cross-check over the same window when the MMSI link exists.
		if mmsi != nil && len(clearedLabels) > 0 {
			fixes, err := activityFixes(ctx, tx, *mmsi, from, to, clearedLabels)
			if err != nil {
				return err
			}
			estimate, err := EstimateActivity(fixes, from, to, service.ActivityParams)
			if err != nil {
				return err
			}
			result.ActivityCrosscheck = &ActivityEstimateResource{
				EstimateID: uuid.NewString(), ImoNumber: imoNumber, Mmsi: *mmsi,
				PeriodFrom: from, PeriodTo: to,
				DistanceNmMilli: U64(estimate.DistanceNmMilli), HoursUnderwayMinutes: U64(estimate.HoursUnderwayMinutes),
				InsufficientCoverage: estimate.InsufficientCoverage, InputDigestSha256: estimate.InputDigestSha256,
				ComputedAt: time.Now().UTC().Truncate(time.Microsecond),
			}
		}
		now := time.Now().UTC().Truncate(time.Microsecond)
		result.ComputedAt = now
		_, err = service.enqueueOutbox(ctx, tx, EventVoyageEmissions, imoNumber, result.ComputationID, result, now, "")
		return err
	})
	if err != nil {
		return VoyageEmissionsResource{}, err
	}
	return result, nil
}

// VoyageEmissionsSummary is the #20 dashboard aggregate: every voyage of
// the ship overlapping the window, its computed CO2, and the voyages that
// could not be computed (incomplete window or no overlapping fuel) listed
// honestly instead of zero-filled.
type VoyageEmissionsSummary struct {
	ImoNumber          string                     `json:"imoNumber"`
	WindowFrom         time.Time                  `json:"windowFrom"`
	WindowTo           time.Time                  `json:"windowTo"`
	Voyages            []VoyageEmissionsResource  `json:"voyages"`
	Skipped            []VoyageSkip               `json:"skipped"`
	TotalCO2TonnesMilli U64                       `json:"totalCo2TonnesMilli"`
	ComputedAt         time.Time                  `json:"computedAt"`
}

// VoyageSkip records why one voyage has no computable emissions.
type VoyageSkip struct {
	VoyageID string `json:"voyageId"`
	Reason   string `json:"reason"`
}

// VoyageEmissionsSummaryForShip computes per-voyage emissions for every
// voyage overlapping [from, to) and aggregates. Individual voyages whose
// window is incomplete are skipped with an honest reason, never estimated.
func (service *Service) VoyageEmissionsSummaryForShip(ctx context.Context, actor, imoNumber string, from, to time.Time, clearedLabels []string) (VoyageEmissionsSummary, error) {
	if !from.Before(to) {
		return VoyageEmissionsSummary{}, errors.New("from must be before to")
	}
	summary := VoyageEmissionsSummary{
		ImoNumber: imoNumber, WindowFrom: from.UTC(), WindowTo: to.UTC(),
		Voyages: []VoyageEmissionsResource{}, Skipped: []VoyageSkip{},
		ComputedAt: time.Now().UTC().Truncate(time.Microsecond),
	}
	var voyageIDs []string
	err := service.withActor(ctx, actor, func(tx pgx.Tx) error {
		var exists bool
		if err := tx.QueryRow(ctx, `SELECT true FROM mrv_ships WHERE imo_number = $1`, imoNumber).Scan(&exists); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrShipNotFound
			}
			return err
		}
		rows, err := tx.Query(ctx, `SELECT voyage_id FROM mrv_voyages
			WHERE imo_number = $1 AND bosp_at IS NOT NULL AND eosp_at IS NOT NULL
			  AND bosp_at < $3 AND eosp_at > $2 ORDER BY bosp_at`, imoNumber, from.UTC(), to.UTC())
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				return err
			}
			voyageIDs = append(voyageIDs, id)
		}
		return rows.Err()
	})
	if err != nil {
		return VoyageEmissionsSummary{}, err
	}
	// Voyages with incomplete windows inside the period are surfaced honestly.
	_ = service.withActor(ctx, actor, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT voyage_id FROM mrv_voyages
			WHERE imo_number = $1 AND (bosp_at IS NULL OR eosp_at IS NULL)
			  AND COALESCE(bosp_at, eosp_at, created_at) >= $2 AND COALESCE(bosp_at, eosp_at, created_at) < $3`,
			imoNumber, from.UTC(), to.UTC())
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				return err
			}
			summary.Skipped = append(summary.Skipped, VoyageSkip{VoyageID: id, Reason: ErrVoyageWindowIncomplete.Error()})
		}
		return rows.Err()
	})
	for _, id := range voyageIDs {
		computed, err := service.VoyageEmissions(ctx, actor, imoNumber, id, clearedLabels)
		if errors.Is(err, ErrVoyageNotFound) || errors.Is(err, ErrVoyageWindowIncomplete) {
			summary.Skipped = append(summary.Skipped, VoyageSkip{VoyageID: id, Reason: err.Error()})
			continue
		}
		if err != nil {
			return VoyageEmissionsSummary{}, err
		}
		summary.Voyages = append(summary.Voyages, computed)
		summary.TotalCO2TonnesMilli += computed.TotalCO2TonnesMilli
	}
	return summary, nil
}

// activityFixes loads the windowed fix list (shared with the activity
// estimate path).
func activityFixes(ctx context.Context, tx pgx.Tx, mmsi string, from, to time.Time, clearedLabels []string) ([]ActivityFix, error) {
	rows, err := tx.Query(ctx, `SELECT observed_at, latitude_micros, longitude_micros,
		COALESCE(speed_over_ground_milliknots, 0)
		FROM ais_positions
		WHERE mmsi = $1 AND observed_at >= $2 AND observed_at < $3 AND classification = ANY($4)
		ORDER BY observed_at`, mmsi, from.UTC(), to.UTC(), clearedLabels)
	if err != nil {
		return nil, fmt.Errorf("activity position query: %w", err)
	}
	defer rows.Close()
	fixes := make([]ActivityFix, 0)
	for rows.Next() {
		var fix ActivityFix
		if err := rows.Scan(&fix.ObservedAt, &fix.LatMicros, &fix.LonMicros, &fix.SogMilliknots); err != nil {
			return nil, err
		}
		fixes = append(fixes, fix)
	}
	return fixes, rows.Err()
}
