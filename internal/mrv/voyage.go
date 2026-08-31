// Voyage ledger (BOSP/EOSP, EU-MRV-compatible) and the AIS-derived activity
// estimate read model. The server attaches geofence zone-event evidence
// digests from the geo position plane; raw evidence never crosses the
// event boundary.
package mrv

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// RecordVoyage records one voyage-ledger entry (mrv-reporter). The server
// attaches AIS geofence evidence: digests of the ship's port-zone enter/exit
// events overlapping the sea passage, when the MMSI link exists.
func (service *Service) RecordVoyage(ctx context.Context, actor, imoNumber string, voyage Voyage) (Voyage, error) {
	if voyage.BospAt != nil && voyage.EospAt != nil && !voyage.EospAt.After(*voyage.BospAt) {
		return Voyage{}, errors.New("eospAt must be after bospAt")
	}
	voyage.Source = VoyageSourceOperator
	voyage.RecordedBy = actor
	err := service.withActor(ctx, actor, func(tx pgx.Tx) error {
		var mmsi *string
		var exists bool
		if err := tx.QueryRow(ctx, `SELECT true, mmsi FROM mrv_ships WHERE imo_number = $1`, imoNumber).
			Scan(&exists, &mmsi); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrShipNotFound
			}
			return err
		}
		digests := []string{}
		if mmsi != nil {
			found, err := geofenceEvidenceDigests(ctx, tx, *mmsi, voyage.BospAt, voyage.EospAt)
			if err != nil {
				return err
			}
			digests = found
		}
		evidenceJSON, err := json.Marshal(digests)
		if err != nil {
			return err
		}
		voyage.VoyageID = uuid.NewString()
		now := time.Now().UTC().Truncate(time.Microsecond)
		voyage.CreatedAt = now
		voyage.GeofenceEvidence = evidenceJSON
		if _, err := tx.Exec(ctx, `INSERT INTO mrv_voyages
			(voyage_id, imo_number, bosp_at, bosp_port, eosp_at, eosp_port, cargo_tonnes_milli,
			 laden_distance_nm_milli, source, geofence_evidence, recorded_by, created_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
			voyage.VoyageID, imoNumber, voyage.BospAt, nilIfEmpty(voyage.BospPort), voyage.EospAt,
			nilIfEmpty(voyage.EospPort), voyage.CargoTonnesMilli, voyage.LadenDistanceNmMilli,
			voyage.Source, evidenceJSON, actor, now); err != nil {
			return err
		}
		resource := VoyageResource{
			VoyageID: voyage.VoyageID, ImoNumber: imoNumber, Source: voyage.Source,
			BospAt: voyage.BospAt, BospPortCode: voyage.BospPort, EospAt: voyage.EospAt,
			EospPortCode: voyage.EospPort, GeofenceEvidenceDigestSha256: digests, RecordedAt: now,
		}
		if voyage.CargoTonnesMilli != nil {
			value := U64(*voyage.CargoTonnesMilli)
			resource.CargoTonnesMilli = &value
		}
		if voyage.LadenDistanceNmMilli != nil {
			value := U64(*voyage.LadenDistanceNmMilli)
			resource.LadenDistanceNmMilli = &value
		}
		_, err = service.enqueueOutbox(ctx, tx, EventVoyage, imoNumber, voyage.VoyageID, resource, now, "")
		return err
	})
	if err != nil {
		return Voyage{}, err
	}
	return voyage, nil
}

// geofenceEvidenceDigests digests the ship's geofence zone events
// overlapping [bosp, eosp] (or a bounded window around the recorded bound
// when only one is present). Digests are per-row sha256 of the canonical
// event row, so evidence stays inside the boundary.
func geofenceEvidenceDigests(ctx context.Context, tx pgx.Tx, mmsi string, bosp, eosp *time.Time) ([]string, error) {
	if bosp == nil && eosp == nil {
		return []string{}, nil
	}
	from, to := time.Time{}, time.Time{}
	switch {
	case bosp != nil && eosp != nil:
		from, to = *bosp, *eosp
	case bosp != nil:
		from, to = bosp.Add(-24*time.Hour), bosp.Add(24*time.Hour)
	default:
		from, to = eosp.Add(-24*time.Hour), eosp.Add(24*time.Hour)
	}
	rows, err := tx.Query(ctx, `SELECT geofence_event_id, zone_id, event, occurred_at
		FROM geofence_events
		WHERE mmsi = $1 AND occurred_at >= $2 AND occurred_at <= $3
		ORDER BY occurred_at LIMIT 100`, mmsi, from.UTC(), to.UTC())
	if err != nil {
		return nil, fmt.Errorf("geofence evidence query: %w", err)
	}
	defer rows.Close()
	digests := []string{}
	for rows.Next() {
		var id, zone, event string
		var occurred time.Time
		if err := rows.Scan(&id, &zone, &event, &occurred); err != nil {
			return nil, err
		}
		sum := sha256.Sum256([]byte(fmt.Sprintf("%s|%s|%s|%s", id, zone, event, occurred.UTC().Format(time.RFC3339))))
		digests = append(digests, "sha256:"+hex.EncodeToString(sum[:]))
	}
	return digests, rows.Err()
}

// EstimateActivityForShip computes the AIS-derived activity estimate for a
// ship over [from, to) from the position plane, and records the signed
// mrv.activity-estimate.v1 event (aggregate = mmsi). Positions are filtered
// to the labels the caller's clearance covers, inheriting the shared-plane
// classification doctrine.
func (service *Service) EstimateActivityForShip(ctx context.Context, actor, imoNumber string, from, to time.Time, clearedLabels []string) (ActivityEstimateResource, error) {
	if !from.Before(to) {
		return ActivityEstimateResource{}, errors.New("from must be before to")
	}
	if len(clearedLabels) == 0 {
		return ActivityEstimateResource{}, errors.New("caller clearance covers no position classifications")
	}
	var result ActivityEstimateResource
	err := service.withActor(ctx, actor, func(tx pgx.Tx) error {
		var mmsi *string
		err := tx.QueryRow(ctx, `SELECT mmsi FROM mrv_ships WHERE imo_number = $1`, imoNumber).Scan(&mmsi)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrShipNotFound
		}
		if err != nil {
			return err
		}
		if mmsi == nil {
			return errors.New("ship has no confirmed AIS link (mmsi is NULL): activity estimate is not computable")
		}
		rows, err := tx.Query(ctx, `SELECT observed_at, latitude_micros, longitude_micros,
			COALESCE(speed_over_ground_milliknots, 0)
			FROM ais_positions
			WHERE mmsi = $1 AND observed_at >= $2 AND observed_at < $3 AND classification = ANY($4)
			ORDER BY observed_at`, *mmsi, from.UTC(), to.UTC(), clearedLabels)
		if err != nil {
			return fmt.Errorf("activity position query: %w", err)
		}
		defer rows.Close()
		fixes := make([]ActivityFix, 0)
		for rows.Next() {
			var fix ActivityFix
			if err := rows.Scan(&fix.ObservedAt, &fix.LatMicros, &fix.LonMicros, &fix.SogMilliknots); err != nil {
				return err
			}
			fixes = append(fixes, fix)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		estimate, err := EstimateActivity(fixes, from, to, service.ActivityParams)
		if err != nil {
			return err
		}
		now := time.Now().UTC().Truncate(time.Microsecond)
		result = ActivityEstimateResource{
			EstimateID: uuid.NewString(), ImoNumber: imoNumber, Mmsi: *mmsi,
			PeriodFrom: from.UTC(), PeriodTo: to.UTC(),
			DistanceNmMilli: U64(estimate.DistanceNmMilli), HoursUnderwayMinutes: U64(estimate.HoursUnderwayMinutes),
			InsufficientCoverage: estimate.InsufficientCoverage, InputDigestSha256: estimate.InputDigestSha256,
			ComputedAt: now,
		}
		_, err = service.enqueueOutbox(ctx, tx, EventActivityEstimate, *mmsi, result.EstimateID, result, now, "")
		return err
	})
	if err != nil {
		return ActivityEstimateResource{}, err
	}
	return result, nil
}

func nilIfEmpty(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}
