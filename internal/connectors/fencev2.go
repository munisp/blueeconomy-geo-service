package connectors

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/munisp/blueeconomy-geo-service/internal/fence"
	"github.com/munisp/blueeconomy-geo-service/internal/sign"
	"github.com/munisp/blueeconomy-geo-service/internal/store"
)

// FenceV2Store is the ingest-side persistence boundary of the WP-10 fence
// evaluator: platform-wide ACTIVE fence reads and transition-event writes,
// both on the dedicated geo_ingest role connection (0014/0017).
type FenceV2Store interface {
	ListActiveGeofencesPlatform(ctx context.Context) ([]store.FenceRow, error)
	InsertGeofenceEventIngest(ctx context.Context, ev store.FenceEventRow) error
}

// FenceV2Publisher is the signed-envelope boundary (the pipeline itself in
// production; a recorder in tests).
type FenceV2Publisher interface {
	PublishSignedEnvelope(ctx context.Context, eventType, correlationID string, payload any, occurredAt time.Time, classification string, headers map[string]string) error
}

// FenceV2Evaluator wires the WP-10 fence engine into the ingest hot path
// (G11, GEO_FENCE_V2_INGEST=true): every validated position is folded into
// the engine against the current ACTIVE geofence set, and transitions are
// announced as signed geo.geofence-event.v1 envelopes and persisted to
// geofence_transition_events — the same artifacts the push-only
// POST /v1/geo/fences/evaluate endpoint produces. Fail-closed doctrine is
// unchanged: a transition that cannot be announced or persisted aborts the
// report (the caller sees the error; nothing is silently dropped).
type FenceV2Evaluator struct {
	Store FenceV2Store
	// ReloadInterval bounds fence-set staleness (default 60s).
	ReloadInterval time.Duration

	mu          sync.Mutex
	engine      *fence.Engine
	fences      []fence.Fence
	meta        map[string]store.FenceRow // geofenceID -> ACTIVE row (name, tenant, classification)
	loadedAt    time.Time
	now         func() time.Time
}

// NewFenceV2Evaluator validates the wiring fail-closed.
func NewFenceV2Evaluator(storage FenceV2Store) (*FenceV2Evaluator, error) {
	if storage == nil {
		return nil, errors.New("fence v2 evaluator store is required")
	}
	return &FenceV2Evaluator{
		Store: storage, ReloadInterval: time.Minute,
		engine: fence.NewEngine(), meta: map[string]store.FenceRow{}, now: time.Now,
	}, nil
}

// fencesLocked returns the current fence set, reloading when stale.
func (evaluator *FenceV2Evaluator) fencesLocked(ctx context.Context) ([]fence.Fence, error) {
	if evaluator.fences != nil && evaluator.now().Sub(evaluator.loadedAt) < evaluator.ReloadInterval {
		return evaluator.fences, nil
	}
	rows, err := evaluator.Store.ListActiveGeofencesPlatform(ctx)
	if err != nil {
		return nil, fmt.Errorf("FENCE_STORE_UNAVAILABLE: %w", err)
	}
	fences := make([]fence.Fence, 0, len(rows))
	meta := make(map[string]store.FenceRow, len(rows))
	for _, row := range rows {
		var raw [][2]int32
		if err := json.Unmarshal(row.VerticesMicros, &raw); err != nil {
			return nil, fmt.Errorf("FENCE_STORE_CORRUPT: persisted ring unreadable for %s", row.GeofenceID)
		}
		vertices := make([]fence.Point, len(raw))
		for i, v := range raw {
			vertices[i] = fence.Point{LatMicros: v[0], LonMicros: v[1]}
		}
		fences = append(fences, fence.Fence{
			GeofenceID: row.GeofenceID, Version: row.Version, Vertices: vertices,
			DwellThresholdSeconds: row.DwellThresholdSeconds, DwellSpeedGateMilliknots: row.DwellSpeedGateMilliknots,
		})
		meta[row.GeofenceID] = row
	}
	evaluator.fences = fences
	evaluator.meta = meta
	evaluator.loadedAt = evaluator.now()
	return fences, nil
}

// ObservePosition folds one validated ingest position into the engine and
// announces/persists the resulting transitions. Positions without a valid
// MMSI (Tier-0 app reports) are outside the v2 fence contract and skip
// silently, exactly as the evaluate endpoint rejects them per-report.
func (evaluator *FenceV2Evaluator) ObservePosition(ctx context.Context, publisher FenceV2Publisher, position store.Position) error {
	if publisher == nil {
		return errors.New("FENCE_EVENTS_UNWIRED: geo.geofence-event.v1 publisher is not configured")
	}
	if !mmsiPattern(position.MMSI) {
		return nil
	}
	speed := -1
	if position.SpeedOverGroundMilliknots != nil {
		speed = int(*position.SpeedOverGroundMilliknots)
	}
	evaluator.mu.Lock()
	fences, err := evaluator.fencesLocked(ctx)
	if err != nil {
		evaluator.mu.Unlock()
		return err
	}
	events := evaluator.engine.Observe(position.MMSI,
		fence.Point{LatMicros: position.LatitudeMicros, LonMicros: position.LongitudeMicros},
		speed, position.ObservedAt.UTC().Unix(), fences)
	type outcome struct {
		event fence.Event
		row   store.FenceRow
	}
	outcomes := make([]outcome, 0, len(events))
	for _, event := range events {
		outcomes = append(outcomes, outcome{event: event, row: evaluator.meta[event.GeofenceID]})
	}
	evaluator.mu.Unlock()

	for _, item := range outcomes {
		event := item.event
		row := item.row
		classification := row.Classification
		if _, err := sign.ParseClassification(classification); err != nil {
			classification = "INTERNAL" // defense in depth; the write boundary validates
		}
		eventID := "gfv2-" + uuid.NewString()
		occurredAt := time.Unix(event.OccurredAtUnix, 0).UTC()
		payload := sign.GeofenceEventRecorded{
			GeofenceEventID: eventID,
			ZoneID:          event.GeofenceID,
			ZoneName:        row.Name, // G13: the fence name, never the id
			Event:           string(event.Type),
			MMSI:            position.MMSI,
			LatitudeMicros:  position.LatitudeMicros,
			LongitudeMicros: position.LongitudeMicros,
			OccurredAt:      occurredAt,
			Classification:  classification,
		}
		canonical, _ := json.Marshal(payload)
		digest := sha256.Sum256(canonical)
		// Fail-closed: announce first; a transition that cannot be announced
		// is not persisted (mirrors POST /v1/geo/fences/evaluate).
		if err := publisher.PublishSignedEnvelope(ctx, sign.EventGeofenceEvent,
			eventID, payload, occurredAt, classification, map[string]string{"producer": "geo-fence-engine"}); err != nil {
			return fmt.Errorf("FENCE_EVENT_PUBLISH_FAILED: %w", err)
		}
		if err := evaluator.Store.InsertGeofenceEventIngest(ctx, store.FenceEventRow{
			EventID: eventID, GeofenceID: event.GeofenceID, GeofenceVersion: event.Version,
			TenantID: row.TenantID, EventType: string(event.Type), MMSI: position.MMSI,
			LatitudeMicros: position.LatitudeMicros, LongitudeMicros: position.LongitudeMicros,
			Classification: classification, EnvelopeDigest: hex.EncodeToString(digest[:]), OccurredAt: occurredAt,
		}); err != nil {
			return fmt.Errorf("FENCE_EVENT_PERSIST_FAILED: %w", err)
		}
	}
	return nil
}

// mmsiPattern mirrors the contract MMSI shape (^[0-9]{9}$) without pulling
// the api package into the hot path.
func mmsiPattern(value string) bool {
	if len(value) != 9 {
		return false
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}
