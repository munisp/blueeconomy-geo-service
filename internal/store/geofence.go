package store

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"
)

// Zone is a governed geofence zone.
type Zone struct {
	ZoneID              string
	TenantID            string
	Name                string
	ClassificationFloor string
	State               string
	MakerPrincipalID    string
	CreatedAt           time.Time
	ApprovedBy          string
	ApprovedAt          *time.Time
}

// GeofenceEvent is a persisted zone crossing awaiting signed publication.
type GeofenceEvent struct {
	GeofenceEventID string
	TenantID        string
	ZoneID          string
	ZoneName        string
	Event           string // ENTER | EXIT
	MMSI            string
	TrackReference  string
	LatitudeMicros  int32
	LongitudeMicros int32
	Classification  string
	OccurredAt      time.Time
}

// Transition is the pure outcome of comparing current zone membership with
// the previously known membership of one vessel.
type Transition struct {
	ZoneID    string
	Direction string // ENTER | EXIT
}

// ComputeTransitions is the pure geofence enter/exit decision: zones present
// in current but not previous are ENTER; present in previous but not current
// are EXIT. Input slices need not be sorted; output is deterministic.
func ComputeTransitions(current, previous []string) []Transition {
	currentSet := make(map[string]struct{}, len(current))
	for _, zone := range current {
		currentSet[zone] = struct{}{}
	}
	previousSet := make(map[string]struct{}, len(previous))
	for _, zone := range previous {
		previousSet[zone] = struct{}{}
	}
	transitions := make([]Transition, 0)
	for zone := range currentSet {
		if _, wasInside := previousSet[zone]; !wasInside {
			transitions = append(transitions, Transition{ZoneID: zone, Direction: "ENTER"})
		}
	}
	for zone := range previousSet {
		if _, isInside := currentSet[zone]; !isInside {
			transitions = append(transitions, Transition{ZoneID: zone, Direction: "EXIT"})
		}
	}
	sort.Slice(transitions, func(i, j int) bool {
		if transitions[i].ZoneID != transitions[j].ZoneID {
			return transitions[i].ZoneID < transitions[j].ZoneID
		}
		return transitions[i].Direction < transitions[j].Direction
	})
	return transitions
}

const zoneStateKeyPrefix = "geo:zonestate:"

// ZoneStateStore keeps per-vessel zone membership in Redis
// (SET geo:zonestate:<vessel> <zone_id>...).
type ZoneStateStore struct {
	client redis.UniversalClient
}

// NewZoneStateStore builds the state store.
func NewZoneStateStore(client redis.UniversalClient) (*ZoneStateStore, error) {
	if client == nil {
		return nil, errors.New("zone state redis client is required")
	}
	return &ZoneStateStore{client: client}, nil
}

// Membership returns the vessel's currently recorded zone ids.
func (state *ZoneStateStore) Membership(ctx context.Context, vesselKey string) ([]string, error) {
	members, err := state.client.SMembers(ctx, zoneStateKeyPrefix+vesselKey).Result()
	if err != nil {
		return nil, fmt.Errorf("read zone state %q: %w", vesselKey, err)
	}
	return members, nil
}

// Replace atomically swaps the vessel's zone membership set.
func (state *ZoneStateStore) Replace(ctx context.Context, vesselKey string, zoneIDs []string) error {
	key := zoneStateKeyPrefix + vesselKey
	pipe := state.client.TxPipeline()
	pipe.Del(ctx, key)
	if len(zoneIDs) > 0 {
		members := make([]any, len(zoneIDs))
		for i, zoneID := range zoneIDs {
			members[i] = zoneID
		}
		pipe.SAdd(ctx, key, members...)
		pipe.Expire(ctx, key, 24*time.Hour)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("replace zone state %q: %w", vesselKey, err)
	}
	return nil
}

// withIngestConn runs fn inside a transaction on the dedicated ingest
// pool, whose connection authenticates AS the geo_ingest role
// (0008_rls_ingest_login.sql). The geofence evaluator is the only
// platform-wide reader/writer: the application role is default-deny when
// app.tenant_id is unset and holds NO geo_ingest membership — privilege
// separation is by connection, because permissive RLS policies OR across
// role memberships and would otherwise re-open cross-tenant reads.
func (store *Store) withIngestConn(ctx context.Context, fn func(tx pgx.Tx) error) error {
	tx, err := store.ingest.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin ingest transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// zonesContaining returns the approved zones intersecting the point. It
// runs on the ingest connection (geo_ingest role) — the only platform-wide zone
// read path.
func zonesContaining(ctx context.Context, tx pgx.Tx, latitudeMicros, longitudeMicros int32) ([]Zone, error) {
	rows, err := tx.Query(ctx, `SELECT zone_id, tenant_id, name, classification_floor
		FROM geofence_zones
		WHERE state = 'approved' AND ST_Intersects(geom, ST_GeogFromText($1))`,
		pointWKT(latitudeMicros, longitudeMicros))
	if err != nil {
		return nil, fmt.Errorf("query zones containing point: %w", err)
	}
	defer rows.Close()
	zones := make([]Zone, 0, 4)
	for rows.Next() {
		var zone Zone
		if err := rows.Scan(&zone.ZoneID, &zone.TenantID, &zone.Name, &zone.ClassificationFloor); err != nil {
			return nil, fmt.Errorf("scan zone row: %w", err)
		}
		zones = append(zones, zone)
	}
	return zones, rows.Err()
}

// EvaluateGeofences computes enter/exit transitions for one vessel position
// against approved zones, updates the Redis zone state, persists the
// geofence events and returns them for signed publication. Event
// classification starts at the zone's classification floor. All database
// work runs on the dedicated geo_ingest-role connection in one transaction.
func (store *Store) EvaluateGeofences(ctx context.Context, state *ZoneStateStore, position Position, eventID func() string) ([]GeofenceEvent, error) {
	if state == nil {
		return nil, errors.New("zone state store is required")
	}
	vesselKey := position.MMSI
	if vesselKey == "" {
		vesselKey = position.VesselRef
	}
	if vesselKey == "" {
		return nil, errors.New("geofence evaluation requires a vessel identity")
	}
	events := make([]GeofenceEvent, 0, 2)
	err := store.withIngestConn(ctx, func(tx pgx.Tx) error {
		zones, err := zonesContaining(ctx, tx, position.LatitudeMicros, position.LongitudeMicros)
		if err != nil {
			return err
		}
		current := make([]string, 0, len(zones))
		byID := make(map[string]Zone, len(zones))
		for _, zone := range zones {
			current = append(current, zone.ZoneID)
			byID[zone.ZoneID] = zone
		}
		previous, err := state.Membership(ctx, vesselKey)
		if err != nil {
			return err
		}
		transitions := ComputeTransitions(current, previous)
		if err := state.Replace(ctx, vesselKey, current); err != nil {
			return err
		}
		for _, transition := range transitions {
			zone := byID[transition.ZoneID]
			if transition.Direction == "EXIT" {
				// Zone metadata may be unavailable after exit (zone deleted or
				// point outside); resolve name/tenant from the recorded state
				// where possible.
				lookup, err := zonesByID(ctx, tx, transition.ZoneID)
				if err != nil {
					return err
				}
				if len(lookup) == 1 {
					zone = lookup[0]
				} else {
					continue
				}
			}
			event := GeofenceEvent{
				GeofenceEventID: eventID(),
				TenantID:        zone.TenantID,
				ZoneID:          zone.ZoneID,
				ZoneName:        zone.Name,
				Event:           transition.Direction,
				MMSI:            position.MMSI,
				TrackReference:  position.VesselRef,
				LatitudeMicros:  position.LatitudeMicros,
				LongitudeMicros: position.LongitudeMicros,
				Classification:  zone.ClassificationFloor,
				OccurredAt:      position.ObservedAt.UTC(),
			}
			if _, err := tx.Exec(ctx, `INSERT INTO geofence_events (
				geofence_event_id, tenant_id, zone_id, zone_name, event, mmsi, track_reference,
				latitude_micros, longitude_micros, classification, occurred_at)
				VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
				event.GeofenceEventID, event.TenantID, event.ZoneID, event.ZoneName, event.Event,
				nullString(event.MMSI), nullString(event.TrackReference),
				event.LatitudeMicros, event.LongitudeMicros, event.Classification, event.OccurredAt); err != nil {
				return fmt.Errorf("insert geofence event %s: %w", event.GeofenceEventID, err)
			}
			events = append(events, event)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return events, nil
}

// zonesByID resolves a zone regardless of state (EXIT bookkeeping after a
// zone reverted to draft). Ingest-role transaction only.
func zonesByID(ctx context.Context, tx pgx.Tx, zoneID string) ([]Zone, error) {
	rows, err := tx.Query(ctx, `SELECT zone_id, tenant_id, name, classification_floor, state
		FROM geofence_zones WHERE zone_id = $1`, zoneID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	zones := make([]Zone, 0, 1)
	for rows.Next() {
		var zone Zone
		if err := rows.Scan(&zone.ZoneID, &zone.TenantID, &zone.Name, &zone.ClassificationFloor, &zone.State); err != nil {
			return nil, err
		}
		zones = append(zones, zone)
	}
	return zones, rows.Err()
}

// nullString maps an empty string to nil for nullable columns.
func nullString(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}
