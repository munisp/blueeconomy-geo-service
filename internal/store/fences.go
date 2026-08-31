package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// FenceRow is one persisted geofence version (WP-10).
type FenceRow struct {
	GeofenceID                 string
	Version                    int
	TenantID                   string
	Name                       string
	Classification             string
	VerticesMicros             json.RawMessage // closed ring [[lat,lon],...]
	DwellThresholdSeconds      int
	DwellSpeedGateMilliknots   int
	State                      string
	CreatedBy                  string
	CreatedAt                  time.Time
	RetiredAt                  *time.Time
}

// FenceEventRow is one persisted fence transition.
type FenceEventRow struct {
	EventID         string
	GeofenceID      string
	GeofenceVersion int
	TenantID        string
	EventType       string
	MMSI            string
	LatitudeMicros  int32
	LongitudeMicros int32
	Classification  string
	EnvelopeDigest  string
	OccurredAt      time.Time
}

// QueueObservationRow is one recorded port queue-length observation.
type QueueObservationRow struct {
	PortCode     string
	QueueLength  int
	Source       string
	ObservedAt   time.Time
}

// TrackPointRow is one recorded position for track queries.
type TrackPointRow struct {
	LatitudeMicros   int32
	LongitudeMicros  int32
	SogMilliknots    int32 // -1 when NULL in storage
	ObservedAt       time.Time
}

// NearestVesselRow is one nearest-vessels result.
type NearestVesselRow struct {
	MMSI            string
	ShipName        string
	LatitudeMicros  int32
	LongitudeMicros int32
	SogMilliknots   int32
	DistanceMeters  float64
	ObservedAt      time.Time
}

const fenceSelect = `SELECT geofence_id, version, tenant_id, name, classification,
	vertices_micros, dwell_threshold_seconds, dwell_speed_gate_milliknots,
	state, created_by, created_at, retired_at FROM geofences`

func scanGeofence(row pgx.Row) (FenceRow, error) {
	var r FenceRow
	err := row.Scan(&r.GeofenceID, &r.Version, &r.TenantID, &r.Name, &r.Classification,
		&r.VerticesMicros, &r.DwellThresholdSeconds, &r.DwellSpeedGateMilliknots,
		&r.State, &r.CreatedBy, &r.CreatedAt, &r.RetiredAt)
	return r, err
}

// ListActiveGeofences returns the single ACTIVE version of every geofence
// the tenant may see (clearance-floor enforced by the caller's labels).
func (store *Store) ListActiveGeofences(ctx context.Context, tenantID string, clearedLabels []string) ([]FenceRow, error) {
	rows, err := store.pool.Query(ctx, fenceSelect+` WHERE state = 'ACTIVE' AND tenant_id = $1 AND classification = ANY($2) ORDER BY geofence_id`, tenantID, clearedLabels)
	if err != nil {
		return nil, fmt.Errorf("list geofences: %w", err)
	}
	defer rows.Close()
	out := []FenceRow{}
	for rows.Next() {
		r, err := scanGeofence(rows)
		if err != nil {
			return nil, fmt.Errorf("scan geofence: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// GetGeofenceHistory returns every version of one geofence, newest first.
func (store *Store) GetGeofenceHistory(ctx context.Context, tenantID, geofenceID string) ([]FenceRow, error) {
	rows, err := store.pool.Query(ctx, fenceSelect+` WHERE geofence_id = $1 AND tenant_id = $2 ORDER BY version DESC`, geofenceID, tenantID)
	if err != nil {
		return nil, fmt.Errorf("get geofence history: %w", err)
	}
	defer rows.Close()
	out := []FenceRow{}
	for rows.Next() {
		r, err := scanGeofence(rows)
		if err != nil {
			return nil, fmt.Errorf("scan geofence: %w", err)
		}
		out = append(out, r)
	}
	if len(out) == 0 {
		return nil, pgx.ErrNoRows
	}
	return out, rows.Err()
}

// CreateGeofenceVersion inserts version 1 of a new geofence, or the next
// version of an existing one (atomically retiring the previously ACTIVE
// row). The PostGIS geography is derived from the integer vertex ring at the
// storage boundary — no float round-trip. It fails closed when the geofence
// exists and expectedVersion does not match the current latest version
// (optimistic concurrency).
func (store *Store) CreateGeofenceVersion(ctx context.Context, row FenceRow, expectedVersion int) (FenceRow, error) {
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return FenceRow{}, fmt.Errorf("begin geofence write: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var latest int
	err = tx.QueryRow(ctx, `SELECT COALESCE(MAX(version), 0) FROM geofences WHERE geofence_id = $1 AND tenant_id = $2`, row.GeofenceID, row.TenantID).Scan(&latest)
	if err != nil {
		return FenceRow{}, fmt.Errorf("read geofence version: %w", err)
	}
	if latest != expectedVersion {
		return FenceRow{}, fmt.Errorf("VERSION_CONFLICT: latest version is %d, expected %d", latest, expectedVersion)
	}
	next := latest + 1

	// Build WKT from the integer ring (micro-degrees → decimal text).
	var vertices [][2]int32
	if err := json.Unmarshal(row.VerticesMicros, &vertices); err != nil {
		return FenceRow{}, fmt.Errorf("vertices_micros must be [[lat,lon],...]: %w", err)
	}
	wkt, err := polygonWKT(vertices)
	if err != nil {
		return FenceRow{}, err
	}

	if _, err := tx.Exec(ctx, `UPDATE geofences SET state = 'RETIRED', retired_at = now() WHERE geofence_id = $1 AND tenant_id = $2 AND state = 'ACTIVE'`, row.GeofenceID, row.TenantID); err != nil {
		return FenceRow{}, fmt.Errorf("retire prior version: %w", err)
	}
	var created FenceRow
	err = tx.QueryRow(ctx, `INSERT INTO geofences
		(geofence_id, version, tenant_id, name, classification, geom, vertices_micros,
		 dwell_threshold_seconds, dwell_speed_gate_milliknots, state, created_by)
		VALUES ($1,$2,$3,$4,$5, ST_GeogFromText($6), $7, $8, $9, 'ACTIVE', $10)
		RETURNING geofence_id, version, tenant_id, name, classification, vertices_micros,
		 dwell_threshold_seconds, dwell_speed_gate_milliknots, state, created_by, created_at, retired_at`,
		row.GeofenceID, next, row.TenantID, row.Name, row.Classification, wkt, row.VerticesMicros,
		row.DwellThresholdSeconds, row.DwellSpeedGateMilliknots, row.CreatedBy).
		Scan(&created.GeofenceID, &created.Version, &created.TenantID, &created.Name, &created.Classification,
			&created.VerticesMicros, &created.DwellThresholdSeconds, &created.DwellSpeedGateMilliknots,
			&created.State, &created.CreatedBy, &created.CreatedAt, &created.RetiredAt)
	if err != nil {
		return FenceRow{}, fmt.Errorf("insert geofence version: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return FenceRow{}, fmt.Errorf("commit geofence write: %w", err)
	}
	return created, nil
}

// RetireGeofence retires the ACTIVE version (no new version). Returns
// pgx.ErrNoRows when nothing was active.
func (store *Store) RetireGeofence(ctx context.Context, tenantID, geofenceID string) error {
	tag, err := store.pool.Exec(ctx, `UPDATE geofences SET state = 'RETIRED', retired_at = now() WHERE geofence_id = $1 AND tenant_id = $2 AND state = 'ACTIVE'`, geofenceID, tenantID)
	if err != nil {
		return fmt.Errorf("retire geofence: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

// polygonWKT renders a closed ring as WKT in lon/lat order with integer
// decimal rendering (no float round-trip).
func polygonWKT(vertices [][2]int32) (string, error) {
	if len(vertices) < 4 {
		return "", errors.New("ring must have at least 4 vertices")
	}
	parts := make([]string, len(vertices))
	for i, v := range vertices {
		parts[i] = fmt.Sprintf("%s %s", microsText(v[1]), microsText(v[0]))
	}
	return "SRID=4326;POLYGON((" + strings.Join(parts, ", ") + "))", nil
}

// microsText renders a fixed-point micro-degree integer as decimal text.
func microsText(v int32) string {
	neg := v < 0
	abs := int64(v)
	if neg {
		abs = -abs
	}
	whole := abs / 1_000_000
	frac := abs % 1_000_000
	s := fmt.Sprintf("%d.%06d", whole, frac)
	if neg {
		s = "-" + s
	}
	return s
}

// InsertGeofenceEvent persists one transition with its envelope digest.
// Events with an empty digest are stored but hidden from reads (unprovenanced).
func (store *Store) InsertGeofenceEvent(ctx context.Context, ev FenceEventRow) error {
	_, err := store.pool.Exec(ctx, `INSERT INTO geofence_events
		(event_id, geofence_id, geofence_version, tenant_id, event_type, mmsi,
		 latitude_micros, longitude_micros, classification, envelope_digest, occurred_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
		ev.EventID, ev.GeofenceID, ev.GeofenceVersion, ev.TenantID, ev.EventType, ev.MMSI,
		ev.LatitudeMicros, ev.LongitudeMicros, ev.Classification, ev.EnvelopeDigest, ev.OccurredAt)
	if err != nil {
		return fmt.Errorf("insert geofence event: %w", err)
	}
	return nil
}

// ListGeofenceEvents returns provenanced (digest-bearing) events, newest
// first. Unprovenanced rows (empty digest) are never served.
func (store *Store) ListGeofenceEvents(ctx context.Context, tenantID, geofenceID string, clearedLabels []string, limit int) ([]FenceEventRow, error) {
	rows, err := store.pool.Query(ctx, `SELECT event_id, geofence_id, geofence_version, event_type, mmsi,
		latitude_micros, longitude_micros, classification, envelope_digest, occurred_at
		FROM geofence_events
		WHERE tenant_id = $1 AND geofence_id = $2 AND classification = ANY($3) AND envelope_digest <> ''
		ORDER BY occurred_at DESC LIMIT $4`, tenantID, geofenceID, clearedLabels, limit)
	if err != nil {
		return nil, fmt.Errorf("list geofence events: %w", err)
	}
	defer rows.Close()
	out := []FenceEventRow{}
	for rows.Next() {
		var r FenceEventRow
		if err := rows.Scan(&r.EventID, &r.GeofenceID, &r.GeofenceVersion, &r.EventType, &r.MMSI,
			&r.LatitudeMicros, &r.LongitudeMicros, &r.Classification, &r.EnvelopeDigest, &r.OccurredAt); err != nil {
			return nil, fmt.Errorf("scan geofence event: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// QueryTrack returns the time-windowed recorded track for one vessel,
// time-ascending. NULL speeds scan as -1 (unknown, never zero-filled).
func (store *Store) QueryTrack(ctx context.Context, mmsi string, from, to time.Time, clearedLabels []string, limit int) ([]TrackPointRow, error) {
	rows, err := store.pool.Query(ctx, `SELECT latitude_micros, longitude_micros,
		COALESCE(speed_over_ground_milliknots, -1), observed_at
		FROM ais_positions
		WHERE mmsi = $1 AND observed_at BETWEEN $2 AND $3 AND classification = ANY($4)
		ORDER BY observed_at ASC LIMIT $5`, mmsi, from, to, clearedLabels, limit)
	if err != nil {
		return nil, fmt.Errorf("query track: %w", err)
	}
	defer rows.Close()
	out := []TrackPointRow{}
	for rows.Next() {
		var r TrackPointRow
		if err := rows.Scan(&r.LatitudeMicros, &r.LongitudeMicros, &r.SogMilliknots, &r.ObservedAt); err != nil {
			return nil, fmt.Errorf("scan track point: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// NearestVessels returns the k nearest vessels with a recorded position
// inside the radius, using the PostGIS geography index on the hot table.
func (store *Store) NearestVessels(ctx context.Context, latMicros, lonMicros int32, radiusMeters float64, clearedLabels []string, limit int) ([]NearestVesselRow, error) {
	point := fmt.Sprintf("SRID=4326;POINT(%s %s)", microsText(lonMicros), microsText(latMicros))
	rows, err := store.pool.Query(ctx, `SELECT DISTINCT ON (mmsi) mmsi, ship_name,
		latitude_micros, longitude_micros, COALESCE(speed_over_ground_milliknots, -1),
		ST_Distance(geom, ST_GeogFromText($1)) AS distance_m, observed_at
		FROM ais_positions
		WHERE mmsi IS NOT NULL AND classification = ANY($2)
		  AND ST_DWithin(geom, ST_GeogFromText($1), $3)
		ORDER BY mmsi, observed_at DESC`, point, clearedLabels, radiusMeters)
	if err != nil {
		return nil, fmt.Errorf("nearest vessels: %w", err)
	}
	defer rows.Close()
	var latest []NearestVesselRow
	for rows.Next() {
		var r NearestVesselRow
		if err := rows.Scan(&r.MMSI, &r.ShipName, &r.LatitudeMicros, &r.LongitudeMicros, &r.SogMilliknots, &r.DistanceMeters, &r.ObservedAt); err != nil {
			return nil, fmt.Errorf("scan nearest vessel: %w", err)
		}
		latest = append(latest, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// Sort by distance ascending (the DISTINCT ON ordering is per-vessel).
	for i := 1; i < len(latest); i++ {
		for j := i; j > 0 && latest[j].DistanceMeters < latest[j-1].DistanceMeters; j-- {
			latest[j], latest[j-1] = latest[j-1], latest[j]
		}
	}
	if len(latest) > limit {
		latest = latest[:limit]
	}
	return latest, nil
}

// LatestPosition returns the freshest recorded position of one vessel.
func (store *Store) LatestPosition(ctx context.Context, mmsi string, clearedLabels []string) (TrackPointRow, error) {
	var r TrackPointRow
	err := store.pool.QueryRow(ctx, `SELECT latitude_micros, longitude_micros,
		COALESCE(speed_over_ground_milliknots, -1), observed_at
		FROM ais_positions WHERE mmsi = $1 AND classification = ANY($2)
		ORDER BY observed_at DESC LIMIT 1`, mmsi, clearedLabels).
		Scan(&r.LatitudeMicros, &r.LongitudeMicros, &r.SogMilliknots, &r.ObservedAt)
	if err != nil {
		return r, err
	}
	return r, nil
}

// QueueObservations returns the recorded queue series for one port,
// time-ascending, from a start time.
func (store *Store) QueueObservations(ctx context.Context, portCode string, since time.Time, limit int) ([]QueueObservationRow, error) {
	rows, err := store.pool.Query(ctx, `SELECT port_code, queue_length, source, observed_at
		FROM port_queue_observations WHERE port_code = $1 AND observed_at >= $2
		ORDER BY observed_at ASC LIMIT $3`, portCode, since, limit)
	if err != nil {
		return nil, fmt.Errorf("queue observations: %w", err)
	}
	defer rows.Close()
	out := []QueueObservationRow{}
	for rows.Next() {
		var r QueueObservationRow
		if err := rows.Scan(&r.PortCode, &r.QueueLength, &r.Source, &r.ObservedAt); err != nil {
			return nil, fmt.Errorf("scan queue observation: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// InsertQueueObservation records one queue-length observation (ingest path
// for eCallUp / gate-event projections).
func (store *Store) InsertQueueObservation(ctx context.Context, row QueueObservationRow) error {
	_, err := store.pool.Exec(ctx, `INSERT INTO port_queue_observations (port_code, queue_length, source, observed_at)
		VALUES ($1,$2,$3,$4)
		ON CONFLICT (port_code, observed_at) DO UPDATE SET queue_length = EXCLUDED.queue_length, source = EXCLUDED.source`,
		row.PortCode, row.QueueLength, row.Source, row.ObservedAt)
	if err != nil {
		return fmt.Errorf("insert queue observation: %w", err)
	}
	return nil
}
