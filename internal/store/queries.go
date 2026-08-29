package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// VesselSummary is one row of GET /v1/geo/vessels.
type VesselSummary struct {
	MMSI                         string    `json:"mmsi,omitempty"`
	VesselRef                    string    `json:"vesselRef,omitempty"`
	SourceClass                  string    `json:"sourceClass"`
	LatitudeMicros               int32     `json:"latitudeMicros"`
	LongitudeMicros              int32     `json:"longitudeMicros"`
	SpeedOverGroundMilliknots    *uint32   `json:"speedOverGroundMilliknots,omitempty"`
	CourseOverGroundMillidegrees *uint32   `json:"courseOverGroundMillidegrees,omitempty"`
	Classification               string    `json:"classification"`
	ObservedAt                   time.Time `json:"observedAt"`
	ShipName                     string    `json:"shipName,omitempty"`
	ShipTypeCode                 *int32    `json:"shipTypeCode,omitempty"`
}

// Vessel360 is GET /v1/geo/vessels/{mmsi}: latest position, current static
// record and recent zone crossings.
type Vessel360 struct {
	Latest      *VesselSummary `json:"latest,omitempty"`
	Static      *StaticRow     `json:"static,omitempty"`
	RecentZones []GeofenceRow  `json:"recentZones"`
}

// StaticRow is the current SCD-2 vessels_static row.
type StaticRow struct {
	MMSI                string     `json:"mmsi"`
	IMO                 string     `json:"imo,omitempty"`
	Callsign            string     `json:"callsign,omitempty"`
	ShipName            string     `json:"shipName,omitempty"`
	ShipTypeCode        int32      `json:"shipTypeCode"`
	DimensionBowM       uint32     `json:"dimensionBowM"`
	DimensionSternM     uint32     `json:"dimensionSternM"`
	DimensionPortM      uint32     `json:"dimensionPortM"`
	DimensionStarboardM uint32     `json:"dimensionStarboardM"`
	DraughtMillimetres  uint32     `json:"draughtMillimetres"`
	Destination         string     `json:"destination,omitempty"`
	ETA                 *time.Time `json:"eta,omitempty"`
	EpfsType            string     `json:"epfsType"`
	Classification      string     `json:"classification"`
	ObservedAt          time.Time  `json:"observedAt"`
}

// GeofenceRow is a persisted geofence event read model.
type GeofenceRow struct {
	GeofenceEventID string    `json:"geofenceEventId"`
	ZoneID          string    `json:"zoneId"`
	ZoneName        string    `json:"zoneName"`
	Event           string    `json:"event"`
	MMSI            string    `json:"mmsi,omitempty"`
	TrackReference  string    `json:"trackReference,omitempty"`
	LatitudeMicros  int32     `json:"latitudeMicros"`
	LongitudeMicros int32     `json:"longitudeMicros"`
	Classification  string    `json:"classification"`
	OccurredAt      time.Time `json:"occurredAt"`
}

// ZoneRow is the REST read model for a geofence zone.
type ZoneRow struct {
	ZoneID              string     `json:"zoneId"`
	TenantID            string     `json:"tenantId"`
	Name                string     `json:"name"`
	ClassificationFloor string     `json:"classificationFloor"`
	State               string     `json:"state"`
	MakerPrincipalID    string     `json:"makerPrincipalId"`
	CreatedAt           time.Time  `json:"createdAt"`
	ApprovedBy          *string    `json:"approvedBy,omitempty"`
	ApprovedAt          *time.Time `json:"approvedAt,omitempty"`
	GeoJSON             string     `json:"geoJson"`
}

// ListVessels returns latest positions inside the bbox (micro-degrees)
// whose classification is covered by the caller's cleared label set.
func (store *Store) ListVessels(ctx context.Context, minLonMicros, minLatMicros, maxLonMicros, maxLatMicros int32, clearedLabels []string, limit int) ([]VesselSummary, error) {
	rows, err := store.pool.Query(ctx, `SELECT l.mmsi, l.vessel_ref, l.source_class,
		l.latitude_micros, l.longitude_micros, l.speed_over_ground_milliknots,
		l.course_over_ground_millidegrees, l.classification, l.observed_at,
		COALESCE(s.ship_name, ''), s.ship_type_code
		FROM latest_positions l
		LEFT JOIN vessels_static s ON s.mmsi = l.mmsi AND s.valid_to IS NULL
		WHERE l.classification = ANY($1)
		  AND ST_Intersects(l.geom, ST_MakeEnvelope($2, $3, $4, $5, 4326)::geography)
		ORDER BY l.observed_at DESC LIMIT $6`,
		clearedLabels,
		float64(minLonMicros)/1e6, float64(minLatMicros)/1e6,
		float64(maxLonMicros)/1e6, float64(maxLatMicros)/1e6, limit)
	if err != nil {
		return nil, fmt.Errorf("list vessels: %w", err)
	}
	defer rows.Close()
	return scanVesselSummaries(rows)
}

func scanVesselSummaries(rows pgx.Rows) ([]VesselSummary, error) {
	vessels := make([]VesselSummary, 0)
	for rows.Next() {
		var vessel VesselSummary
		var mmsi, vesselRef *string
		if err := rows.Scan(&mmsi, &vesselRef, &vessel.SourceClass,
			&vessel.LatitudeMicros, &vessel.LongitudeMicros, &vessel.SpeedOverGroundMilliknots,
			&vessel.CourseOverGroundMillidegrees, &vessel.Classification, &vessel.ObservedAt,
			&vessel.ShipName, &vessel.ShipTypeCode); err != nil {
			return nil, fmt.Errorf("scan vessel summary: %w", err)
		}
		if mmsi != nil {
			vessel.MMSI = *mmsi
		}
		if vesselRef != nil {
			vessel.VesselRef = *vesselRef
		}
		vessels = append(vessels, vessel)
	}
	return vessels, rows.Err()
}

// GetVessel360 resolves the vessel-360 view: latest position, current
// static record and the most recent zone crossings, clearance-filtered.
func (store *Store) GetVessel360(ctx context.Context, mmsi string, clearedLabels []string) (*Vessel360, error) {
	view := &Vessel360{RecentZones: []GeofenceRow{}}
	rows, err := store.pool.Query(ctx, `SELECT l.mmsi, l.vessel_ref, l.source_class,
		l.latitude_micros, l.longitude_micros, l.speed_over_ground_milliknots,
		l.course_over_ground_millidegrees, l.classification, l.observed_at,
		COALESCE(s.ship_name, ''), s.ship_type_code
		FROM latest_positions l
		LEFT JOIN vessels_static s ON s.mmsi = l.mmsi AND s.valid_to IS NULL
		WHERE l.mmsi = $1 AND l.classification = ANY($2) LIMIT 1`, mmsi, clearedLabels)
	if err != nil {
		return nil, fmt.Errorf("vessel-360 latest: %w", err)
	}
	latest, err := scanVesselSummaries(rows)
	rows.Close()
	if err != nil {
		return nil, err
	}
	if len(latest) == 1 {
		view.Latest = &latest[0]
	}
	var static StaticRow
	err = store.pool.QueryRow(ctx, `SELECT mmsi, imo, callsign, ship_name, ship_type_code,
		dimension_bow_m, dimension_stern_m, dimension_port_m, dimension_starboard_m,
		draught_millimetres, destination, eta, epfs_type, classification, observed_at
		FROM vessels_static WHERE mmsi = $1 AND valid_to IS NULL AND classification = ANY($2)
		ORDER BY valid_from DESC LIMIT 1`, mmsi, clearedLabels).
		Scan(&static.MMSI, &static.IMO, &static.Callsign, &static.ShipName, &static.ShipTypeCode,
			&static.DimensionBowM, &static.DimensionSternM, &static.DimensionPortM, &static.DimensionStarboardM,
			&static.DraughtMillimetres, &static.Destination, &static.ETA, &static.EpfsType,
			&static.Classification, &static.ObservedAt)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("vessel-360 static: %w", err)
	}
	if err == nil {
		view.Static = &static
	}
	eventRows, err := store.pool.Query(ctx, `SELECT geofence_event_id, zone_id, zone_name, event,
		COALESCE(mmsi, ''), COALESCE(track_reference, ''), latitude_micros, longitude_micros,
		classification, occurred_at
		FROM geofence_events WHERE mmsi = $1 AND classification = ANY($2)
		ORDER BY occurred_at DESC LIMIT 20`, mmsi, clearedLabels)
	if err != nil {
		return nil, fmt.Errorf("vessel-360 zones: %w", err)
	}
	defer eventRows.Close()
	for eventRows.Next() {
		var event GeofenceRow
		if err := eventRows.Scan(&event.GeofenceEventID, &event.ZoneID, &event.ZoneName, &event.Event,
			&event.MMSI, &event.TrackReference, &event.LatitudeMicros, &event.LongitudeMicros,
			&event.Classification, &event.OccurredAt); err != nil {
			return nil, fmt.Errorf("vessel-360 zone scan: %w", err)
		}
		view.RecentZones = append(view.RecentZones, event)
	}
	if view.Latest == nil && view.Static == nil {
		return nil, nil
	}
	return view, eventRows.Err()
}

// GetTrack renders the vessel's track over [from, to] as a GeoJSON
// LineString via ST_MakeLine, plus the fixed-point coordinate list.
func (store *Store) GetTrack(ctx context.Context, mmsi string, from, to time.Time, clearedLabels []string) (geoJSON string, points int, err error) {
	if !from.Before(to) {
		return "", 0, errors.New("track from must be before to")
	}
	err = store.pool.QueryRow(ctx, `SELECT COALESCE(ST_AsGeoJSON(ST_MakeLine(geom::geometry ORDER BY observed_at)), ''),
		count(*)
		FROM ais_positions
		WHERE mmsi = $1 AND observed_at >= $2 AND observed_at <= $3 AND classification = ANY($4)`,
		mmsi, from.UTC(), to.UTC(), clearedLabels).Scan(&geoJSON, &points)
	if err != nil {
		return "", 0, fmt.Errorf("track query: %w", err)
	}
	return geoJSON, points, nil
}

// ListZones returns the tenant's zones visible at the caller's clearance.
func (store *Store) ListZones(ctx context.Context, tenantID string, clearedLabels []string) ([]ZoneRow, error) {
	zones := make([]ZoneRow, 0)
	err := store.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT zone_id, tenant_id, name, classification_floor, state,
			maker_principal_id, created_at, approved_by, approved_at, ST_AsGeoJSON(geom::geometry)
			FROM geofence_zones ORDER BY created_at DESC`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var zone ZoneRow
			if err := rows.Scan(&zone.ZoneID, &zone.TenantID, &zone.Name, &zone.ClassificationFloor,
				&zone.State, &zone.MakerPrincipalID, &zone.CreatedAt, &zone.ApprovedBy, &zone.ApprovedAt, &zone.GeoJSON); err != nil {
				return err
			}
			if labelCleared(zone.ClassificationFloor, clearedLabels) {
				zones = append(zones, zone)
			}
		}
		return rows.Err()
	})
	if err != nil {
		return nil, fmt.Errorf("list zones: %w", err)
	}
	return zones, nil
}

// labelCleared reports whether a row label is inside the cleared set.
func labelCleared(label string, clearedLabels []string) bool {
	for _, cleared := range clearedLabels {
		if cleared == label {
			return true
		}
	}
	return false
}

// CreateZone is the maker half of zone administration: a zone is persisted
// as a draft and becomes effective only after a four-eyes approval.
func (store *Store) CreateZone(ctx context.Context, tenantID string, zone ZoneRow, polygonGeoJSON string) error {
	if strings.TrimSpace(zone.ZoneID) == "" || strings.TrimSpace(zone.Name) == "" {
		return errors.New("zone id and name are required")
	}
	if strings.TrimSpace(zone.MakerPrincipalID) == "" {
		return errors.New("maker principal is required")
	}
	switch zone.ClassificationFloor {
	case "PUBLIC", "INTERNAL", "RESTRICTED", "CONFIDENTIAL", "SECRET":
	default:
		return errors.New("zone classification floor is invalid")
	}
	return store.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		// PostGIS 3.3 has no ST_GeogFromGeoJSON — parse as geometry and cast.
		if _, err := tx.Exec(ctx, `INSERT INTO geofence_zones
			(zone_id, tenant_id, name, geom, classification_floor, state, maker_principal_id)
			VALUES ($1, $2, $3, ST_GeomFromGeoJSON($4)::geography, $5, 'draft', $6)`,
			zone.ZoneID, tenantID, zone.Name, polygonGeoJSON, zone.ClassificationFloor, zone.MakerPrincipalID); err != nil {
			return fmt.Errorf("create zone %s: %w", zone.ZoneID, err)
		}
		return nil
	})
}

// ErrMakerCheckerConflict is returned when the maker attempts to approve
// their own zone.
var ErrMakerCheckerConflict = errors.New("maker may not approve own zone (four-eyes)")

// ErrZoneNotDraft is returned when approving a zone that is not in draft.
var ErrZoneNotDraft = errors.New("zone is not in draft state")

// ApproveZone is the checker half: a principal distinct from the maker
// approves the draft, recording the approval ledger entry.
func (store *Store) ApproveZone(ctx context.Context, tenantID, zoneID, checkerPrincipalID string) error {
	if strings.TrimSpace(checkerPrincipalID) == "" {
		return errors.New("checker principal is required")
	}
	return store.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		var maker, state string
		err := tx.QueryRow(ctx, `SELECT maker_principal_id, state FROM geofence_zones
			WHERE zone_id = $1 FOR UPDATE`, zoneID).Scan(&maker, &state)
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("zone %s not found", zoneID)
		}
		if err != nil {
			return err
		}
		if maker == checkerPrincipalID {
			return ErrMakerCheckerConflict
		}
		if state != "draft" {
			return ErrZoneNotDraft
		}
		if _, err := tx.Exec(ctx, `INSERT INTO geofence_zone_approvals (zone_id, principal_id)
			VALUES ($1, $2)`, zoneID, checkerPrincipalID); err != nil {
			return fmt.Errorf("record zone approval: %w", err)
		}
		if _, err := tx.Exec(ctx, `UPDATE geofence_zones
			SET state = 'approved', approved_by = $2, approved_at = now()
			WHERE zone_id = $1`, zoneID, checkerPrincipalID); err != nil {
			return fmt.Errorf("approve zone %s: %w", zoneID, err)
		}
		return nil
	})
}

// VesselsInZone lists the tenant-visible vessels currently inside an
// approved zone.
func (store *Store) VesselsInZone(ctx context.Context, tenantID, zoneID string, clearedLabels []string, limit int) ([]VesselSummary, error) {
	vessels := make([]VesselSummary, 0)
	err := store.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		var wkt string
		err := tx.QueryRow(ctx, `SELECT ST_AsText(geom::geometry) FROM geofence_zones
			WHERE zone_id = $1 AND state = 'approved'`, zoneID).Scan(&wkt)
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("zone %s not found or not approved", zoneID)
		}
		if err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `SELECT l.mmsi, l.vessel_ref, l.source_class,
			l.latitude_micros, l.longitude_micros, l.speed_over_ground_milliknots,
			l.course_over_ground_millidegrees, l.classification, l.observed_at,
			COALESCE(s.ship_name, ''), s.ship_type_code
			FROM latest_positions l
			LEFT JOIN vessels_static s ON s.mmsi = l.mmsi AND s.valid_to IS NULL
			WHERE l.classification = ANY($1)
			  AND ST_Intersects(l.geom, (SELECT geom FROM geofence_zones WHERE zone_id = $2))
			ORDER BY l.observed_at DESC LIMIT $3`, clearedLabels, zoneID, limit)
		if err != nil {
			return err
		}
		defer rows.Close()
		scanned, err := scanVesselSummaries(rows)
		if err != nil {
			return err
		}
		vessels = scanned
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("vessels in zone: %w", err)
	}
	return vessels, nil
}
