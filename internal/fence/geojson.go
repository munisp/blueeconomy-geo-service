package fence

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
)

// GeoJSON protection-zone loading (Phase 19). Zones are seeded ONLY from a
// real, operator-provided GeoJSON file path supplied via configuration
// (GEO_ZONE_SEED_GEOJSON); nothing is bundled or fabricated. The loader is
// deliberately pure file-in/structures-out so it is unit-testable, and it
// fails closed: a malformed file, a non-polygon feature, an out-of-range
// coordinate or an unknown zone category aborts the whole load — a
// partially seeded protection-zone set is never silently accepted.

// SeedZone is one polygon zone parsed from a GeoJSON feature, ready to be
// written as version 1 of a geofence.
type SeedZone struct {
	GeofenceID     string
	Name           string
	Category       ZoneCategory
	Classification string
	// VerticesMicros is the closed ring [[latMicros, lonMicros], ...] in
	// fixed-point micro-degrees — the same representation the
	// zone-management endpoints accept.
	VerticesMicros [][2]int32
}

type geoJSONDocument struct {
	Type     string           `json:"type"`
	Features []geoJSONFeature `json:"features"`
}

type geoJSONFeature struct {
	Type       string `json:"type"`
	Properties struct {
		ID             string `json:"id"`
		GeofenceID     string `json:"geofenceId"`
		Name           string `json:"name"`
		Category       string `json:"zoneCategory"`
		Classification string `json:"classification"`
	} `json:"properties"`
	Geometry struct {
		Type        string          `json:"type"`
		Coordinates json.RawMessage `json:"coordinates"`
	} `json:"geometry"`
}

// LoadSeedZones parses a GeoJSON FeatureCollection of Polygon features into
// seed zones. Every feature must carry an id (or geofenceId) and a name in
// its properties; zoneCategory defaults to "general" and classification to
// "INTERNAL". The outer ring of each polygon is converted to a closed
// micro-degree ring (holes are rejected — protection zones are simple
// polygons by doctrine).
func LoadSeedZones(path string) ([]SeedZone, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read zone seed geojson: %w", err)
	}
	var doc geoJSONDocument
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("parse zone seed geojson: %w", err)
	}
	if doc.Type != "FeatureCollection" {
		return nil, fmt.Errorf("zone seed geojson must be a FeatureCollection, got %q", doc.Type)
	}
	if len(doc.Features) == 0 {
		return nil, fmt.Errorf("zone seed geojson contains no features; refusing to seed an empty set implicitly")
	}
	zones := make([]SeedZone, 0, len(doc.Features))
	seen := map[string]bool{}
	for index, feature := range doc.Features {
		if feature.Type != "Feature" || feature.Geometry.Type != "Polygon" {
			return nil, fmt.Errorf("feature %d: only Polygon features are admitted (got %s/%s)", index, feature.Type, feature.Geometry.Type)
		}
		var rings [][][2]float64
		if err := json.Unmarshal(feature.Geometry.Coordinates, &rings); err != nil {
			return nil, fmt.Errorf("feature %d: polygon coordinates unreadable: %w", index, err)
		}
		if len(rings) != 1 {
			return nil, fmt.Errorf("feature %d: polygons with holes are not admitted (%d rings)", index, len(rings))
		}
		ring, err := microRing(rings[0])
		if err != nil {
			return nil, fmt.Errorf("feature %d: %w", index, err)
		}
		if err := ValidateGeometry(pointsOf(ring)); err != nil {
			return nil, fmt.Errorf("feature %d: %w", index, err)
		}
		id := feature.Properties.GeofenceID
		if id == "" {
			id = feature.Properties.ID
		}
		if id == "" {
			return nil, fmt.Errorf("feature %d: properties.id (or geofenceId) is required", index)
		}
		if seen[id] {
			return nil, fmt.Errorf("feature %d: duplicate zone id %q", index, id)
		}
		seen[id] = true
		if feature.Properties.Name == "" {
			return nil, fmt.Errorf("feature %d: properties.name is required", index)
		}
		category, ok := NormalizeZoneCategory(feature.Properties.Category)
		if !ok {
			return nil, fmt.Errorf("feature %d: zoneCategory %q is not admitted", index, feature.Properties.Category)
		}
		classification := feature.Properties.Classification
		if classification == "" {
			classification = "INTERNAL"
		}
		zones = append(zones, SeedZone{
			GeofenceID: id, Name: feature.Properties.Name, Category: category,
			Classification: classification, VerticesMicros: ring,
		})
	}
	return zones, nil
}

// microRing converts a GeoJSON [lon, lat] float ring into a closed
// micro-degree [lat, lon] integer ring, rounding to the nearest micro-degree
// and closing the ring when the source left it open.
func microRing(coords [][2]float64) ([][2]int32, error) {
	if len(coords) < 3 {
		return nil, fmt.Errorf("ring must have at least 3 positions, got %d", len(coords))
	}
	ring := make([][2]int32, 0, len(coords)+1)
	for i, c := range coords {
		lon, lat := c[0], c[1]
		if lat < -90 || lat > 90 || lon < -180 || lon > 180 {
			return nil, fmt.Errorf("position %d out of range (lon=%v lat=%v)", i, lon, lat)
		}
		ring = append(ring, [2]int32{int32(math.Round(lat * 1e6)), int32(math.Round(lon * 1e6))})
	}
	if ring[0] != ring[len(ring)-1] {
		ring = append(ring, ring[0])
	}
	return ring, nil
}

func pointsOf(ring [][2]int32) []Point {
	points := make([]Point, len(ring))
	for i, v := range ring {
		points[i] = Point{LatMicros: v[0], LonMicros: v[1]}
	}
	return points
}
