package fence

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestZoneAlertRule(t *testing.T) {
	// Protected categories alert on ENTRY and EXIT.
	for _, category := range []ZoneCategory{CategoryMPA, CategoryEEZRestricted, CategoryTrafficSeparation, CategoryFishingClosure} {
		require.Equal(t, "PROTECTED_ZONE_ENTRY", ZoneAlert(category, EventEnter))
		require.Equal(t, "PROTECTED_ZONE_EXIT", ZoneAlert(category, EventExit))
		require.Empty(t, ZoneAlert(category, EventDwell), "dwell never alerts")
	}
	// Informational categories never alert.
	for _, category := range []ZoneCategory{CategoryGeneral, CategoryAnchorage} {
		require.Empty(t, ZoneAlert(category, EventEnter))
		require.Empty(t, ZoneAlert(category, EventExit))
	}
}

func TestNormalizeZoneCategory(t *testing.T) {
	category, ok := NormalizeZoneCategory("")
	require.True(t, ok)
	require.Equal(t, CategoryGeneral, category)
	category, ok = NormalizeZoneCategory("mpa")
	require.True(t, ok)
	require.Equal(t, CategoryMPA, category)
	_, ok = NormalizeZoneCategory("sandbox")
	require.False(t, ok, "unknown categories fail closed")
}

const testSeedGeoJSON = `{
  "type": "FeatureCollection",
  "features": [{
    "type": "Feature",
    "properties": {"id": "mpa.lagos.bay", "name": "Lagos Bay MPA", "zoneCategory": "mpa"},
    "geometry": {"type": "Polygon", "coordinates": [[
      [3.900000, -4.000000], [4.000000, -4.000000],
      [4.000000, -5.000000], [3.900000, -5.000000], [3.900000, -4.000000]
    ]]}
  }]
}`

func writeSeed(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "zones.geojson")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	return path
}

func TestLoadSeedZones(t *testing.T) {
	zones, err := LoadSeedZones(writeSeed(t, testSeedGeoJSON))
	require.NoError(t, err)
	require.Len(t, zones, 1)
	zone := zones[0]
	require.Equal(t, "mpa.lagos.bay", zone.GeofenceID)
	require.Equal(t, "Lagos Bay MPA", zone.Name)
	require.Equal(t, CategoryMPA, zone.Category)
	require.Equal(t, "INTERNAL", zone.Classification)
	require.Len(t, zone.VerticesMicros, 5)
	// GeoJSON is [lon,lat]; the ring is [latMicros, lonMicros].
	require.Equal(t, [2]int32{-4_000_000, 3_900_000}, zone.VerticesMicros[0])
	require.Equal(t, zone.VerticesMicros[0], zone.VerticesMicros[4], "ring closed")
}

func TestLoadSeedZonesDefaultsCategory(t *testing.T) {
	content := `{"type":"FeatureCollection","features":[{"type":"Feature",
	  "properties":{"id":"z1","name":"Z1"},
	  "geometry":{"type":"Polygon","coordinates":[[[3.9,-4.0],[4.0,-4.0],[4.0,-5.0],[3.9,-4.0]]]}}]}`
	zones, err := LoadSeedZones(writeSeed(t, content))
	require.NoError(t, err)
	require.Equal(t, CategoryGeneral, zones[0].Category)
	// Open ring is closed by the loader.
	require.Equal(t, zones[0].VerticesMicros[0], zones[0].VerticesMicros[len(zones[0].VerticesMicros)-1])
}

func TestLoadSeedZonesFailsClosed(t *testing.T) {
	cases := map[string]string{
		"missing file":        "",
		"not a collection":    `{"type":"Feature","features":[]}`,
		"empty collection":    `{"type":"FeatureCollection","features":[]}`,
		"non polygon":         `{"type":"FeatureCollection","features":[{"type":"Feature","properties":{"id":"z","name":"Z"},"geometry":{"type":"Point","coordinates":[3.9,-4.0]}}]}`,
		"unknown category":    `{"type":"FeatureCollection","features":[{"type":"Feature","properties":{"id":"z","name":"Z","zoneCategory":"sandbox"},"geometry":{"type":"Polygon","coordinates":[[[3.9,-4.0],[4.0,-4.0],[4.0,-5.0],[3.9,-4.0]]]}}]}`,
		"missing name":        `{"type":"FeatureCollection","features":[{"type":"Feature","properties":{"id":"z"},"geometry":{"type":"Polygon","coordinates":[[[3.9,-4.0],[4.0,-4.0],[4.0,-5.0],[3.9,-4.0]]]}}]}`,
		"out of range coords": `{"type":"FeatureCollection","features":[{"type":"Feature","properties":{"id":"z","name":"Z"},"geometry":{"type":"Polygon","coordinates":[[[3.9,-94.0],[4.0,-4.0],[4.0,-5.0],[3.9,-94.0]]]}}]}`,
		"polygon with holes":  `{"type":"FeatureCollection","features":[{"type":"Feature","properties":{"id":"z","name":"Z"},"geometry":{"type":"Polygon","coordinates":[[[3.9,-4.0],[4.0,-4.0],[4.0,-5.0],[3.9,-4.0]],[[3.91,-4.1],[3.92,-4.1],[3.92,-4.2],[3.91,-4.1]]]}}]}`,
	}
	for name, content := range cases {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "does-not-exist.geojson")
			if content != "" {
				path = writeSeed(t, content)
			}
			_, err := LoadSeedZones(path)
			require.Error(t, err)
		})
	}
}
