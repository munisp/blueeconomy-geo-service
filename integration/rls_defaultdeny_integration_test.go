// Phase-6 remediation regression tests. Same infrastructure gates as
// pipeline_integration_test.go.
package integration

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/munisp/blueeconomy-geo-service/internal/store"
)

// TestTenantRLSDefaultDeny proves the 0007 posture: without a bound tenant
// the app role reads zero rows and cannot write; with a bound tenant only
// its own rows are visible; the ingest evaluator (geo_ingest role) still
// works platform-wide (covered end-to-end by TestGeofenceEnterExit).
func TestTenantRLSDefaultDeny(t *testing.T) {
	h := newHarness(t)
	h.clean(t)
	ctx := context.Background()

	require.NoError(t, h.store.CreateZone(ctx, testTenant, store.ZoneRow{
		ZoneID: testZoneID, Name: "ITEST Deny Zone",
		ClassificationFloor: "PUBLIC", MakerPrincipalID: "itest-maker",
	}, zonePolygon))

	// Unbound session: default-deny reads.
	var zoneCount, eventCount int
	require.NoError(t, h.store.Pool().QueryRow(ctx, `SELECT count(*) FROM geofence_zones`).Scan(&zoneCount))
	require.Zero(t, zoneCount, "unbound tenant must read zero zones")
	require.NoError(t, h.store.Pool().QueryRow(ctx, `SELECT count(*) FROM geofence_events`).Scan(&eventCount))
	require.Zero(t, eventCount, "unbound tenant must read zero events")

	// Unbound session: default-deny writes (WITH CHECK rejects the row).
	_, err := h.store.Pool().Exec(ctx, `INSERT INTO geofence_zones
		(zone_id, tenant_id, name, geom, classification_floor, state, maker_principal_id)
		VALUES ('itest-zone-unbound', 'itest-tenant', 'unbound', ST_GeomFromGeoJSON($1)::geography, 'PUBLIC', 'draft', 'itest-maker')`,
		zonePolygon)
	require.Error(t, err, "unbound tenant must not insert zones")

	// Bound sessions see exactly their own rows.
	zones, err := h.store.ListZones(ctx, testTenant, []string{"PUBLIC"})
	require.NoError(t, err)
	require.Len(t, zones, 1)
	zones, err = h.store.ListZones(ctx, testTenant2, []string{"PUBLIC"})
	require.NoError(t, err)
	require.Empty(t, zones)
}
