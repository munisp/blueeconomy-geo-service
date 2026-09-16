package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/munisp/blueeconomy-geo-service/internal/fence"
	"github.com/munisp/blueeconomy-geo-service/internal/store"
)

// seedZonesFromGeoJSON implements the GEO_ZONE_SEED_GEOJSON boot loader
// (Phase 19). When the env var is set it points at a real, operator-provided
// GeoJSON FeatureCollection of protection-zone polygons; each feature becomes
// version 1 of a geofence under GEO_ZONE_SEED_TENANT. The loader is
// idempotent (a zone id that already exists is skipped, never versioned) and
// fail-closed (a malformed file, a missing tenant, or a store failure aborts
// startup — a partially seeded protection-zone set is never silently
// accepted). Unset means no seeding: the empty state is the honest state.
func seedZonesFromGeoJSON(ctx context.Context, storage *store.Store, logger *log.Logger) error {
	path := strings.TrimSpace(os.Getenv("GEO_ZONE_SEED_GEOJSON"))
	if path == "" {
		return nil
	}
	tenantID := strings.TrimSpace(os.Getenv("GEO_ZONE_SEED_TENANT"))
	if tenantID == "" {
		return errors.New("GEO_ZONE_SEED_TENANT is required when GEO_ZONE_SEED_GEOJSON is set")
	}
	zones, err := fence.LoadSeedZones(path)
	if err != nil {
		return fmt.Errorf("GEO_ZONE_SEED_GEOJSON: %w", err)
	}
	seeded, skipped := 0, 0
	for _, zone := range zones {
		if _, err := storage.GetGeofenceHistory(ctx, tenantID, zone.GeofenceID); err == nil {
			skipped++
			continue
		} else if !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("seed zone %s: read existing: %w", zone.GeofenceID, err)
		}
		vertices, err := json.Marshal(zone.VerticesMicros)
		if err != nil {
			return fmt.Errorf("seed zone %s: encode ring: %w", zone.GeofenceID, err)
		}
		if _, err := storage.CreateGeofenceVersion(ctx, store.FenceRow{
			GeofenceID: zone.GeofenceID, TenantID: tenantID, Name: zone.Name,
			Classification: zone.Classification, VerticesMicros: vertices,
			ZoneCategory: string(zone.Category), CreatedBy: "zone-seed",
		}, 0); err != nil {
			return fmt.Errorf("seed zone %s: %w", zone.GeofenceID, err)
		}
		seeded++
	}
	logger.Printf("protection-zone seeding: %d seeded, %d already present (GEO_ZONE_SEED_GEOJSON=%s tenant=%s)",
		seeded, skipped, path, tenantID)
	return nil
}
