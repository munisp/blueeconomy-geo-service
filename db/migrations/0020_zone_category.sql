-- 0020_zone_category: protection-zone semantics on the WP-10 versioned
-- geofence engine (Phase 19 MPA / EEZ / IUU gap). A zone_category classifies
-- every fence version ('general' keeps the pre-0020 behaviour); transition
-- events snapshot the category of the fence version that produced them so
-- MPA/EEZ incursion alerting can filter persisted events without joining
-- fence history. No zones are seeded — the empty state is the honest state;
-- operators load real boundaries through the zone-management endpoints or
-- the GEO_ZONE_SEED_GEOJSON config-gated boot loader (never bundled
-- fixtures).
--
-- (Numbered 0020 because Phase 19 F2 took 0019 for the queue-observations
-- grant; filename-ordered migrations only.)

ALTER TABLE geofences
    ADD COLUMN zone_category TEXT NOT NULL DEFAULT 'general'
        CHECK (zone_category IN ('general', 'mpa', 'eez_restricted', 'traffic_separation', 'fishing_closure', 'anchorage'));

ALTER TABLE geofence_transition_events
    ADD COLUMN zone_category TEXT NOT NULL DEFAULT 'general'
        CHECK (zone_category IN ('general', 'mpa', 'eez_restricted', 'traffic_separation', 'fishing_closure', 'anchorage'));

-- Alerting/read path: incursion review by category over time.
CREATE INDEX geofence_transition_events_category_time
    ON geofence_transition_events (tenant_id, zone_category, occurred_at DESC);
