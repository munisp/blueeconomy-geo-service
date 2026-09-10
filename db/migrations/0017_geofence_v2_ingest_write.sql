-- 0017_geofence_v2_ingest_write: let the ingest-time WP-10 fence evaluator
-- (G11, GEO_FENCE_V2_INGEST=true) persist geofence_transition_events through
-- the dedicated geo_ingest role connection, mirroring the platform-wide
-- geofences read granted in 0014. The evaluator attributes each event to the
-- fence's owning tenant (read from the platform-wide fence row); tenants
-- still read back only their own events through geofence_transition_events_tenant_policy.

GRANT INSERT ON geofence_transition_events TO geo_ingest;

-- Permissive FOR INSERT policy for the ingest role only; the existing tenant
-- policy's WITH CHECK remains in force for the application role (permissive
-- policies OR, so the app role is unchanged and geo_ingest gains insert).
CREATE POLICY geofence_transition_events_ingest_insert ON geofence_transition_events
    FOR INSERT TO geo_ingest
    WITH CHECK (true);
