-- 0007_rls_default_deny: close the fail-open tenant predicate from
-- 0004/0005. The previous USING/WITH CHECK clauses admitted every row when
-- app.tenant_id was NULL or '' — any session that simply forgot to bind the
-- tenant read (and wrote) across all tenants.
--
-- New model:
--   * Application role `geo` (REST API + hot-path writers): tenant policies
--     compare tenant_id against the bound GUC only. When app.tenant_id is
--     unset the comparison is NULL and the row is DENIED — default deny.
--   * Ingest role `geo_ingest` (NOLOGIN, NOBYPASSRLS): the geofence
--     evaluator legitimately operates platform-wide (one position write is
--     evaluated against every tenant's approved zones, and crossings are
--     recorded per zone tenant). That privilege is now an explicit,
--     documented role instead of a blanket NULL-admit clause. The ingest
--     code path acquires it per transaction with SET LOCAL ROLE geo_ingest;
--     membership is granted to `geo` (deployment) and to the migration
--     runner where required (integration harness).
-- FORCE ROW LEVEL SECURITY stays on; there is no PUBLIC policy that admits
-- unbound sessions.

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'geo_ingest') THEN
        CREATE ROLE geo_ingest NOSUPERUSER NOBYPASSRLS NOLOGIN;
    END IF;
END $$;

GRANT SELECT ON geofence_zones TO geo_ingest;
GRANT INSERT ON geofence_events TO geo_ingest;
-- The application role runs the ingest geofence evaluation inside a
-- SET LOCAL ROLE transaction; membership does not weaken the tenant
-- policies (they are default-deny for `geo` itself).
GRANT geo_ingest TO geo;

DROP POLICY geofence_zones_tenant_policy ON geofence_zones;
CREATE POLICY geofence_zones_tenant_policy ON geofence_zones
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

-- Platform-wide read for the ingest geofence evaluator only.
CREATE POLICY geofence_zones_ingest_read ON geofence_zones
    FOR SELECT TO geo_ingest
    USING (true);

DROP POLICY geofence_events_tenant_policy ON geofence_events;
CREATE POLICY geofence_events_tenant_policy ON geofence_events
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

-- The ingest evaluator records crossings against the zone's tenant.
CREATE POLICY geofence_events_ingest_insert ON geofence_events
    FOR INSERT TO geo_ingest
    WITH CHECK (true);
