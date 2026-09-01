-- 0014_geofence_v2_rls: close the RLS gap on the WP-10 geofence tables from
-- 0013_geofence_v2. `geofences` is tenant-scoped (tenant_id NOT NULL) but was
-- created without row-level security, so any role with table privileges could
-- read or write every tenant's fences. `geofence_zone_approvals` (0001) is
-- tenant-scoped transitively through geofence_zones and likewise had no
-- policy.
--
-- Doctrine matches 0007_rls_default_deny: policies compare tenant_id against
-- the bound app.tenant_id GUC only; an unbound session is DENIED (default
-- deny). The ingest geofence evaluator reads fences platform-wide through the
-- explicit geo_ingest role (SET LOCAL ROLE), mirroring geofence_zones.

ALTER TABLE geofences ENABLE ROW LEVEL SECURITY;
ALTER TABLE geofences FORCE ROW LEVEL SECURITY;
CREATE POLICY geofences_tenant_policy ON geofences
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

-- Platform-wide read for the ingest geofence evaluator only (fence matching
-- runs platform-wide; recorded events stay per-tenant).
CREATE POLICY geofences_ingest_read ON geofences
    FOR SELECT TO geo_ingest
    USING (true);
GRANT SELECT ON geofences TO geo_ingest;

-- Approvals inherit the zone's tenant. The subquery is evaluated under the
-- geofence_zones tenant policy, so an unbound or foreign-tenant session sees
-- no zones and therefore no approvals (fail-closed).
ALTER TABLE geofence_zone_approvals ENABLE ROW LEVEL SECURITY;
ALTER TABLE geofence_zone_approvals FORCE ROW LEVEL SECURITY;
CREATE POLICY geofence_zone_approvals_tenant_policy ON geofence_zone_approvals
    USING (EXISTS (SELECT 1 FROM geofence_zones z
                   WHERE z.zone_id = geofence_zone_approvals.zone_id
                     AND z.tenant_id = current_setting('app.tenant_id', true)))
    WITH CHECK (EXISTS (SELECT 1 FROM geofence_zones z
                   WHERE z.zone_id = geofence_zone_approvals.zone_id
                     AND z.tenant_id = current_setting('app.tenant_id', true)));
