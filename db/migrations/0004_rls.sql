-- 0004_rls: row-level security on the tenant-scoped read paths. FORCE ROW
-- LEVEL SECURITY applies the policies to the table owner as well; the
-- application role `geo` is NOBYPASSRLS (see 0001_core.sql). The tenant is
-- bound per transaction with:
--     SET LOCAL app.tenant_id = '<tenant>';
-- Classification-ladder enforcement (reader clearance >= row classification)
-- is applied by the API layer per request, identical to the
-- maritime-intelligence clearance doctrine; RLS governs tenant isolation.

-- The tenant predicate admits a row when the transaction's tenant matches,
-- or when no tenant is bound: the ingest-side geofence evaluator runs in the
-- platform-wide context (app.tenant_id unset) so a single position write can
-- be evaluated against every tenant's approved zones. The REST API always
-- binds app.tenant_id per request transaction, so API readers only ever see
-- their own tenant's rows.
ALTER TABLE geofence_zones ENABLE ROW LEVEL SECURITY;
ALTER TABLE geofence_zones FORCE ROW LEVEL SECURITY;
CREATE POLICY geofence_zones_tenant_policy ON geofence_zones
    USING (current_setting('app.tenant_id', true) IS NULL
           OR current_setting('app.tenant_id', true) = ''
           OR tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

ALTER TABLE geofence_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE geofence_events FORCE ROW LEVEL SECURITY;
CREATE POLICY geofence_events_tenant_policy ON geofence_events
    USING (current_setting('app.tenant_id', true) IS NULL
           OR current_setting('app.tenant_id', true) = ''
           OR tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
