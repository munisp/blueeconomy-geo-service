-- 0005_rls_ingest_write_policy: the 0004 WITH CHECK clauses required the
-- tenant binding on writes, which broke the ingest-side geofence evaluator
-- (it runs in the platform-wide context with app.tenant_id unset when
-- persisting crossings). Align WITH CHECK with the USING predicate: writes
-- are admitted for the bound tenant, or in the platform ingest context.
DROP POLICY geofence_zones_tenant_policy ON geofence_zones;
CREATE POLICY geofence_zones_tenant_policy ON geofence_zones
    USING (current_setting('app.tenant_id', true) IS NULL
           OR current_setting('app.tenant_id', true) = ''
           OR tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (current_setting('app.tenant_id', true) IS NULL
                OR current_setting('app.tenant_id', true) = ''
                OR tenant_id = current_setting('app.tenant_id', true));

DROP POLICY geofence_events_tenant_policy ON geofence_events;
CREATE POLICY geofence_events_tenant_policy ON geofence_events
    USING (current_setting('app.tenant_id', true) IS NULL
           OR current_setting('app.tenant_id', true) = ''
           OR tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (current_setting('app.tenant_id', true) IS NULL
                OR current_setting('app.tenant_id', true) = ''
                OR tenant_id = current_setting('app.tenant_id', true));
