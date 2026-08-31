-- 0008_rls_ingest_login: repair the privilege-escalation flaw in the 0007
-- design. 0007 granted `geo_ingest TO geo` so the application role could
-- SET LOCAL ROLE into the platform-wide ingest role for geofence
-- evaluation. That pattern is self-defeating: PostgreSQL permissive
-- policies OR across EVERY role the session user is a member of, so the
-- permissive geofence_zones_ingest_read USING(true) policy applied to
-- every `geo` session at all times — silently re-opening the cross-tenant
-- read the default-deny rework was written to close (proven live by
-- TestTenantRLSDefaultDeny / TestTenantRLS returning cross-tenant rows).
--
-- New model — privilege separation by CONNECTION, not by SET ROLE:
--   * `geo` (application role): loses geo_ingest membership. It is subject
--     only to the tenant policies; unbound sessions remain default-deny.
--   * `geo_ingest`: becomes a LOGIN role with its own DSN
--     (GEO_INGEST_PG_DSN, provisioned from an external secret). The
--     geofence evaluator connects AS this role on a dedicated pool. The
--     role holds exactly SELECT ON geofence_zones and INSERT ON
--     geofence_events (granted in 0007) — it cannot read positions,
--     vessels, SOS alerts or any tenant-governed table.
-- The password is set out-of-band by the deployment (secret rotation
-- friendly); this migration only flips LOGIN and revokes the membership.

-- Revoke the membership from EVERY grantee (deployment `geo` role, test
-- roles, operators) — any surviving grantee silently regains the
-- platform-wide permissive read through policy OR-semantics.
DO $$
DECLARE
    grantee RECORD;
BEGIN
    FOR grantee IN
        SELECT member_role.rolname AS rolname
        FROM pg_auth_members membership
        JOIN pg_roles member_role ON member_role.oid = membership.member
        WHERE membership.roleid = (SELECT oid FROM pg_roles WHERE rolname = 'geo_ingest')
    LOOP
        EXECUTE format('REVOKE geo_ingest FROM %I', grantee.rolname);
    END LOOP;
END $$;

ALTER ROLE geo_ingest LOGIN;
