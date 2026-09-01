-- Phase 11 performance audit: missing index justified by actual query code.
-- Idempotent; does not duplicate any index declared in migrations 0001-0013.

-- devices.Store.ListReleasesForKind (firmware rollout bucketing):
--   WHERE tenant_id = $1 AND kind = $2 ORDER BY created_at DESC, id DESC
-- firmware_releases (0010_devices.sql) ships no secondary index.
CREATE INDEX IF NOT EXISTS firmware_releases_tenant_kind_idx
    ON firmware_releases (tenant_id, kind, created_at DESC, id DESC);
