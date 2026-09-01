# Security Posture — blueeconomy-geo-service

Phase 11 security audit (branch `phase11/security`).

## Controls verified
- **Secrets**: working-tree scan clean.
- **AuthN/Z**: every admin route wraps the platform authenticator + `RequireRoles`; device-plane endpoints authenticate by Ed25519 signed proof / one-time activation secret (verified: VerifyTelemetry/VerifyProof on telemetry, firmware, mqtt-auth, activate). Maker/checker four-eyes on zones, devices, firmware.
- **Injection**: parameterized pgx queries; no string-built SQL.
- **RLS**: default-deny tenant policies (0007) with explicit `geo_ingest` platform role.

## Fixes this phase
- **HIGH (RLS gap)**: new migration `0014_geofence_v2_rls.sql` — enables and forces RLS on the WP-10 `geofences` table (tenant-scoped, previously unprotected) with a default-deny tenant policy plus a `geo_ingest`-only platform read, and adds an RLS policy on `geofence_zone_approvals` inherited through `geofence_zones`.

## Residuals
- `0014` reviewed against the 0007 doctrine but not applied to a live database in this offline environment; apply in staging before promotion.
- MRV tables are flag-administration (not tenant-scoped) by design; documented in 0012.
