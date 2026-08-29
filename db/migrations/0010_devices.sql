-- 0009_devices: device-management plane foundation (Citizen Services
-- Advisory §9; IoT evidence gaps: device registry, provisioning/PKI,
-- lifecycle, OTA manifest, broker auth).
--
-- Privilege model mirrors the 0007/0008 doctrine:
--   * Application role `geo` (REST API): tenant policies compare tenant_id
--     against the bound app.tenant_id GUC only — an unbound session is
--     default-deny. FORCE ROW LEVEL SECURITY is on for every table.
--   * Device-verification role `geo_devices` (LOGIN, NOBYPASSRLS, password
--     provisioned out-of-band, GEO_DEVICES_PG_DSN): the verify-at-ingest
--     path authenticates DEVICE-signed envelopes whose principal is proven
--     by an Ed25519 signature, not by an HTTP tenant binding, so it runs on
--     a dedicated CONNECTION as this least-privilege role. It holds exactly
--     SELECT on devices / device_keys / firmware_releases / provisioning_
--     requests, UPDATE (status) on provisioning_requests (activation
--     consume-on-use) and INSERT on device_audit_events. It can never write
--     the registry, rotate keys or publish firmware.
--   * geo_devices is NEVER granted to `geo` (permissive policies OR across
--     role memberships — see 0008_rls_ingest_login.sql); privilege
--     separation is by connection, never by SET ROLE.

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'geo_devices') THEN
        CREATE ROLE geo_devices NOSUPERUSER NOBYPASSRLS LOGIN;
    END IF;
END $$;

-- Device registry. key_epoch mirrors the current signing epoch; device_keys
-- is the ledger of every epoch.
CREATE TABLE devices (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('AIS','GT06','LORAWAN','POS','VALIDATOR','GATEWAY','SENSOR')),
    owner_agency TEXT NOT NULL,
    label TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'ACTIVE'
        CHECK (status IN ('ACTIVE','SUSPENDED','REVOKED','DECOMMISSIONED')),
    key_epoch INTEGER NOT NULL DEFAULT 1 CHECK (key_epoch >= 1),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_by TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Per-epoch Ed25519 public keys. PREVIOUS stays valid for a configurable
-- grace window after rotation so offline devices keep authenticating until
-- they fetch the new epoch; REVOKED is terminal and immediate.
CREATE TABLE device_keys (
    device_id UUID NOT NULL REFERENCES devices(id) ON DELETE CASCADE,
    epoch INTEGER NOT NULL CHECK (epoch >= 1),
    ed25519_public_key BYTEA NOT NULL CHECK (octet_length(ed25519_public_key) = 32),
    status TEXT NOT NULL CHECK (status IN ('CURRENT','PREVIOUS','REVOKED')),
    rotated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (device_id, epoch)
);
-- Exactly one CURRENT epoch per device.
CREATE UNIQUE INDEX device_keys_one_current ON device_keys (device_id) WHERE status = 'CURRENT';

-- Maker/checker provisioning ledger. The same table carries device
-- provisioning (payload.type = 'PROVISION') and key-rotation fleet actions
-- (payload.type = 'ROTATE'): both are four-eyes, consume-on-use decisions.
-- maker <> checker is enforced here (SQL) and in the application.
CREATE TABLE provisioning_requests (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id TEXT NOT NULL,
    payload JSONB NOT NULL,
    requested_by TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING'
        CHECK (status IN ('PENDING','APPROVED','REJECTED','CONSUMED')),
    decided_by TEXT,
    decided_at TIMESTAMPTZ,
    -- SHA-256 of the one-time activation secret; the secret itself is
    -- returned exactly once at approval time and never persisted.
    activation_secret_hash BYTEA CHECK (activation_secret_hash IS NULL OR octet_length(activation_secret_hash) = 32),
    device_id UUID REFERENCES devices(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (decided_by IS NULL OR decided_by <> requested_by)
);

-- OTA firmware releases. artifact_url points at the EXTERNAL artifact
-- store (this service never hosts artifacts); artifact_sha256 is the
-- device-side verification anchor carried in the signed manifest.
CREATE TABLE firmware_releases (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('AIS','GT06','LORAWAN','POS','VALIDATOR','GATEWAY','SENSOR')),
    version TEXT NOT NULL,
    artifact_sha256 TEXT NOT NULL CHECK (artifact_sha256 ~ '^[0-9a-f]{64}$'),
    artifact_url TEXT NOT NULL,
    rollout_percent INTEGER NOT NULL CHECK (rollout_percent BETWEEN 0 AND 100),
    min_epoch INTEGER NOT NULL DEFAULT 1 CHECK (min_epoch >= 1),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, kind, version)
);

-- Audit ledger for provisioning decisions, authentication outcomes,
-- rotations and lifecycle transitions. tenant_id may be '' for
-- device-plane events that predate tenant resolution (unknown devices).
CREATE TABLE device_audit_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id TEXT NOT NULL DEFAULT '',
    device_id UUID,
    event TEXT NOT NULL,
    actor TEXT NOT NULL,
    reason TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX device_audit_events_device ON device_audit_events (device_id, created_at);

-- RLS: tenant policies are default-deny (0007 doctrine — no NULL-admit
-- clause). geo_devices holds the platform-wide read the verify-at-ingest
-- path needs; every other session binds app.tenant_id or sees nothing.
ALTER TABLE devices ENABLE ROW LEVEL SECURITY;
ALTER TABLE devices FORCE ROW LEVEL SECURITY;
CREATE POLICY devices_tenant_policy ON devices
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
CREATE POLICY devices_deviceplane_read ON devices
    FOR SELECT TO geo_devices
    USING (true);

ALTER TABLE device_keys ENABLE ROW LEVEL SECURITY;
ALTER TABLE device_keys FORCE ROW LEVEL SECURITY;
CREATE POLICY device_keys_tenant_policy ON device_keys
    USING (EXISTS (SELECT 1 FROM devices d
                   WHERE d.id = device_keys.device_id
                     AND d.tenant_id = current_setting('app.tenant_id', true)))
    WITH CHECK (EXISTS (SELECT 1 FROM devices d
                        WHERE d.id = device_keys.device_id
                          AND d.tenant_id = current_setting('app.tenant_id', true)));
CREATE POLICY device_keys_deviceplane_read ON device_keys
    FOR SELECT TO geo_devices
    USING (true);

ALTER TABLE provisioning_requests ENABLE ROW LEVEL SECURITY;
ALTER TABLE provisioning_requests FORCE ROW LEVEL SECURITY;
CREATE POLICY provisioning_requests_tenant_policy ON provisioning_requests
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
-- Activation consume-on-use: the device-plane role may read a request and
-- flip exactly the status column (APPROVED -> CONSUMED), nothing else.
CREATE POLICY provisioning_requests_deviceplane_read ON provisioning_requests
    FOR SELECT TO geo_devices
    USING (true);
CREATE POLICY provisioning_requests_deviceplane_consume ON provisioning_requests
    FOR UPDATE TO geo_devices
    USING (status = 'APPROVED')
    WITH CHECK (status = 'CONSUMED');

ALTER TABLE firmware_releases ENABLE ROW LEVEL SECURITY;
ALTER TABLE firmware_releases FORCE ROW LEVEL SECURITY;
CREATE POLICY firmware_releases_tenant_policy ON firmware_releases
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
CREATE POLICY firmware_releases_deviceplane_read ON firmware_releases
    FOR SELECT TO geo_devices
    USING (true);

ALTER TABLE device_audit_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE device_audit_events FORCE ROW LEVEL SECURITY;
CREATE POLICY device_audit_events_tenant_policy ON device_audit_events
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
CREATE POLICY device_audit_events_deviceplane_insert ON device_audit_events
    FOR INSERT TO geo_devices
    WITH CHECK (true);

GRANT SELECT, INSERT, UPDATE, DELETE ON devices TO geo;
GRANT SELECT, INSERT, UPDATE, DELETE ON device_keys TO geo;
GRANT SELECT, INSERT, UPDATE ON provisioning_requests TO geo;
GRANT SELECT, INSERT ON firmware_releases TO geo;
GRANT SELECT, INSERT ON device_audit_events TO geo;

GRANT SELECT ON devices TO geo_devices;
GRANT SELECT ON device_keys TO geo_devices;
GRANT SELECT ON firmware_releases TO geo_devices;
GRANT SELECT ON provisioning_requests TO geo_devices;
GRANT UPDATE (status) ON provisioning_requests TO geo_devices;
GRANT INSERT ON device_audit_events TO geo_devices;
