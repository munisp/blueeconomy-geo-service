-- 0013_geofence_v2: PostGIS-backed versioned geofences (WP-10), persisted
-- fence transition events (ENTER/EXIT/DWELL), and the port queue-length
-- observation series that feeds the congestion baseline forecaster.
--
-- Versioning model: a geofence is identified by geofence_id; every geometry
-- or threshold change inserts a NEW row with version+1. At most one row per
-- geofence_id may be ACTIVE (partial unique index). Geometry edits never
-- mutate history, so every fence event can be attributed to the exact fence
-- version that produced it (provenance).

CREATE TABLE geofences (
    geofence_id TEXT NOT NULL CHECK (geofence_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,63}$'),
    version INTEGER NOT NULL CHECK (version >= 1),
    tenant_id TEXT NOT NULL CHECK (length(tenant_id) BETWEEN 1 AND 128),
    name TEXT NOT NULL CHECK (length(name) BETWEEN 1 AND 256),
    classification TEXT NOT NULL CHECK (classification IN ('PUBLIC', 'INTERNAL', 'RESTRICTED', 'CONFIDENTIAL', 'SECRET')),
    geom geography(POLYGON, 4326) NOT NULL,
    -- Fixed-point micro-degree vertex ring (closed, CCW), mirrored from geom
    -- at the write boundary so readers never round-trip floats.
    vertices_micros JSONB NOT NULL,
    dwell_threshold_seconds INTEGER NOT NULL DEFAULT 0 CHECK (dwell_threshold_seconds BETWEEN 0 AND 86400),
    dwell_speed_gate_milliknots INTEGER NOT NULL DEFAULT 1000 CHECK (dwell_speed_gate_milliknots BETWEEN 0 AND 102300),
    state TEXT NOT NULL DEFAULT 'ACTIVE' CHECK (state IN ('ACTIVE', 'RETIRED')),
    created_by TEXT NOT NULL CHECK (length(created_by) BETWEEN 1 AND 128),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    retired_at TIMESTAMPTZ,
    PRIMARY KEY (geofence_id, version)
);

-- At most one ACTIVE version per geofence.
CREATE UNIQUE INDEX geofences_one_active ON geofences (geofence_id) WHERE state = 'ACTIVE';
CREATE INDEX geofences_geom_gist ON geofences USING GIST (geom) WHERE state = 'ACTIVE';

CREATE TABLE geofence_transition_events (
    event_id TEXT NOT NULL PRIMARY KEY CHECK (event_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    geofence_id TEXT NOT NULL,
    geofence_version INTEGER NOT NULL,
    tenant_id TEXT NOT NULL,
    event_type TEXT NOT NULL CHECK (event_type IN ('ENTER', 'EXIT', 'DWELL')),
    mmsi TEXT NOT NULL CHECK (mmsi ~ '^[0-9]{9}$'),
    latitude_micros INTEGER NOT NULL CHECK (latitude_micros BETWEEN -90000000 AND 90000000),
    longitude_micros INTEGER NOT NULL CHECK (longitude_micros BETWEEN -180000000 AND 180000000),
    classification TEXT NOT NULL CHECK (classification IN ('PUBLIC', 'INTERNAL', 'RESTRICTED', 'CONFIDENTIAL', 'SECRET')),
    -- Signed geo.fence.v1 envelope digest (hex SHA-256 of the canonical
    -- envelope); empty only until the publisher acknowledges — readers must
    -- treat empty digest rows as unprovenanced and the API hides them.
    envelope_digest TEXT NOT NULL DEFAULT '' CHECK (envelope_digest = '' OR envelope_digest ~ '^[0-9a-f]{64}$'),
    occurred_at TIMESTAMPTZ NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    FOREIGN KEY (geofence_id, geofence_version) REFERENCES geofences (geofence_id, version)
);
CREATE INDEX geofence_transition_events_vessel_time ON geofence_transition_events (mmsi, occurred_at DESC);
CREATE INDEX geofence_transition_events_fence_time ON geofence_transition_events (geofence_id, occurred_at DESC);

-- Tenant RLS (0014 doctrine): default deny on unbound sessions; writers
-- bind app.tenant_id per transaction.
ALTER TABLE geofence_transition_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE geofence_transition_events FORCE ROW LEVEL SECURITY;
CREATE POLICY geofence_transition_events_tenant_policy ON geofence_transition_events
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
GRANT SELECT, INSERT ON geofence_transition_events TO geo;

-- Port queue-length observations (from eCallUp / gate events). This is the
-- recorded history the congestion baseline forecaster trains on. Fail-closed:
-- the forecaster reads ONLY this table; when empty it reports
-- INSUFFICIENT_HISTORY rather than inventing a forecast.
CREATE TABLE port_queue_observations (
    port_code TEXT NOT NULL CHECK (port_code ~ '^[A-Z]{5}$'),
    queue_length INTEGER NOT NULL CHECK (queue_length >= 0),
    source TEXT NOT NULL CHECK (length(source) BETWEEN 1 AND 128),
    observed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (port_code, observed_at)
);
CREATE INDEX port_queue_observations_time ON port_queue_observations (observed_at DESC);
