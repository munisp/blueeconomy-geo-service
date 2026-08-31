-- 0003_events: geofence crossings, Tier-0 app position reports and SOS
-- alerts. Idempotency for mobile-originated records is enforced by
-- UNIQUE(reporter_id, outbox_id) per the geo.app-position-report.v1 and
-- geo.sos.v1 contracts (offline replays apply exactly once).

CREATE TABLE geofence_events (
    geofence_event_id TEXT PRIMARY KEY CHECK (geofence_event_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    tenant_id TEXT NOT NULL CHECK (tenant_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    zone_id TEXT NOT NULL REFERENCES geofence_zones(zone_id),
    zone_name TEXT NOT NULL CHECK (length(zone_name) BETWEEN 1 AND 200),
    event TEXT NOT NULL CHECK (event IN ('ENTER', 'EXIT')),
    mmsi TEXT CHECK (mmsi IS NULL OR mmsi ~ '^[0-9]{9}$'),
    track_reference TEXT CHECK (track_reference IS NULL OR length(track_reference) BETWEEN 1 AND 128),
    latitude_micros INTEGER NOT NULL CHECK (latitude_micros BETWEEN -90000000 AND 90000000),
    longitude_micros INTEGER NOT NULL CHECK (longitude_micros BETWEEN -180000000 AND 180000000),
    classification TEXT NOT NULL CHECK (classification IN ('PUBLIC', 'INTERNAL', 'RESTRICTED', 'CONFIDENTIAL', 'SECRET')),
    occurred_at TIMESTAMPTZ NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Contract invariant: exactly one of mmsi / track_reference.
    CHECK ((mmsi IS NULL) <> (track_reference IS NULL))
);

CREATE INDEX geofence_events_zone_idx ON geofence_events (zone_id, occurred_at DESC);
CREATE INDEX geofence_events_mmsi_idx ON geofence_events (mmsi, occurred_at DESC) WHERE mmsi IS NOT NULL;

CREATE TABLE app_position_reports (
    position_report_id TEXT PRIMARY KEY CHECK (position_report_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    reporter_id TEXT NOT NULL CHECK (length(reporter_id) BETWEEN 1 AND 128),
    vessel_reference TEXT NOT NULL CHECK (length(vessel_reference) BETWEEN 1 AND 128),
    latitude_micros INTEGER NOT NULL CHECK (latitude_micros BETWEEN -90000000 AND 90000000),
    longitude_micros INTEGER NOT NULL CHECK (longitude_micros BETWEEN -180000000 AND 180000000),
    accuracy_m INTEGER NOT NULL DEFAULT 0 CHECK (accuracy_m >= 0),
    speed_millimetres_per_second INTEGER CHECK (speed_millimetres_per_second IS NULL OR speed_millimetres_per_second >= 0),
    recorded_at TIMESTAMPTZ NOT NULL,
    outbox_id TEXT NOT NULL CHECK (length(outbox_id) BETWEEN 1 AND 128),
    classification TEXT NOT NULL DEFAULT 'PUBLIC' CHECK (classification IN ('PUBLIC', 'INTERNAL', 'RESTRICTED', 'CONFIDENTIAL', 'SECRET')),
    received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (reporter_id, outbox_id),
    CHECK (NOT (latitude_micros = 0 AND longitude_micros = 0))
);

-- SOS alerts: classification floor RESTRICTED enforced at the storage
-- boundary, mirroring the geo.sos.v1 contract floor.
CREATE TABLE sos_alerts (
    sos_alert_id TEXT PRIMARY KEY CHECK (sos_alert_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    reporter_id TEXT NOT NULL CHECK (length(reporter_id) BETWEEN 1 AND 128),
    vessel_reference TEXT NOT NULL CHECK (length(vessel_reference) BETWEEN 1 AND 128),
    latitude_micros INTEGER NOT NULL CHECK (latitude_micros BETWEEN -90000000 AND 90000000),
    longitude_micros INTEGER NOT NULL CHECK (longitude_micros BETWEEN -180000000 AND 180000000),
    recorded_at TIMESTAMPTZ NOT NULL,
    outbox_id TEXT NOT NULL CHECK (length(outbox_id) BETWEEN 1 AND 128),
    free_text TEXT NOT NULL DEFAULT '' CHECK (length(free_text) <= 280),
    classification TEXT NOT NULL CHECK (classification IN ('RESTRICTED', 'CONFIDENTIAL', 'SECRET')),
    state TEXT NOT NULL DEFAULT 'RAISED' CHECK (state IN ('RAISED', 'ACKNOWLEDGED', 'RESOLVED')),
    received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (reporter_id, outbox_id),
    CHECK (NOT (latitude_micros = 0 AND longitude_micros = 0))
);

CREATE INDEX sos_alerts_state_idx ON sos_alerts (state, received_at DESC) WHERE state <> 'RESOLVED';

GRANT SELECT, INSERT ON geofence_events, app_position_reports TO geo;
GRANT SELECT, INSERT, UPDATE ON sos_alerts TO geo;
