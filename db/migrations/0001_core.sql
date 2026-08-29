-- 0001_core: roles, geofence zones (maker-checker), vessel static data (SCD-2)
-- and the latest-position upsert target.
--
-- Role convention (fleet doctrine): the application connects as role `geo`
-- (NOSUPERUSER NOBYPASSRLS). Migrations run as a separate migrator role that
-- owns the tables; the `geo` role receives DML-only grants and is subject to
-- row-level security on tenant-scoped tables (see 0004_rls.sql).

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'geo') THEN
        CREATE ROLE geo NOSUPERUSER NOBYPASSRLS LOGIN;
    END IF;
END $$;

-- Geofence zones are governed spatial objects. A zone is created by a maker
-- (state 'draft') and only becomes effective after a *different* principal
-- approves it (state 'approved') -- four-eyes, mirroring the CVFF
-- separation-of-duties pattern (UNIQUE(zone_id, principal_id) on the
-- approval ledger and CHECK (checker <> maker)).
CREATE TABLE geofence_zones (
    zone_id TEXT PRIMARY KEY CHECK (zone_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    tenant_id TEXT NOT NULL CHECK (tenant_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    name TEXT NOT NULL CHECK (length(name) BETWEEN 1 AND 200),
    geom geography(POLYGON, 4326) NOT NULL,
    classification_floor TEXT NOT NULL CHECK (classification_floor IN ('PUBLIC', 'INTERNAL', 'RESTRICTED', 'CONFIDENTIAL', 'SECRET')),
    state TEXT NOT NULL DEFAULT 'draft' CHECK (state IN ('draft', 'approved')),
    maker_principal_id TEXT NOT NULL CHECK (length(maker_principal_id) BETWEEN 1 AND 512),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    approved_by TEXT CHECK (approved_by IS NULL OR length(approved_by) BETWEEN 1 AND 512),
    approved_at TIMESTAMPTZ,
    CHECK ((state = 'approved') = (approved_by IS NOT NULL AND approved_at IS NOT NULL)),
    CHECK (approved_by IS NULL OR approved_by <> maker_principal_id)
);

-- Approval ledger: one approval per (zone, principal); a principal can never
-- approve a zone it made (enforced by trigger, mirroring the CVFF four-eyes
-- UNIQUE(application_id, principal_id) discipline).
CREATE TABLE geofence_zone_approvals (
    zone_id TEXT NOT NULL REFERENCES geofence_zones(zone_id),
    principal_id TEXT NOT NULL CHECK (length(principal_id) BETWEEN 1 AND 512),
    decided_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (zone_id, principal_id)
);

CREATE FUNCTION geofence_zone_four_eyes() RETURNS trigger AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM geofence_zone_approvals a WHERE a.zone_id = NEW.zone_id
               AND a.principal_id = (SELECT z.maker_principal_id FROM geofence_zones z WHERE z.zone_id = NEW.zone_id)) THEN
        RAISE EXCEPTION 'geofence zone maker may not approve own zone (four-eyes)';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER geofence_zone_approvals_four_eyes
    AFTER INSERT ON geofence_zone_approvals
    FOR EACH ROW EXECUTE FUNCTION geofence_zone_four_eyes();

CREATE INDEX geofence_zones_geom_idx ON geofence_zones USING GiST (geom);
CREATE INDEX geofence_zones_state_idx ON geofence_zones (state) WHERE state = 'approved';

-- Vessel static and voyage data (AIS message types 5/19/24 or registry
-- updates) kept as slowly-changing dimension type 2: every change closes the
-- previous row (valid_to) and opens a new current row (valid_to IS NULL).
CREATE TABLE vessels_static (
    static_report_id TEXT NOT NULL CHECK (static_report_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    mmsi TEXT NOT NULL CHECK (mmsi ~ '^[0-9]{9}$'),
    imo TEXT NOT NULL DEFAULT '' CHECK (imo = '' OR imo ~ '^[0-9]{7}$'),
    callsign TEXT NOT NULL DEFAULT '' CHECK (length(callsign) <= 16),
    ship_name TEXT NOT NULL DEFAULT '' CHECK (length(ship_name) <= 128),
    ship_type_code INTEGER NOT NULL DEFAULT 0 CHECK (ship_type_code BETWEEN 0 AND 99),
    dimension_bow_m INTEGER NOT NULL DEFAULT 0 CHECK (dimension_bow_m BETWEEN 0 AND 511),
    dimension_stern_m INTEGER NOT NULL DEFAULT 0 CHECK (dimension_stern_m BETWEEN 0 AND 511),
    dimension_port_m INTEGER NOT NULL DEFAULT 0 CHECK (dimension_port_m BETWEEN 0 AND 63),
    dimension_starboard_m INTEGER NOT NULL DEFAULT 0 CHECK (dimension_starboard_m BETWEEN 0 AND 63),
    draught_millimetres INTEGER NOT NULL DEFAULT 0 CHECK (draught_millimetres BETWEEN 0 AND 25500),
    destination TEXT NOT NULL DEFAULT '' CHECK (length(destination) <= 128),
    eta TIMESTAMPTZ,
    epfs_type TEXT NOT NULL DEFAULT 'UNSPECIFIED' CHECK (epfs_type IN ('GPS', 'GLONASS', 'COMBINED_GPS_GLONASS', 'LORAN_C', 'CHAYKA', 'INTEGRATED_NAVIGATION', 'OBSERVED', 'GALILEO', 'INTERNAL_GNSS', 'UNSPECIFIED')),
    source_class TEXT NOT NULL CHECK (source_class IN ('AIS', 'GSM_TRACKER', 'SAT_TRACKER', 'APP_REPORT')),
    classification TEXT NOT NULL CHECK (classification IN ('PUBLIC', 'INTERNAL', 'RESTRICTED', 'CONFIDENTIAL', 'SECRET')),
    observed_at TIMESTAMPTZ NOT NULL,
    valid_from TIMESTAMPTZ NOT NULL,
    valid_to TIMESTAMPTZ,
    PRIMARY KEY (static_report_id, valid_from),
    CHECK (valid_to IS NULL OR valid_to > valid_from)
);

-- At most one current (open) SCD-2 row per MMSI.
CREATE UNIQUE INDEX vessels_static_current_idx ON vessels_static (mmsi) WHERE valid_to IS NULL;
CREATE INDEX vessels_static_mmsi_idx ON vessels_static (mmsi, valid_from DESC);

-- Hot upsert target: the most recent validated position per vessel. Exactly
-- one of mmsi / vessel_ref is populated (app reports may have no MMSI).
CREATE TABLE latest_positions (
    mmsi TEXT CHECK (mmsi IS NULL OR mmsi ~ '^[0-9]{9}$'),
    vessel_ref TEXT CHECK (vessel_ref IS NULL OR length(vessel_ref) BETWEEN 1 AND 128),
    position_report_id TEXT NOT NULL CHECK (position_report_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    source_class TEXT NOT NULL CHECK (source_class IN ('AIS', 'GSM_TRACKER', 'SAT_TRACKER', 'APP_REPORT')),
    geom geography(POINT, 4326) NOT NULL,
    latitude_micros INTEGER NOT NULL CHECK (latitude_micros BETWEEN -90000000 AND 90000000),
    longitude_micros INTEGER NOT NULL CHECK (longitude_micros BETWEEN -180000000 AND 180000000),
    speed_over_ground_milliknots INTEGER CHECK (speed_over_ground_milliknots IS NULL OR speed_over_ground_milliknots BETWEEN 0 AND 102300),
    course_over_ground_millidegrees INTEGER CHECK (course_over_ground_millidegrees IS NULL OR course_over_ground_millidegrees BETWEEN 0 AND 360000),
    heading_millidegrees INTEGER CHECK (heading_millidegrees IS NULL OR heading_millidegrees BETWEEN 0 AND 360000),
    nav_status INTEGER CHECK (nav_status IS NULL OR nav_status BETWEEN 0 AND 15),
    classification TEXT NOT NULL CHECK (classification IN ('PUBLIC', 'INTERNAL', 'RESTRICTED', 'CONFIDENTIAL', 'SECRET')),
    observed_at TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (mmsi IS NOT NULL OR vessel_ref IS NOT NULL),
    CHECK (NOT (latitude_micros = 0 AND longitude_micros = 0))
);

-- One upsert row per vessel identity.
CREATE UNIQUE INDEX latest_positions_mmsi_idx ON latest_positions (mmsi) WHERE mmsi IS NOT NULL;
CREATE UNIQUE INDEX latest_positions_vessel_ref_idx ON latest_positions (vessel_ref) WHERE vessel_ref IS NOT NULL;
CREATE INDEX latest_positions_geom_idx ON latest_positions USING GiST (geom);
CREATE INDEX latest_positions_classification_idx ON latest_positions (classification, observed_at DESC);

GRANT SELECT, INSERT, UPDATE ON geofence_zones, geofence_zone_approvals TO geo;
GRANT SELECT, INSERT, UPDATE ON vessels_static, latest_positions TO geo;
