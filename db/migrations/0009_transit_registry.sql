-- 0009_transit_registry: the NIWA/operator-maintained route & jetty
-- registry — the master data the GTFS static feed factory, the GTFS-RT
-- producer (AIS→vehicle-position adapter) and the ETA engine consume
-- (Citizen Services Advisory §5, three-tier service taxonomy).
--
-- Tenant model: every table is tenant-scoped and follows the 0007
-- default-deny RLS posture (FORCE ROW LEVEL SECURITY; the row is visible
-- only when the transaction binds app.tenant_id to the row's tenant).
-- The application role `geo` receives DML grants only; migrations own the
-- tables. The feed builders read this registry per tenant and join it
-- against the SHARED position plane (latest_positions / ais_positions,
-- classification-clearance enforced at the service layer) — the registry
-- is tenant-governed master data, the position plane is not.
--
-- Doctrine: this registry is the source of truth operators maintain
-- (seedable via cmd/geo-transitseed). The GTFS-RT layer NEVER fabricates
-- schedule rows; what is not in the registry is not in the feed.

CREATE TABLE transit_agencies (
    agency_id TEXT PRIMARY KEY CHECK (agency_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    tenant_id TEXT NOT NULL CHECK (tenant_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    name TEXT NOT NULL CHECK (length(name) BETWEEN 1 AND 200),
    url TEXT NOT NULL CHECK (length(url) BETWEEN 1 AND 512),
    -- IANA timezone name used to render calendar/service days.
    timezone TEXT NOT NULL CHECK (length(timezone) BETWEEN 1 AND 64),
    lang TEXT NOT NULL DEFAULT '' CHECK (length(lang) <= 16),
    phone TEXT NOT NULL DEFAULT '' CHECK (length(phone) <= 64)
);

CREATE TABLE transit_routes (
    route_id TEXT PRIMARY KEY CHECK (route_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    tenant_id TEXT NOT NULL CHECK (tenant_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    agency_id TEXT NOT NULL REFERENCES transit_agencies(agency_id),
    short_name TEXT NOT NULL DEFAULT '' CHECK (length(short_name) <= 64),
    long_name TEXT NOT NULL DEFAULT '' CHECK (length(long_name) <= 200),
    -- GTFS route_type; 4 = ferry (water transport is first-class GTFS).
    route_type INTEGER NOT NULL DEFAULT 4 CHECK (route_type BETWEEN 0 AND 1702),
    -- Fallback speed for the ETA engine, used ONLY when a vessel has no
    -- reported speed observations; such trip updates are emitted with
    -- schedule_relationship=SCHEDULED (honest static fallback), never as
    -- live predictions.
    default_speed_milliknots INTEGER NOT NULL DEFAULT 6000 CHECK (default_speed_milliknots BETWEEN 500 AND 60000),
    active BOOLEAN NOT NULL DEFAULT true,
    CHECK (short_name <> '' OR long_name <> '')
);
CREATE INDEX transit_routes_agency_idx ON transit_routes (agency_id);
CREATE INDEX transit_routes_active_idx ON transit_routes (active) WHERE active;

-- Jetties/terminals as GTFS stops. Coordinates are stored fixed-point
-- micro-degrees per the geo.*.v1 doctrine; the geography column is derived
-- at the storage boundary (no float round-trip).
CREATE TABLE transit_stops (
    stop_id TEXT PRIMARY KEY CHECK (stop_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    tenant_id TEXT NOT NULL CHECK (tenant_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    name TEXT NOT NULL CHECK (length(name) BETWEEN 1 AND 200),
    geom geography(POINT, 4326) NOT NULL,
    latitude_micros INTEGER NOT NULL CHECK (latitude_micros BETWEEN -90000000 AND 90000000),
    longitude_micros INTEGER NOT NULL CHECK (longitude_micros BETWEEN -180000000 AND 180000000),
    -- GTFS zone_id (fare zone); free text, may be empty.
    zone_id TEXT NOT NULL DEFAULT '' CHECK (length(zone_id) <= 64)
);
CREATE INDEX transit_stops_geom_idx ON transit_stops USING GiST (geom);

-- Weekly service calendars (GTFS calendar.txt). Dates are inclusive
-- service-day bounds in the agency timezone.
CREATE TABLE transit_calendars (
    service_id TEXT PRIMARY KEY CHECK (service_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    tenant_id TEXT NOT NULL CHECK (tenant_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    monday BOOLEAN NOT NULL,
    tuesday BOOLEAN NOT NULL,
    wednesday BOOLEAN NOT NULL,
    thursday BOOLEAN NOT NULL,
    friday BOOLEAN NOT NULL,
    saturday BOOLEAN NOT NULL,
    sunday BOOLEAN NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    CHECK (end_date >= start_date)
);

CREATE TABLE transit_trips (
    trip_id TEXT PRIMARY KEY CHECK (trip_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    tenant_id TEXT NOT NULL CHECK (tenant_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    route_id TEXT NOT NULL REFERENCES transit_routes(route_id),
    service_id TEXT NOT NULL REFERENCES transit_calendars(service_id),
    headsign TEXT NOT NULL DEFAULT '' CHECK (length(headsign) <= 200),
    direction_id SMALLINT CHECK (direction_id IN (0, 1))
);
CREATE INDEX transit_trips_route_idx ON transit_trips (route_id);
CREATE INDEX transit_trips_service_idx ON transit_trips (service_id);

-- Scheduled stop visits. arrival/departure are SECONDS after midnight
-- (agency timezone, GTFS convention; may exceed 86400 for after-midnight
-- trips). Monotonic per trip enforced at the storage boundary.
CREATE TABLE transit_stop_times (
    tenant_id TEXT NOT NULL CHECK (tenant_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    trip_id TEXT NOT NULL REFERENCES transit_trips(trip_id),
    stop_sequence INTEGER NOT NULL CHECK (stop_sequence > 0),
    stop_id TEXT NOT NULL REFERENCES transit_stops(stop_id),
    arrival_seconds INTEGER NOT NULL CHECK (arrival_seconds >= 0 AND arrival_seconds < 172800),
    departure_seconds INTEGER NOT NULL CHECK (departure_seconds >= 0 AND departure_seconds < 172800),
    PRIMARY KEY (trip_id, stop_sequence),
    CHECK (departure_seconds >= arrival_seconds)
);
CREATE INDEX transit_stop_times_stop_idx ON transit_stop_times (stop_id);

-- Route ↔ vessel assignment: which AIS identity (MMSI, already the geo
-- primary key and the GTFS-RT vehicle.id) serves which route during which
-- window. NULL bounds are open-ended. A vessel may serve several routes
-- over time; overlapping windows for one MMSI are rejected by the
-- exclusion constraint (trip matching must never be ambiguous).
CREATE TABLE transit_route_vessels (
    tenant_id TEXT NOT NULL CHECK (tenant_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    route_id TEXT NOT NULL REFERENCES transit_routes(route_id),
    mmsi TEXT NOT NULL CHECK (mmsi ~ '^[0-9]{9}$'),
    imo TEXT NOT NULL DEFAULT '' CHECK (imo = '' OR imo ~ '^[0-9]{7}$'),
    valid_from TIMESTAMPTZ,
    valid_to TIMESTAMPTZ,
    PRIMARY KEY (route_id, mmsi),
    CHECK (valid_to IS NULL OR valid_from IS NULL OR valid_to > valid_from)
);
CREATE INDEX transit_route_vessels_mmsi_idx ON transit_route_vessels (mmsi);

-- Service alerts (GTFS-RT alerts.pb): weather suspensions, channel
-- closures, jetty works. Scoped to a route or a stop (at least one —
-- informed_entity is required by the spec). The active window is
-- half-open [starts_at, ends_at); NULL bounds mean open-ended. `active`
-- is the operator kill-switch.
CREATE TABLE transit_alerts (
    alert_id TEXT PRIMARY KEY CHECK (alert_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    tenant_id TEXT NOT NULL CHECK (tenant_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    cause TEXT NOT NULL CHECK (cause IN ('UNKNOWN_CAUSE','OTHER_CAUSE','TECHNICAL_PROBLEM','STRIKE','DEMONSTRATION','ACCIDENT','HOLIDAY','WEATHER','MAINTENANCE','CONSTRUCTION','POLICE_ACTIVITY','MEDICAL_EMERGENCY')),
    effect TEXT NOT NULL CHECK (effect IN ('NO_SERVICE','REDUCED_SERVICE','SIGNIFICANT_DELAYS','DETOUR','ADDITIONAL_SERVICE','MODIFIED_SERVICE','OTHER_EFFECT','UNKNOWN_EFFECT','STOP_MOVED','NO_EFFECT','ACCESSIBILITY_ISSUE')),
    route_id TEXT REFERENCES transit_routes(route_id),
    stop_id TEXT REFERENCES transit_stops(stop_id),
    starts_at TIMESTAMPTZ,
    ends_at TIMESTAMPTZ,
    header_text TEXT NOT NULL CHECK (length(header_text) BETWEEN 1 AND 512),
    description_text TEXT NOT NULL DEFAULT '' CHECK (length(description_text) <= 4096),
    url TEXT NOT NULL DEFAULT '' CHECK (length(url) <= 512),
    active BOOLEAN NOT NULL DEFAULT true,
    created_by TEXT NOT NULL CHECK (length(created_by) BETWEEN 1 AND 512),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (route_id IS NOT NULL OR stop_id IS NOT NULL),
    CHECK (ends_at IS NULL OR starts_at IS NULL OR ends_at > starts_at)
);
CREATE INDEX transit_alerts_window_idx ON transit_alerts (active, starts_at, ends_at);

-- RLS: default-deny tenant policies, mirroring 0007_rls_default_deny.sql.
-- Unbound sessions (app.tenant_id unset) are denied; bound sessions see
-- exactly their own tenant's registry rows. FORCE applies the policies to
-- the table owner as well.
ALTER TABLE transit_agencies ENABLE ROW LEVEL SECURITY;
ALTER TABLE transit_agencies FORCE ROW LEVEL SECURITY;
CREATE POLICY transit_agencies_tenant_policy ON transit_agencies
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

ALTER TABLE transit_routes ENABLE ROW LEVEL SECURITY;
ALTER TABLE transit_routes FORCE ROW LEVEL SECURITY;
CREATE POLICY transit_routes_tenant_policy ON transit_routes
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

ALTER TABLE transit_stops ENABLE ROW LEVEL SECURITY;
ALTER TABLE transit_stops FORCE ROW LEVEL SECURITY;
CREATE POLICY transit_stops_tenant_policy ON transit_stops
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

ALTER TABLE transit_calendars ENABLE ROW LEVEL SECURITY;
ALTER TABLE transit_calendars FORCE ROW LEVEL SECURITY;
CREATE POLICY transit_calendars_tenant_policy ON transit_calendars
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

ALTER TABLE transit_trips ENABLE ROW LEVEL SECURITY;
ALTER TABLE transit_trips FORCE ROW LEVEL SECURITY;
CREATE POLICY transit_trips_tenant_policy ON transit_trips
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

ALTER TABLE transit_stop_times ENABLE ROW LEVEL SECURITY;
ALTER TABLE transit_stop_times FORCE ROW LEVEL SECURITY;
CREATE POLICY transit_stop_times_tenant_policy ON transit_stop_times
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

ALTER TABLE transit_route_vessels ENABLE ROW LEVEL SECURITY;
ALTER TABLE transit_route_vessels FORCE ROW LEVEL SECURITY;
CREATE POLICY transit_route_vessels_tenant_policy ON transit_route_vessels
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

ALTER TABLE transit_alerts ENABLE ROW LEVEL SECURITY;
ALTER TABLE transit_alerts FORCE ROW LEVEL SECURITY;
CREATE POLICY transit_alerts_tenant_policy ON transit_alerts
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

GRANT SELECT, INSERT, UPDATE, DELETE ON transit_agencies, transit_routes, transit_stops,
    transit_calendars, transit_trips, transit_stop_times, transit_route_vessels TO geo;
GRANT SELECT, INSERT, UPDATE ON transit_alerts TO geo;
