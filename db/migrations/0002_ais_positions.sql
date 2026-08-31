-- 0002_ais_positions: the validated vessel-position hot table. RANGE
-- partitioned daily on observed_at (declarative partitioning); BRIN on
-- observed_at for time-ordered scans, GiST on geom for spatial predicates on
-- the hot partitions. All coordinates and speeds are fixed-point integers
-- (micro-degrees, milli-knots, milli-degrees) per the geo.*.v1 contracts;
-- floating-point coordinates are prohibited.

CREATE TABLE ais_positions (
    position_report_id TEXT NOT NULL CHECK (position_report_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    mmsi TEXT CHECK (mmsi IS NULL OR mmsi ~ '^[0-9]{9}$'),
    vessel_ref TEXT CHECK (vessel_ref IS NULL OR length(vessel_ref) BETWEEN 1 AND 128),
    source_class TEXT NOT NULL CHECK (source_class IN ('AIS', 'GSM_TRACKER', 'SAT_TRACKER', 'APP_REPORT')),
    geom geography(POINT, 4326) NOT NULL,
    latitude_micros INTEGER NOT NULL CHECK (latitude_micros BETWEEN -90000000 AND 90000000),
    longitude_micros INTEGER NOT NULL CHECK (longitude_micros BETWEEN -180000000 AND 180000000),
    speed_over_ground_milliknots INTEGER CHECK (speed_over_ground_milliknots IS NULL OR speed_over_ground_milliknots BETWEEN 0 AND 102300),
    course_over_ground_millidegrees INTEGER CHECK (course_over_ground_millidegrees IS NULL OR course_over_ground_millidegrees BETWEEN 0 AND 360000),
    heading_millidegrees INTEGER CHECK (heading_millidegrees IS NULL OR heading_millidegrees BETWEEN 0 AND 360000),
    nav_status INTEGER CHECK (nav_status IS NULL OR nav_status BETWEEN 0 AND 15),
    position_accuracy TEXT NOT NULL DEFAULT 'UNSPECIFIED' CHECK (position_accuracy IN ('UNSPECIFIED', 'LOW', 'HIGH')),
    receiver_id TEXT NOT NULL DEFAULT '' CHECK (length(receiver_id) <= 128),
    ais_message_type INTEGER CHECK (ais_message_type IS NULL OR ais_message_type BETWEEN 1 AND 27),
    imo TEXT NOT NULL DEFAULT '' CHECK (imo = '' OR imo ~ '^[0-9]{7}$'),
    callsign TEXT NOT NULL DEFAULT '' CHECK (length(callsign) <= 16),
    ship_name TEXT NOT NULL DEFAULT '' CHECK (length(ship_name) <= 128),
    classification TEXT NOT NULL CHECK (classification IN ('PUBLIC', 'INTERNAL', 'RESTRICTED', 'CONFIDENTIAL', 'SECRET')),
    observed_at TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (position_report_id, observed_at),
    CHECK (mmsi IS NOT NULL OR vessel_ref IS NOT NULL),
    -- Contract validation mirrored at the storage boundary: MMSI is
    -- mandatory for every source class except APP_REPORT, and the (0,0)
    -- null-island is rejected outright.
    CHECK (source_class = 'APP_REPORT' OR mmsi IS NOT NULL),
    CHECK (NOT (latitude_micros = 0 AND longitude_micros = 0))
) PARTITION BY RANGE (observed_at);

-- Idempotent daily partition provisioning. The ingest service calls this for
-- today and tomorrow at startup and on a daily timer; inserts into a day
-- without a partition fail closed (no default partition silently absorbing
-- misrouted rows).
CREATE FUNCTION geo_ensure_position_partition(day DATE) RETURNS TEXT AS $$
DECLARE
    partition_name TEXT := 'ais_positions_' || to_char(day, 'YYYYMMDD');
    start_ts TIMESTAMPTZ := day::timestamptz;
    end_ts TIMESTAMPTZ := (day + 1)::timestamptz;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = partition_name) THEN
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF ais_positions FOR VALUES FROM (%L) TO (%L)',
            partition_name, start_ts, end_ts);
    END IF;
    RETURN partition_name;
END;
$$ LANGUAGE plpgsql;

-- Provisional partitions around the migration date; the service provisions
-- forward from here via geo_ensure_position_partition.
SELECT geo_ensure_position_partition(current_date - 1);
SELECT geo_ensure_position_partition(current_date);
SELECT geo_ensure_position_partition(current_date + 1);

CREATE INDEX ais_positions_observed_brin ON ais_positions USING BRIN (observed_at) WITH (pages_per_range = 32);
CREATE INDEX ais_positions_geom_idx ON ais_positions USING GiST (geom);
CREATE INDEX ais_positions_mmsi_idx ON ais_positions (mmsi, observed_at DESC) WHERE mmsi IS NOT NULL;
CREATE INDEX ais_positions_classification_idx ON ais_positions (classification, observed_at DESC);

GRANT SELECT, INSERT ON ais_positions TO geo;
GRANT EXECUTE ON FUNCTION geo_ensure_position_partition(DATE) TO geo;
