-- 0011_partition_tz_utc (PRA-128): geo_ensure_position_partition cast
-- day::timestamptz, which resolves midnight in the SESSION TimeZone. On any
-- server whose timezone is not UTC (e.g. Asia/Shanghai), daily partitions
-- were bounded <day> 16:00 UTC -> <day+1> 16:00 UTC instead of the UTC
-- calendar day, silently misrouting AIS rows observed in the off-UTC window
-- and breaking the "insert into an unprovisioned day fails closed" doctrine.
--
-- Fix: compute the bounds explicitly in UTC
-- (day::timestamp AT TIME ZONE 'UTC'), which is session-TimeZone immune.
-- 0002 is immutable (already applied); this migration recreates the
-- function only.
--
-- OPERATOR ATTENTION: partitions created BEFORE this migration on a
-- non-UTC server are misaligned and are NOT corrected here — their bounds
-- overlap/gap the true UTC day, so rows may sit in the wrong daily
-- partition. Remediation is an operational decision (rebuild the affected
-- partitions by moving rows into UTC-aligned partitions); it is
-- deliberately not automated in this migration.

CREATE OR REPLACE FUNCTION geo_ensure_position_partition(day DATE) RETURNS TEXT AS $$
DECLARE
    partition_name TEXT := 'ais_positions_' || to_char(day, 'YYYYMMDD');
    -- UTC-explicit: midnight of the UTC calendar day, independent of the
    -- session TimeZone (day::timestamptz was session-TZ relative).
    start_ts TIMESTAMPTZ := (day::timestamp AT TIME ZONE 'UTC');
    end_ts TIMESTAMPTZ := ((day + 1)::timestamp AT TIME ZONE 'UTC');
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = partition_name) THEN
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF ais_positions FOR VALUES FROM (%L) TO (%L)',
            partition_name, start_ts, end_ts);
    END IF;
    RETURN partition_name;
END;
$$ LANGUAGE plpgsql;
