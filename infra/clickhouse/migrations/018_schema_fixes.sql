-- Migration 018: schema performance and correctness fixes
--
-- 1. Fix typed_discovery_results MV: two independent arrayJoin calls produce a
--    cross-product (N candidates → N² rows). Replaced with a single arrayJoin on
--    a zipped (index, value) tuple. Existing data is truncated and rehydrated.
--
--    On a fresh install this MV drop+recreate is idempotent with the definition
--    in migration 016 (which already has the correct single-arrayJoin form).
--
-- 2. ORDER BY / PRIMARY KEY corrections are handled at CREATE TABLE time in the
--    original migrations (008, 009, 013, 016) so fresh installs get the correct
--    sort keys without needing ALTER TABLE mutations here.
--
--    For existing instances running ClickHouse < 25.x, MODIFY PRIMARY KEY is not
--    supported, so ALTER TABLE MODIFY ORDER BY cannot change the leading column.
--    Those instances retain the original sort key; the correctness fixes below
--    and the dbt model updates are what matter for data quality.

-- ============================================================================
-- 1. typed_discovery_results — eliminate N² arrayJoin cross-product
-- ============================================================================

DROP VIEW IF EXISTS naap.mv_typed_discovery_results;
TRUNCATE TABLE naap.typed_discovery_results;

CREATE MATERIALIZED VIEW naap.mv_typed_discovery_results
TO naap.typed_discovery_results
AS
SELECT
    concat(event_id, '#', lower(JSONExtractString(pair.2, 'address')), '#', toString(pair.1)) AS row_id,
    event_id,
    event_ts,
    org,
    gateway,
    lower(JSONExtractString(pair.2, 'address')) AS orch_address,
    JSONExtractString(pair.2, 'url')            AS orch_url,
    toUInt64OrDefault(JSONExtractString(pair.2, 'latency_ms')) AS latency_ms,
    pair.2                                      AS data
FROM (
    SELECT
        event_id,
        event_ts,
        org,
        gateway,
        -- Single arrayJoin on a zipped (index, value) array — no cross-product
        arrayJoin(
            arrayMap((x, i) -> tuple(i, x),
                JSONExtractArrayRaw(data),
                arrayEnumerate(JSONExtractArrayRaw(data)))
        ) AS pair
    FROM naap.events
    WHERE event_type = 'discovery_results'
      AND data NOT IN ('', '[]', 'null')
)
WHERE lower(JSONExtractString(pair.2, 'address')) != '';

-- Rehydrate from events with corrected (non-exploded) row counts.
-- On a fresh install this produces zero rows (no events yet) — safe to run.
INSERT INTO naap.typed_discovery_results
SELECT
    concat(event_id, '#', lower(JSONExtractString(pair.2, 'address')), '#', toString(pair.1)) AS row_id,
    event_id,
    event_ts,
    org,
    gateway,
    lower(JSONExtractString(pair.2, 'address')) AS orch_address,
    JSONExtractString(pair.2, 'url')            AS orch_url,
    toUInt64OrDefault(JSONExtractString(pair.2, 'latency_ms')) AS latency_ms,
    pair.2                                      AS data
FROM (
    SELECT
        event_id,
        event_ts,
        org,
        gateway,
        arrayJoin(
            arrayMap((x, i) -> tuple(i, x),
                JSONExtractArrayRaw(data),
                arrayEnumerate(JSONExtractArrayRaw(data)))
        ) AS pair
    FROM naap.events
    WHERE event_type = 'discovery_results'
      AND data NOT IN ('', '[]', 'null')
)
WHERE lower(JSONExtractString(pair.2, 'address')) != '';
