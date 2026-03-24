-- Migration 011: fix orch_address in stream state and reliability tables
--
-- Root cause: ai_stream_status events carry orchestrator_info.address = the
-- node's local keypair address (e.g. 0x52cf...), NOT the on-chain ETH address
-- used for payments and the network registry (e.g. 0xd003...).
--
-- The correct ETH address is available in agg_orch_state (keyed by ETH address,
-- URI populated by migration 010). We resolve local → ETH via URI:
--   orchestrator_info.url → agg_orch_state.uri → agg_orch_state.orch_address
--
-- ClickHouse 24.3 does not support correlated subqueries in MV definitions;
-- we use a LEFT JOIN against agg_orch_state instead.
--
-- Tables fixed:
--   agg_orch_reliability_hourly  (REL-003 — per-orch reliability breakdown)
--   agg_stream_state             (STR-001 — active streams per orch)
--
-- Procedure for each table:
--   1. Drop the old MV (stop new incorrect writes)
--   2. Truncate the aggregate table (clear bad data)
--   3. Recreate the MV with a LEFT JOIN-based URI lookup
--   4. Backfill the aggregate table from naap.events

-- ── agg_orch_reliability_hourly ──────────────────────────────────────────────

DROP VIEW IF EXISTS naap.mv_orch_reliability_hourly;

TRUNCATE TABLE naap.agg_orch_reliability_hourly;

CREATE MATERIALIZED VIEW IF NOT EXISTS naap.mv_orch_reliability_hourly
TO naap.agg_orch_reliability_hourly
AS
SELECT
    toStartOfHour(e.event_ts)                                             AS hour,
    e.org,
    lower(ifNull(o.orch_address, ''))                                     AS orch_address,
    1                                                                      AS ai_stream_count,
    toUInt64(JSONExtractString(e.data, 'state') IN ('DEGRADED_INFERENCE', 'DEGRADED_INPUT'))
                                                                           AS degraded_count,
    toUInt64(JSONExtractUInt(e.data, 'inference_status', 'restart_count') > 0)
                                                                           AS restart_count,
    toUInt64(JSONExtractString(e.data, 'inference_status', 'last_error') NOT IN ('', 'null'))
                                                                           AS error_count
FROM naap.events e
LEFT JOIN (SELECT uri, orch_address FROM naap.agg_orch_state FINAL) o
    ON o.uri = JSONExtractString(e.data, 'orchestrator_info', 'url')
WHERE e.event_type = 'ai_stream_status'
  AND JSONExtractString(e.data, 'orchestrator_info', 'url') != '';

INSERT INTO naap.agg_orch_reliability_hourly
SELECT
    toStartOfHour(e.event_ts)                                             AS hour,
    e.org,
    lower(ifNull(o.orch_address, ''))                                     AS orch_address,
    1                                                                      AS ai_stream_count,
    toUInt64(JSONExtractString(e.data, 'state') IN ('DEGRADED_INFERENCE', 'DEGRADED_INPUT'))
                                                                           AS degraded_count,
    toUInt64(JSONExtractUInt(e.data, 'inference_status', 'restart_count') > 0)
                                                                           AS restart_count,
    toUInt64(JSONExtractString(e.data, 'inference_status', 'last_error') NOT IN ('', 'null'))
                                                                           AS error_count
FROM naap.events e
LEFT JOIN (SELECT uri, orch_address FROM naap.agg_orch_state FINAL) o
    ON o.uri = JSONExtractString(e.data, 'orchestrator_info', 'url')
WHERE e.event_type = 'ai_stream_status'
  AND JSONExtractString(e.data, 'orchestrator_info', 'url') != '';

-- ── agg_stream_state ─────────────────────────────────────────────────────────

DROP VIEW IF EXISTS naap.mv_stream_state;

TRUNCATE TABLE naap.agg_stream_state;

CREATE MATERIALIZED VIEW IF NOT EXISTS naap.mv_stream_state
TO naap.agg_stream_state
AS
SELECT
    JSONExtractString(e.data, 'stream_id')                                AS stream_id,
    e.org,
    if(e.event_type = 'ai_stream_status',
        JSONExtractString(e.data, 'pipeline'), '')                        AS pipeline,
    if(e.event_type = 'ai_stream_status',
        JSONExtractString(e.data, 'state'), 'UNKNOWN')                    AS state,
    e.event_ts                                                             AS last_seen,
    if(e.event_type = 'stream_trace'
        AND JSONExtractString(e.data, 'type') = 'gateway_receive_stream_request',
        e.event_ts, toDateTime64(0, 3, 'UTC'))                            AS started_at,
    if(e.event_type = 'stream_trace'
        AND JSONExtractString(e.data, 'type') = 'gateway_ingest_stream_closed',
        1, 0)                                                              AS is_closed,
    if(e.event_type = 'stream_trace'
        AND JSONExtractString(e.data, 'type') = 'gateway_no_orchestrators_available',
        1, 0)                                                              AS has_failure,
    if(e.event_type = 'ai_stream_status',
        lower(ifNull(o.orch_address, '')),
        ''
    )                                                                      AS orch_address
FROM naap.events e
LEFT JOIN (SELECT uri, orch_address FROM naap.agg_orch_state FINAL) o
    ON o.uri = JSONExtractString(e.data, 'orchestrator_info', 'url')
WHERE e.event_type IN ('stream_trace', 'ai_stream_status')
  AND JSONExtractString(e.data, 'stream_id') != '';

INSERT INTO naap.agg_stream_state
SELECT
    JSONExtractString(e.data, 'stream_id')                                AS stream_id,
    e.org,
    if(e.event_type = 'ai_stream_status',
        JSONExtractString(e.data, 'pipeline'), '')                        AS pipeline,
    if(e.event_type = 'ai_stream_status',
        JSONExtractString(e.data, 'state'), 'UNKNOWN')                    AS state,
    e.event_ts                                                             AS last_seen,
    if(e.event_type = 'stream_trace'
        AND JSONExtractString(e.data, 'type') = 'gateway_receive_stream_request',
        e.event_ts, toDateTime64(0, 3, 'UTC'))                            AS started_at,
    if(e.event_type = 'stream_trace'
        AND JSONExtractString(e.data, 'type') = 'gateway_ingest_stream_closed',
        1, 0)                                                              AS is_closed,
    if(e.event_type = 'stream_trace'
        AND JSONExtractString(e.data, 'type') = 'gateway_no_orchestrators_available',
        1, 0)                                                              AS has_failure,
    if(e.event_type = 'ai_stream_status',
        lower(ifNull(o.orch_address, '')),
        ''
    )                                                                      AS orch_address
FROM naap.events e
LEFT JOIN (SELECT uri, orch_address FROM naap.agg_orch_state FINAL) o
    ON o.uri = JSONExtractString(e.data, 'orchestrator_info', 'url')
WHERE e.event_type IN ('stream_trace', 'ai_stream_status')
  AND JSONExtractString(e.data, 'stream_id') != '';
