-- Migration 006: stream activity aggregate views (R2 — STR-001 through STR-003)
--
-- Source events:
--   stream_trace      — lifecycle events (start, close, no-orch, swap)
--   ai_stream_status  — pipeline, state, orch, per-stream status updates
--
-- Two aggregate tables:
--
-- 1. agg_stream_state (ReplacingMergeTree by stream_id, version=last_seen)
--    One row per stream per event. FINAL collapses to latest row per stream.
--    Used by STR-001 (active streams): query WHERE last_seen > now() - 30s AND is_closed = 0.
--
-- 2. agg_stream_hourly (SummingMergeTree)
--    Hourly counts of stream lifecycle events per org+pipeline bucket.
--    Used by STR-002 (summary) and STR-003 (history).
--    SummingMergeTree merges rows with matching ORDER BY keys by summing numeric columns.

-- ── agg_stream_state ─────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS naap.agg_stream_state
(
    stream_id    String,
    org          LowCardinality(String),
    -- pipeline: populated from ai_stream_status events; '' for stream_trace-only streams.
    pipeline     LowCardinality(String),
    -- state: ONLINE | LOADING | DEGRADED_INPUT | DEGRADED_INFERENCE | UNKNOWN
    state        LowCardinality(String),
    last_seen    DateTime64(3, 'UTC'),
    started_at   DateTime64(3, 'UTC'),
    -- 1 if gateway_ingest_stream_closed has been seen for this stream_id.
    is_closed    UInt8,
    -- 1 if gateway_no_orchestrators_available was the failure mode.
    has_failure  UInt8,
    orch_address String
)
ENGINE = ReplacingMergeTree(last_seen)
ORDER BY stream_id
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS naap.mv_stream_state
TO naap.agg_stream_state
AS
SELECT
    JSONExtractString(data, 'stream_id')                                  AS stream_id,
    org,
    if(event_type = 'ai_stream_status',
        JSONExtractString(data, 'pipeline'), '')                          AS pipeline,
    if(event_type = 'ai_stream_status',
        JSONExtractString(data, 'state'), 'UNKNOWN')                      AS state,
    event_ts                                                               AS last_seen,
    -- started_at is only meaningful for gateway_receive_stream_request rows;
    -- ReplacingMergeTree(last_seen) will keep the latest row, so started_at
    -- reflects the last event seen, not the true start. The Go API joins
    -- against the earliest event_ts for a given stream_id from naap.events
    -- when an accurate start time is required.
    if(event_type = 'stream_trace'
        AND JSONExtractString(data, 'type') = 'gateway_receive_stream_request',
        event_ts, toDateTime64(0, 3, 'UTC'))                              AS started_at,
    if(event_type = 'stream_trace'
        AND JSONExtractString(data, 'type') = 'gateway_ingest_stream_closed',
        1, 0)                                                              AS is_closed,
    if(event_type = 'stream_trace'
        AND JSONExtractString(data, 'type') = 'gateway_no_orchestrators_available',
        1, 0)                                                              AS has_failure,
    if(event_type = 'ai_stream_status',
        lower(JSONExtractString(data, 'orchestrator_info', 'address')), '') AS orch_address
FROM naap.events
WHERE event_type IN ('stream_trace', 'ai_stream_status')
  AND JSONExtractString(data, 'stream_id') != '';

-- ── agg_stream_hourly ────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS naap.agg_stream_hourly
(
    hour       DateTime,
    org        LowCardinality(String),
    -- pipeline is not available in stream_trace events; the Go API joins
    -- agg_stream_hourly with agg_stream_state to add pipeline breakdown.
    -- Kept '' here to avoid premature denormalisation.
    pipeline   LowCardinality(String),
    started    UInt64,    -- gateway_receive_stream_request count
    completed  UInt64,    -- gateway_ingest_stream_closed count
    no_orch    UInt64,    -- gateway_no_orchestrators_available count
    orch_swap  UInt64     -- orchestrator_swap count
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (org, pipeline, hour)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS naap.mv_stream_hourly
TO naap.agg_stream_hourly
AS
SELECT
    toStartOfHour(event_ts)                                               AS hour,
    org,
    ''                                                                     AS pipeline,
    countIf(JSONExtractString(data, 'type') = 'gateway_receive_stream_request')    AS started,
    countIf(JSONExtractString(data, 'type') = 'gateway_ingest_stream_closed')      AS completed,
    countIf(JSONExtractString(data, 'type') = 'gateway_no_orchestrators_available') AS no_orch,
    countIf(JSONExtractString(data, 'type') = 'orchestrator_swap')                 AS orch_swap
FROM naap.events
WHERE event_type = 'stream_trace'
GROUP BY hour, org, pipeline;
