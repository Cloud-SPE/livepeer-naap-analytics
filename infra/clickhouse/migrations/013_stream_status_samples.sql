-- Migration 013: per-sample stream status table + materialized view (dashboard support)
--
-- Provides sub-hourly stream status data for Grafana live-operations and overview panels.
-- Populated by ai_stream_status events with canonical ETH orch address resolution
-- (same URI→agg_orch_state JOIN pattern as migration 011).
--
-- TTL: 30 days (short-lived operational data; hourly aggregates in agg_fps_hourly
-- and agg_webrtc_hourly cover longer retention needs).

CREATE TABLE IF NOT EXISTS naap.agg_stream_status_samples
(
    sample_ts       DateTime64(3, 'UTC'),
    org             LowCardinality(String),
    stream_id       String,
    gateway         String,
    -- orch_address: canonical ETH address resolved from orchestrator_info.url
    -- via agg_orch_state.uri JOIN. Falls back to raw address if not resolvable.
    orch_address    String,
    pipeline        LowCardinality(String),
    -- state: ONLINE | LOADING | DEGRADED_INPUT | DEGRADED_INFERENCE | UNKNOWN
    state           LowCardinality(String),
    output_fps      Float64,   -- inference_status.fps
    input_fps       Float64,   -- input_status.fps
    e2e_latency_ms  Float64,   -- inference_status.latency_ms (0 if absent)
    -- is_attributed: 1 if orch_address was successfully resolved via URI JOIN
    is_attributed   UInt8
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(sample_ts)
ORDER BY (org, stream_id, sample_ts)
TTL toDateTime(sample_ts) + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS naap.mv_stream_status_samples
TO naap.agg_stream_status_samples
AS
SELECT
    event_ts                                                             AS sample_ts,
    org,
    JSONExtractString(data, 'stream_id')                                AS stream_id,
    gateway,
    -- Resolve local keypair address to canonical ETH address via URI.
    -- Same approach as migration 011 (fix_orch_address) for stream state
    -- and reliability tables. If the URI is not in agg_orch_state, fall
    -- back to the raw address from the event payload.
    ifNull(
        o.orch_address,
        lower(JSONExtractString(data, 'orchestrator_info', 'address'))
    )                                                                    AS orch_address,
    JSONExtractString(data, 'pipeline')                                  AS pipeline,
    multiIf(
        JSONExtractString(data, 'inference_status', 'state') = 'Running',  'ONLINE',
        JSONExtractString(data, 'inference_status', 'state') = 'Idle',     'LOADING',
        JSONExtractString(data, 'input_status',     'state') = 'Degraded', 'DEGRADED_INPUT',
        JSONExtractString(data, 'inference_status', 'state') = 'Degraded', 'DEGRADED_INFERENCE',
        'UNKNOWN'
    )                                                                    AS state,
    JSONExtractFloat(data, 'inference_status', 'fps')                   AS output_fps,
    JSONExtractFloat(data, 'input_status',     'fps')                   AS input_fps,
    JSONExtractFloat(data, 'inference_status', 'latency_ms')            AS e2e_latency_ms,
    toUInt8(ifNull(o.orch_address, '') != '')                           AS is_attributed
FROM naap.events
LEFT JOIN (SELECT uri, orch_address FROM naap.agg_orch_state FINAL) o
    ON o.uri = JSONExtractString(data, 'orchestrator_info', 'url')
WHERE event_type = 'ai_stream_status'
  AND JSONExtractString(data, 'stream_id') != '';
