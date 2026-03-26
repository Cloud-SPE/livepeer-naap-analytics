-- Migration 008: reliability aggregate views (R5 — REL-001 through REL-004)
--
-- Source events:
--   stream_trace      — no_orch_available, orch_swap signals
--   ai_stream_status  — degraded state, restart_count, last_error
--
-- REL-001/REL-002 network-level rates are computed at query time from
-- agg_stream_hourly (migration 006) — no additional table needed.
--
-- This migration adds agg_orch_reliability_hourly for per-orchestrator
-- breakdown (REL-003).
--
-- Failure type mapping (from spec R5):
--   degraded_count  → ai_stream_status.state IN (DEGRADED_INFERENCE, DEGRADED_INPUT)
--   restart_count   → ai_stream_status.inference_status.restart_count > 0
--   error_count     → ai_stream_status.inference_status.last_error not empty/null

CREATE TABLE IF NOT EXISTS naap.agg_orch_reliability_hourly
(
    hour             DateTime,
    org              LowCardinality(String),
    orch_address     String,
    -- Proxy for streams handled: count of ai_stream_status events.
    -- The Go API filters to orchs with >= 10 events (REL-003-a).
    ai_stream_count  UInt64,
    degraded_count   UInt64,
    restart_count    UInt64,
    error_count      UInt64
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (org, hour, orch_address)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS naap.mv_orch_reliability_hourly
TO naap.agg_orch_reliability_hourly
AS
SELECT
    toStartOfHour(event_ts)                                               AS hour,
    org,
    lower(JSONExtractString(data, 'orchestrator_info', 'address'))        AS orch_address,
    1                                                                      AS ai_stream_count,
    -- DEGRADED_INFERENCE or DEGRADED_INPUT state
    toUInt64(JSONExtractString(data, 'state') IN ('DEGRADED_INFERENCE', 'DEGRADED_INPUT'))
                                                                           AS degraded_count,
    -- restart_count > 0 in inference_status
    toUInt64(JSONExtractUInt(data, 'inference_status', 'restart_count') > 0)
                                                                           AS restart_count,
    -- last_error is non-empty string (not null, not '')
    toUInt64(JSONExtractString(data, 'inference_status', 'last_error') NOT IN ('', 'null'))
                                                                           AS error_count
FROM naap.events
WHERE event_type = 'ai_stream_status'
  AND JSONExtractString(data, 'orchestrator_info', 'address') != '';
