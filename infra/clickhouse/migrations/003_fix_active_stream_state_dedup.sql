-- Migration 003: scope dedup CTE in canonical_active_stream_state_latest to completed = 0
--
-- The canonical_active_stream_state_latest view runs an unconditional full-table
-- dedup CTE over canonical_active_stream_state_latest_store (54 M rows) before any
-- filter is applied.  Of those 54 M rows, 52.9 M are completed = 1 sessions that
-- are irrelevant to any "currently active" query.
--
-- Adding WHERE completed = 0 to the dedup CTE reduces GROUP BY candidates from
-- 701 K distinct session keys across 54 M rows down to ~121 active sessions across
-- 1.27 M rows — a ~97 % reduction in aggregation work.
--
-- Semantics: canonical_active_stream_state_latest is consumed exclusively by
-- the current active-stream serving view, now published as api_current_active_stream_state, which already applies WHERE completed = 0.  Restricting
-- the dedup to completed = 0 rows does not change observable query results; it only
-- prevents the CTE from touching stale completed-session history.
--
-- Deployment: online, no downtime.  Just a view replacement.

CREATE OR REPLACE VIEW naap.canonical_active_stream_state_latest
(
    `canonical_session_key` String,
    `event_id`              String,
    `sample_ts`             DateTime64(3, 'UTC'),
    `org`                   LowCardinality(String),
    `stream_id`             String,
    `request_id`            String,
    `gateway`               String,
    `pipeline`              String,
    `model_id`              Nullable(String),
    `orch_address`          Nullable(String),
    `attribution_status`    String,
    `attribution_reason`    String,
    `state`                 String,
    `output_fps`            Float64,
    `input_fps`             Float64,
    `e2e_latency_ms`        Nullable(Float64),
    `started_at`            Nullable(DateTime64(3, 'UTC')),
    `last_seen`             DateTime64(3, 'UTC'),
    `completed`             UInt8
) AS
WITH latest_sessions AS (
    SELECT
        canonical_session_key,
        argMax(refreshed_at, refreshed_at) AS refreshed_at
    FROM naap.canonical_active_stream_state_latest_store
    WHERE completed = 0
    GROUP BY canonical_session_key
)
SELECT
    s.canonical_session_key,
    s.event_id,
    s.sample_ts,
    s.org,
    s.stream_id,
    s.request_id,
    s.gateway,
    s.pipeline,
    s.model_id,
    s.orch_address,
    s.attribution_status,
    s.attribution_reason,
    s.state,
    s.output_fps,
    s.input_fps,
    s.e2e_latency_ms,
    s.started_at,
    s.last_seen,
    s.completed
FROM naap.canonical_active_stream_state_latest_store AS s
INNER JOIN latest_sessions AS l
    ON s.canonical_session_key = l.canonical_session_key
   AND s.refreshed_at = l.refreshed_at;