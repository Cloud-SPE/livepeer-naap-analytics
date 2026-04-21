-- Phase 3 of serving-layer-v2: denormalize orchestrator_uri onto the two
-- serving stores whose handler callers previously had to LEFT JOIN
-- naap.api_orchestrator_identity on every request. Carrying the URI on
-- each row lets the resolver stamp it at write time from session attribution
-- or canonical_capability_orchestrator_identity_latest, and the API layer
-- reads the serving view column rather than rejoining identity per request.
--
-- Safe to run on fresh stacks (bootstrap has these columns too); safe to
-- re-run (IF NOT EXISTS guards the column adds).

ALTER TABLE naap.api_hourly_streaming_sla_store
    ADD COLUMN IF NOT EXISTS `orchestrator_uri` String DEFAULT '' AFTER `orchestrator_address`;

ALTER TABLE naap.canonical_active_stream_state_latest_store
    ADD COLUMN IF NOT EXISTS `orchestrator_uri` String DEFAULT '' AFTER `orch_address`;

-- canonical_active_stream_state_latest was materialized as a plain view by
-- migration 003. It is not one of the api/api_base models dbt rebuilds on
-- replay, so its column list has to be kept in lockstep with the store by
-- replaying the CREATE OR REPLACE VIEW here.
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
    `orchestrator_uri`      String,
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
    s.orchestrator_uri,
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
