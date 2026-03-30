-- Migration 025: session-evidence rollups for canonical refresh
--
-- These tables move canonical refresh off event-grain normalized_* scans.
-- They pre-aggregate session and hour evidence keyed by the access patterns
-- used by canonical-refresh and API read-model publication.

CREATE TABLE IF NOT EXISTS naap.normalized_session_trace_rollup_latest
(
    org                           LowCardinality(String),
    canonical_session_key         String,
    stream_id_state               AggregateFunction(argMaxIf, String, DateTime64(3, 'UTC'), UInt8),
    request_id_state              AggregateFunction(argMaxIf, String, DateTime64(3, 'UTC'), UInt8),
    pipeline_id_state             AggregateFunction(argMaxIf, String, DateTime64(3, 'UTC'), UInt8),
    raw_pipeline_hint_state       AggregateFunction(argMaxIf, String, DateTime64(3, 'UTC'), UInt8),
    started_at_state              AggregateFunction(minIf, DateTime64(3, 'UTC'), UInt8),
    started_count_state           AggregateFunction(sum, UInt64),
    playable_seen_count_state     AggregateFunction(sum, UInt64),
    no_orch_count_state           AggregateFunction(sum, UInt64),
    completed_count_state         AggregateFunction(sum, UInt64),
    swap_count_state              AggregateFunction(sum, UInt64),
    trace_last_seen_state         AggregateFunction(max, DateTime64(3, 'UTC'))
)
ENGINE = AggregatingMergeTree
PARTITION BY org
ORDER BY (org, canonical_session_key)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS naap.mv_normalized_session_trace_rollup_latest
TO naap.normalized_session_trace_rollup_latest
AS
SELECT
    org,
    canonical_session_key,
    argMaxIfState(stream_id, event_ts, toUInt8(stream_id != '')) AS stream_id_state,
    argMaxIfState(request_id, event_ts, toUInt8(request_id != '')) AS request_id_state,
    argMaxIfState(pipeline_id, event_ts, toUInt8(pipeline_id != '')) AS pipeline_id_state,
    argMaxIfState(raw_pipeline_hint, event_ts, toUInt8(raw_pipeline_hint != '')) AS raw_pipeline_hint_state,
    minIfState(event_ts, toUInt8(trace_type = 'gateway_receive_stream_request')) AS started_at_state,
    sumState(toUInt64(trace_type = 'gateway_receive_stream_request')) AS started_count_state,
    sumState(toUInt64(trace_type = 'gateway_receive_few_processed_segments')) AS playable_seen_count_state,
    sumState(toUInt64(trace_type = 'gateway_no_orchestrators_available')) AS no_orch_count_state,
    sumState(toUInt64(trace_type = 'gateway_ingest_stream_closed')) AS completed_count_state,
    sumState(toUInt64(trace_type = 'orchestrator_swap')) AS swap_count_state,
    maxState(event_ts) AS trace_last_seen_state
FROM naap.normalized_stream_trace
WHERE canonical_session_key != ''
GROUP BY org, canonical_session_key;

CREATE TABLE IF NOT EXISTS naap.normalized_session_status_rollup_latest
(
    org                                   LowCardinality(String),
    canonical_session_key                 String,
    stream_id_state                       AggregateFunction(argMaxIf, String, DateTime64(3, 'UTC'), UInt8),
    request_id_state                      AggregateFunction(argMaxIf, String, DateTime64(3, 'UTC'), UInt8),
    canonical_pipeline_state              AggregateFunction(argMaxIf, String, DateTime64(3, 'UTC'), UInt8),
    restart_seen_count_state              AggregateFunction(sum, UInt64),
    error_seen_count_state                AggregateFunction(sum, UInt64),
    degraded_input_seen_count_state       AggregateFunction(sum, UInt64),
    degraded_inference_seen_count_state   AggregateFunction(sum, UInt64),
    status_sample_count_state             AggregateFunction(sum, UInt64),
    status_error_sample_count_state       AggregateFunction(sum, UInt64),
    online_seen_count_state               AggregateFunction(sum, UInt64),
    positive_output_seen_count_state      AggregateFunction(sum, UInt64),
    running_state_samples_count_state     AggregateFunction(sum, UInt64),
    status_last_seen_state                AggregateFunction(max, DateTime64(3, 'UTC'))
)
ENGINE = AggregatingMergeTree
PARTITION BY org
ORDER BY (org, canonical_session_key)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS naap.mv_normalized_session_status_rollup_latest
TO naap.normalized_session_status_rollup_latest
AS
SELECT
    org,
    canonical_session_key,
    argMaxIfState(stream_id, event_ts, toUInt8(stream_id != '')) AS stream_id_state,
    argMaxIfState(request_id, event_ts, toUInt8(request_id != '')) AS request_id_state,
    argMaxIfState(raw_pipeline_hint, event_ts, toUInt8(raw_pipeline_hint != '')) AS canonical_pipeline_state,
    sumState(toUInt64(restart_count > 0)) AS restart_seen_count_state,
    sumState(toUInt64(last_error NOT IN ('', 'null'))) AS error_seen_count_state,
    sumState(toUInt64(state = 'DEGRADED_INPUT')) AS degraded_input_seen_count_state,
    sumState(toUInt64(state = 'DEGRADED_INFERENCE')) AS degraded_inference_seen_count_state,
    sumState(toUInt64(1)) AS status_sample_count_state,
    sumState(toUInt64(last_error NOT IN ('', 'null'))) AS status_error_sample_count_state,
    sumState(toUInt64(state = 'ONLINE')) AS online_seen_count_state,
    sumState(toUInt64(output_fps > 0)) AS positive_output_seen_count_state,
    sumState(toUInt64(state IN ('ONLINE', 'DEGRADED_INPUT', 'DEGRADED_INFERENCE'))) AS running_state_samples_count_state,
    maxState(event_ts) AS status_last_seen_state
FROM naap.normalized_ai_stream_status
WHERE canonical_session_key != ''
GROUP BY org, canonical_session_key;

CREATE TABLE IF NOT EXISTS naap.normalized_session_event_rollup_latest
(
    org                           LowCardinality(String),
    canonical_session_key         String,
    event_pipeline_hint_state     AggregateFunction(argMaxIf, String, DateTime64(3, 'UTC'), UInt8),
    any_event_message_count_state AggregateFunction(sum, UInt64),
    event_last_seen_state         AggregateFunction(max, DateTime64(3, 'UTC'))
)
ENGINE = AggregatingMergeTree
PARTITION BY org
ORDER BY (org, canonical_session_key)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS naap.mv_normalized_session_event_rollup_latest
TO naap.normalized_session_event_rollup_latest
AS
SELECT
    org,
    canonical_session_key,
    argMaxIfState(raw_pipeline_hint, event_ts, toUInt8(raw_pipeline_hint != '')) AS event_pipeline_hint_state,
    sumState(toUInt64(message != '')) AS any_event_message_count_state,
    maxState(event_ts) AS event_last_seen_state
FROM naap.normalized_ai_stream_events
WHERE canonical_session_key != ''
GROUP BY org, canonical_session_key;

CREATE TABLE IF NOT EXISTS naap.normalized_session_orchestrator_observation_rollup_latest
(
    org                           LowCardinality(String),
    canonical_session_key         String,
    observed_orch_address_state   AggregateFunction(argMaxIf, String, DateTime64(3, 'UTC'), UInt8),
    observed_orch_url_state       AggregateFunction(argMaxIf, String, DateTime64(3, 'UTC'), UInt8),
    observed_orch_address_uniq    AggregateFunction(uniqExactIf, String, UInt8),
    observed_orch_url_uniq        AggregateFunction(uniqExactIf, String, UInt8),
    observation_last_seen_state   AggregateFunction(max, DateTime64(3, 'UTC'))
)
ENGINE = AggregatingMergeTree
PARTITION BY org
ORDER BY (org, canonical_session_key)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS naap.mv_normalized_session_orch_observation_from_trace
TO naap.normalized_session_orchestrator_observation_rollup_latest
AS
SELECT
    org,
    canonical_session_key,
    argMaxIfState(orch_raw_address, event_ts, toUInt8(orch_raw_address != '')) AS observed_orch_address_state,
    argMaxIfState(orch_url, event_ts, toUInt8(orch_url != '')) AS observed_orch_url_state,
    uniqExactIfState(orch_raw_address, toUInt8(orch_raw_address != '')) AS observed_orch_address_uniq,
    uniqExactIfState(lower(orch_url), toUInt8(orch_url != '')) AS observed_orch_url_uniq,
    maxState(event_ts) AS observation_last_seen_state
FROM naap.normalized_stream_trace
WHERE canonical_session_key != ''
  AND (orch_raw_address != '' OR orch_url != '')
GROUP BY org, canonical_session_key;

CREATE MATERIALIZED VIEW IF NOT EXISTS naap.mv_normalized_session_orch_observation_from_status
TO naap.normalized_session_orchestrator_observation_rollup_latest
AS
SELECT
    org,
    canonical_session_key,
    argMaxIfState(orch_raw_address, event_ts, toUInt8(orch_raw_address != '')) AS observed_orch_address_state,
    argMaxIfState(orch_url, event_ts, toUInt8(orch_url != '')) AS observed_orch_url_state,
    uniqExactIfState(orch_raw_address, toUInt8(orch_raw_address != '')) AS observed_orch_address_uniq,
    uniqExactIfState(lower(orch_url), toUInt8(orch_url != '')) AS observed_orch_url_uniq,
    maxState(event_ts) AS observation_last_seen_state
FROM naap.normalized_ai_stream_status
WHERE canonical_session_key != ''
  AND (orch_raw_address != '' OR orch_url != '')
GROUP BY org, canonical_session_key;

CREATE MATERIALIZED VIEW IF NOT EXISTS naap.mv_normalized_session_orch_observation_from_events
TO naap.normalized_session_orchestrator_observation_rollup_latest
AS
SELECT
    org,
    canonical_session_key,
    argMaxIfState(orch_raw_address, event_ts, toUInt8(orch_raw_address != '')) AS observed_orch_address_state,
    argMaxIfState(orch_url, event_ts, toUInt8(orch_url != '')) AS observed_orch_url_state,
    uniqExactIfState(orch_raw_address, toUInt8(orch_raw_address != '')) AS observed_orch_address_uniq,
    uniqExactIfState(lower(orch_url), toUInt8(orch_url != '')) AS observed_orch_url_uniq,
    maxState(event_ts) AS observation_last_seen_state
FROM naap.normalized_ai_stream_events
WHERE canonical_session_key != ''
  AND (orch_raw_address != '' OR orch_url != '')
GROUP BY org, canonical_session_key;

CREATE TABLE IF NOT EXISTS naap.normalized_session_status_hour_rollup
(
    org                               LowCardinality(String),
    canonical_session_key             String,
    hour                              DateTime('UTC'),
    stream_id_state                   AggregateFunction(argMaxIf, String, DateTime64(3, 'UTC'), UInt8),
    request_id_state                  AggregateFunction(argMaxIf, String, DateTime64(3, 'UTC'), UInt8),
    status_samples_state              AggregateFunction(sum, UInt64),
    fps_positive_samples_state        AggregateFunction(sum, UInt64),
    running_state_samples_state       AggregateFunction(sum, UInt64),
    degraded_input_samples_state      AggregateFunction(sum, UInt64),
    degraded_inference_samples_state  AggregateFunction(sum, UInt64),
    error_samples_state               AggregateFunction(sum, UInt64),
    output_fps_sum_state              AggregateFunction(sum, Float64),
    input_fps_sum_state               AggregateFunction(sum, Float64),
    e2e_latency_sum_state             AggregateFunction(sum, Float64),
    e2e_latency_count_state           AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
PARTITION BY (org, toYYYYMM(hour))
ORDER BY (org, canonical_session_key, hour)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS naap.mv_normalized_session_status_hour_rollup
TO naap.normalized_session_status_hour_rollup
AS
SELECT
    org,
    canonical_session_key,
    toStartOfHour(event_ts) AS hour,
    argMaxIfState(stream_id, event_ts, toUInt8(stream_id != '')) AS stream_id_state,
    argMaxIfState(request_id, event_ts, toUInt8(request_id != '')) AS request_id_state,
    sumState(toUInt64(1)) AS status_samples_state,
    sumState(toUInt64(output_fps > 0)) AS fps_positive_samples_state,
    sumState(toUInt64(state IN ('ONLINE', 'DEGRADED_INPUT', 'DEGRADED_INFERENCE'))) AS running_state_samples_state,
    sumState(toUInt64(state = 'DEGRADED_INPUT')) AS degraded_input_samples_state,
    sumState(toUInt64(state = 'DEGRADED_INFERENCE')) AS degraded_inference_samples_state,
    sumState(toUInt64(last_error NOT IN ('', 'null'))) AS error_samples_state,
    sumState(output_fps) AS output_fps_sum_state,
    sumState(input_fps) AS input_fps_sum_state,
    sumState(if(e2e_latency_ms > 0, e2e_latency_ms, 0.0)) AS e2e_latency_sum_state,
    sumState(toUInt64(e2e_latency_ms > 0)) AS e2e_latency_count_state
FROM naap.normalized_ai_stream_status
WHERE canonical_session_key != ''
GROUP BY org, canonical_session_key, hour;

CREATE TABLE IF NOT EXISTS naap.canonical_capability_snapshot_latest
(
    org                 LowCardinality(String),
    orch_address        String,
    orch_uri_norm       String,
    snapshot_row_id     AggregateFunction(argMaxIf, String, DateTime64(3, 'UTC'), UInt8),
    source_event_id     AggregateFunction(argMaxIf, String, DateTime64(3, 'UTC'), UInt8),
    snapshot_ts         AggregateFunction(max, DateTime64(3, 'UTC')),
    orch_name           AggregateFunction(argMaxIf, String, DateTime64(3, 'UTC'), UInt8),
    orch_uri            AggregateFunction(argMaxIf, String, DateTime64(3, 'UTC'), UInt8),
    version             AggregateFunction(argMaxIf, String, DateTime64(3, 'UTC'), UInt8),
    raw_capabilities    AggregateFunction(argMaxIf, String, DateTime64(3, 'UTC'), UInt8)
)
ENGINE = AggregatingMergeTree
PARTITION BY org
ORDER BY (org, orch_address, orch_uri_norm)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS naap.mv_canonical_capability_snapshot_latest
TO naap.canonical_capability_snapshot_latest
AS
SELECT
    org,
    orch_address,
    orch_uri_norm,
    argMaxIfState(row_id, event_ts, toUInt8(row_id != '')) AS snapshot_row_id,
    argMaxIfState(event_id, event_ts, toUInt8(event_id != '')) AS source_event_id,
    maxState(event_ts) AS snapshot_ts,
    argMaxIfState(orch_name, event_ts, toUInt8(orch_name != '')) AS orch_name,
    argMaxIfState(orch_uri, event_ts, toUInt8(orch_uri != '')) AS orch_uri,
    argMaxIfState(version, event_ts, toUInt8(version != '')) AS version,
    argMaxIfState(raw_capabilities, event_ts, toUInt8(raw_capabilities != '')) AS raw_capabilities
FROM naap.normalized_network_capabilities
WHERE orch_address != ''
GROUP BY org, orch_address, orch_uri_norm;

CREATE TABLE IF NOT EXISTS naap.canonical_capability_hardware_inventory
(
    snapshot_row_id            String,
    snapshot_ts                DateTime64(3, 'UTC'),
    org                        LowCardinality(String),
    orch_address               String,
    orch_uri_norm              String,
    pipeline_id                String,
    model_id                   String,
    gpu_id                     String,
    gpu_model_name             Nullable(String),
    gpu_memory_bytes_total     Nullable(UInt64),
    runner_version             Nullable(String),
    cuda_version               Nullable(String)
)
ENGINE = ReplacingMergeTree(snapshot_ts)
PARTITION BY (org, toYYYYMM(snapshot_ts))
ORDER BY (org, orch_address, pipeline_id, model_id, gpu_id, snapshot_row_id)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS naap.mv_canonical_capability_hardware_inventory
TO naap.canonical_capability_hardware_inventory
AS
SELECT
    row_id AS snapshot_row_id,
    event_ts AS snapshot_ts,
    org,
    orch_address,
    orch_uri_norm,
    JSONExtractString(hardware_json, 'pipeline') AS pipeline_id,
    JSONExtractString(hardware_json, 'model_id') AS model_id,
    ifNull(nullIf(JSONExtractString(gpu_info_raw, '0', 'id'), ''), '') AS gpu_id,
    nullIf(JSONExtractString(gpu_info_raw, '0', 'name'), '') AS gpu_model_name,
    nullIf(JSONExtractUInt(gpu_info_raw, '0', 'memory_total'), 0) AS gpu_memory_bytes_total,
    cast(null as Nullable(String)) AS runner_version,
    cast(null as Nullable(String)) AS cuda_version
FROM (
    SELECT
        row_id,
        event_ts,
        org,
        orch_address,
        orch_uri_norm,
        hardware_json,
        JSONExtractRaw(hardware_json, 'gpu_info') AS gpu_info_raw
    FROM (
        SELECT
            row_id,
            event_ts,
            org,
            orch_address,
            orch_uri_norm,
            arrayJoin(JSONExtractArrayRaw(raw_capabilities, 'hardware')) AS hardware_json
        FROM naap.normalized_network_capabilities
        WHERE length(JSONExtractArrayRaw(raw_capabilities, 'hardware')) > 0
    )
)
WHERE pipeline_id != '';
