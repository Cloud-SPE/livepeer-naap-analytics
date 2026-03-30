-- Rebuild session-evidence rollups from existing normalized_* tables.
--
-- This is intentionally destructive for the rollup tables: run it when you
-- want the rollup layer to reflect the current normalized_* history without
-- replaying Kafka from scratch.

TRUNCATE TABLE naap.normalized_session_trace_rollup_latest;
TRUNCATE TABLE naap.normalized_session_status_rollup_latest;
TRUNCATE TABLE naap.normalized_session_event_rollup_latest;
TRUNCATE TABLE naap.normalized_session_orchestrator_observation_rollup_latest;
TRUNCATE TABLE naap.normalized_session_status_hour_rollup;
TRUNCATE TABLE naap.canonical_capability_snapshot_latest;
TRUNCATE TABLE naap.canonical_capability_hardware_inventory;
TRUNCATE TABLE naap.canonical_capability_hardware_inventory_by_snapshot;
TRUNCATE TABLE naap.canonical_capability_snapshots_by_address;
TRUNCATE TABLE naap.canonical_capability_snapshots_by_uri;
TRUNCATE TABLE naap.canonical_latest_orchestrator_pipeline_inventory_agg;

INSERT INTO naap.normalized_session_trace_rollup_latest
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

INSERT INTO naap.normalized_session_status_rollup_latest
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

INSERT INTO naap.normalized_session_event_rollup_latest
SELECT
    org,
    canonical_session_key,
    argMaxIfState(raw_pipeline_hint, event_ts, toUInt8(raw_pipeline_hint != '')) AS event_pipeline_hint_state,
    sumState(toUInt64(message != '')) AS any_event_message_count_state,
    maxState(event_ts) AS event_last_seen_state
FROM naap.normalized_ai_stream_events
WHERE canonical_session_key != ''
GROUP BY org, canonical_session_key;

INSERT INTO naap.normalized_session_orchestrator_observation_rollup_latest
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

INSERT INTO naap.normalized_session_orchestrator_observation_rollup_latest
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

INSERT INTO naap.normalized_session_orchestrator_observation_rollup_latest
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

INSERT INTO naap.normalized_session_status_hour_rollup
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

INSERT INTO naap.canonical_capability_snapshot_latest
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

INSERT INTO naap.canonical_capability_hardware_inventory
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

INSERT INTO naap.canonical_capability_hardware_inventory_by_snapshot
SELECT
    snapshot_row_id,
    snapshot_ts,
    org,
    orch_address,
    orch_uri_norm,
    pipeline_id,
    model_id,
    gpu_id,
    gpu_model_name,
    gpu_memory_bytes_total,
    runner_version,
    cuda_version
FROM naap.canonical_capability_hardware_inventory;

INSERT INTO naap.canonical_capability_snapshots_by_address
SELECT
    row_id AS snapshot_row_id,
    event_ts AS snapshot_ts,
    org,
    orch_address,
    orch_uri,
    orch_uri_norm
FROM naap.normalized_network_capabilities
WHERE orch_address != '';

INSERT INTO naap.canonical_capability_snapshots_by_uri
SELECT
    row_id AS snapshot_row_id,
    event_ts AS snapshot_ts,
    org,
    orch_address,
    orch_uri,
    orch_uri_norm
FROM naap.normalized_network_capabilities
WHERE orch_uri_norm != '';

INSERT INTO naap.canonical_latest_orchestrator_pipeline_inventory_agg
SELECT
    org,
    orch_address,
    pipeline_id,
    model_id,
    argMaxIfState(gpu_id, snapshot_ts, toUInt8(gpu_id != '')) AS gpu_id_state,
    argMaxIfState(ifNull(gpu_model_name, ''), snapshot_ts, toUInt8(ifNull(gpu_model_name, '') != '')) AS gpu_model_name_state,
    argMaxIfState(ifNull(gpu_memory_bytes_total, toUInt64(0)), snapshot_ts, toUInt8(ifNull(gpu_memory_bytes_total, toUInt64(0)) > 0)) AS gpu_memory_bytes_total_state,
    argMaxIfState(ifNull(runner_version, ''), snapshot_ts, toUInt8(ifNull(runner_version, '') != '')) AS runner_version_state,
    argMaxIfState(ifNull(cuda_version, ''), snapshot_ts, toUInt8(ifNull(cuda_version, '') != '')) AS cuda_version_state,
    maxState(snapshot_ts) AS last_seen_state
FROM naap.canonical_capability_hardware_inventory
WHERE pipeline_id != ''
GROUP BY org, orch_address, pipeline_id, model_id;
