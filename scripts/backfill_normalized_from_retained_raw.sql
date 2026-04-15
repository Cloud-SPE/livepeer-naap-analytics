-- Rebuild raw-derived normalized and aggregate tables from retained
-- accepted_raw_events history.
--
-- This script is intentionally destructive for the target tables. Run it when
-- you want ClickHouse to repopulate the supported downstream spine from
-- retained raw history without replaying Kafka.

TRUNCATE TABLE IF EXISTS naap.agg_discovery_latency_hourly;
TRUNCATE TABLE IF EXISTS naap.agg_fps_hourly;
TRUNCATE TABLE IF EXISTS naap.agg_orch_reliability_hourly;
TRUNCATE TABLE IF EXISTS naap.agg_orch_state;
TRUNCATE TABLE IF EXISTS naap.agg_payment_hourly;
TRUNCATE TABLE IF EXISTS naap.agg_stream_hourly;
TRUNCATE TABLE IF EXISTS naap.agg_stream_state;
TRUNCATE TABLE IF EXISTS naap.agg_stream_status_samples;
TRUNCATE TABLE IF EXISTS naap.agg_webrtc_hourly;
TRUNCATE TABLE IF EXISTS naap.typed_stream_ingest_metrics;

TRUNCATE TABLE IF EXISTS naap.normalized_ai_batch_job;
TRUNCATE TABLE IF EXISTS naap.normalized_ai_llm_request;
TRUNCATE TABLE IF EXISTS naap.normalized_ai_stream_events;
TRUNCATE TABLE IF EXISTS naap.normalized_ai_stream_status;
TRUNCATE TABLE IF EXISTS naap.normalized_byoc_auth;
TRUNCATE TABLE IF EXISTS naap.normalized_byoc_job;
TRUNCATE TABLE IF EXISTS naap.normalized_byoc_payment;
TRUNCATE TABLE IF EXISTS naap.normalized_network_capabilities;
TRUNCATE TABLE IF EXISTS naap.normalized_session_attribution_input_latest_store;
TRUNCATE TABLE IF EXISTS naap.normalized_session_event_rollup_latest;
TRUNCATE TABLE IF EXISTS naap.normalized_session_orchestrator_observation_rollup_latest;
TRUNCATE TABLE IF EXISTS naap.normalized_session_status_hour_rollup;
TRUNCATE TABLE IF EXISTS naap.normalized_session_status_rollup_latest;
TRUNCATE TABLE IF EXISTS naap.normalized_session_trace_rollup_latest;
TRUNCATE TABLE IF EXISTS naap.normalized_stream_trace;
TRUNCATE TABLE IF EXISTS naap.normalized_worker_lifecycle;

INSERT INTO naap.normalized_ai_stream_events
SELECT
    event_id,
    event_ts,
    org,
    gateway,
    JSONExtractString(data, 'stream_id') AS stream_id,
    JSONExtractString(data, 'request_id') AS request_id,
    multiIf(
        org = '', '',
        JSONExtractString(data, 'stream_id') != '' AND JSONExtractString(data, 'request_id') != '',
            concat(org, '|', JSONExtractString(data, 'stream_id'), '|', JSONExtractString(data, 'request_id')),
        JSONExtractString(data, 'stream_id') != '',
            concat(org, '|', JSONExtractString(data, 'stream_id'), '|_missing_request'),
        JSONExtractString(data, 'request_id') != '',
            concat(org, '|_missing_stream|', JSONExtractString(data, 'request_id')),
        ''
    ) AS canonical_session_key,
    JSONExtractString(data, 'pipeline') AS raw_pipeline_hint,
    event_subtype AS event_name,
    lower(JSONExtractString(data, 'orchestrator_info', 'address')) AS orch_raw_address,
    JSONExtractString(data, 'orchestrator_info', 'url') AS orch_url,
    lower(JSONExtractString(data, 'orchestrator_info', 'url')) AS orch_url_norm,
    JSONExtractString(data, 'message') AS message,
    data
FROM naap.accepted_raw_events
WHERE event_type = 'ai_stream_events';

INSERT INTO naap.normalized_ai_stream_status
WITH
    JSONExtractString(data, 'stream_id') AS derived_stream_id,
    JSONExtractString(data, 'request_id') AS derived_request_id,
    JSONExtractString(data, 'pipeline') AS derived_pipeline,
    lower(JSONExtractString(data, 'orchestrator_info', 'address')) AS derived_orch_address,
    JSONExtractString(data, 'orchestrator_info', 'url') AS derived_orch_url,
    JSONExtractFloat(data, 'inference_status', 'fps') AS derived_output_fps,
    JSONExtractFloat(data, 'input_status', 'fps') AS derived_input_fps,
    JSONExtractFloat(data, 'inference_status', 'latency_ms') AS derived_e2e_latency_ms,
    JSONExtractString(data, 'state') AS derived_state
SELECT
    event_id,
    event_ts,
    org,
    gateway,
    derived_stream_id AS stream_id,
    derived_request_id AS request_id,
    multiIf(
        org = '', '',
        derived_stream_id != '' AND derived_request_id != '',
            concat(org, '|', derived_stream_id, '|', derived_request_id),
        derived_stream_id != '',
            concat(org, '|', derived_stream_id, '|_missing_request'),
        derived_request_id != '',
            concat(org, '|_missing_stream|', derived_request_id),
        ''
    ) AS canonical_session_key,
    derived_pipeline AS raw_pipeline_hint,
    derived_state AS state,
    derived_orch_address AS orch_raw_address,
    derived_orch_url AS orch_url,
    lower(derived_orch_url) AS orch_url_norm,
    derived_output_fps AS output_fps,
    derived_input_fps AS input_fps,
    derived_e2e_latency_ms AS e2e_latency_ms,
    parseDateTime64BestEffortOrNull(JSONExtractString(data, 'start_time')) AS start_time,
    JSONExtractUInt(data, 'inference_status', 'restart_count') AS restart_count,
    JSONExtractString(data, 'inference_status', 'last_error') AS last_error,
    coalesce(
        parseDateTime64BestEffortOrNull(JSONExtractString(data, 'inference_status', 'last_error_time')),
        toDateTime64(0, 3, 'UTC')
    ) AS last_error_ts,
    data
FROM naap.accepted_raw_events
WHERE event_type = 'ai_stream_status';

INSERT INTO naap.normalized_network_capabilities
SELECT
    concat(event_id, '#', lower(JSONExtractString(orch_json, 'address'))) AS row_id,
    event_id,
    event_ts,
    org,
    lower(JSONExtractString(orch_json, 'address')) AS orch_address,
    if(
        JSONExtractString(orch_json, 'local_address') != '',
        JSONExtractString(orch_json, 'local_address'),
        JSONExtractString(orch_json, 'address')
    ) AS orch_name,
    coalesce(nullIf(JSONExtractString(orch_json, 'orch_uri'), ''), JSONExtractString(orch_json, 'uri')) AS orch_uri,
    lower(coalesce(nullIf(JSONExtractString(orch_json, 'orch_uri'), ''), JSONExtractString(orch_json, 'uri'))) AS orch_uri_norm,
    JSONExtractString(orch_json, 'version') AS version,
    orch_json AS raw_capabilities
FROM (
    SELECT
        event_id,
        event_ts,
        org,
        arrayJoin(JSONExtractArrayRaw(data)) AS orch_json
    FROM naap.accepted_raw_events
    WHERE event_type = 'network_capabilities'
      AND data NOT IN ('', '[]', 'null')
)
WHERE JSONExtractString(orch_json, 'address') != '';

INSERT INTO naap.normalized_stream_trace
SELECT
    event_id,
    event_ts,
    org,
    gateway,
    JSONExtractString(data, 'stream_id') AS stream_id,
    JSONExtractString(data, 'request_id') AS request_id,
    multiIf(
        org = '', '',
        JSONExtractString(data, 'stream_id') != '' AND JSONExtractString(data, 'request_id') != '',
            concat(org, '|', JSONExtractString(data, 'stream_id'), '|', JSONExtractString(data, 'request_id')),
        JSONExtractString(data, 'stream_id') != '',
            concat(org, '|', JSONExtractString(data, 'stream_id'), '|_missing_request'),
        JSONExtractString(data, 'request_id') != '',
            concat(org, '|_missing_stream|', JSONExtractString(data, 'request_id')),
        ''
    ) AS canonical_session_key,
    event_subtype AS trace_type,
    JSONExtractString(data, 'pipeline') AS raw_pipeline_hint,
    JSONExtractString(data, 'pipeline_id') AS pipeline_id,
    lower(JSONExtractString(data, 'orchestrator_info', 'address')) AS orch_raw_address,
    JSONExtractString(data, 'orchestrator_info', 'url') AS orch_url,
    lower(JSONExtractString(data, 'orchestrator_info', 'url')) AS orch_url_norm,
    JSONExtractString(data, 'message') AS message,
    data
FROM naap.accepted_raw_events
WHERE event_type = 'stream_trace';

INSERT INTO naap.normalized_ai_batch_job
SELECT
    event_id,
    event_ts,
    org,
    gateway,
    JSONExtractString(data, 'request_id') AS request_id,
    JSONExtractString(data, 'pipeline') AS pipeline,
    JSONExtractString(data, 'model_id') AS model_id,
    event_subtype AS subtype,
    if(
        event_subtype = 'ai_batch_request_completed',
        toUInt8(JSONExtractBool(data, 'success')),
        CAST(NULL, 'Nullable(UInt8)')
    ) AS success,
    toUInt16(JSONExtractUInt(data, 'tries')) AS tries,
    JSONExtractInt(data, 'duration_ms') AS duration_ms,
    JSONExtractString(data, 'orch_url') AS orch_url,
    lower(JSONExtractString(data, 'orch_url')) AS orch_url_norm,
    JSONExtractFloat(data, 'latency_score') AS latency_score,
    JSONExtractFloat(data, 'price_per_unit') AS price_per_unit,
    JSONExtractString(data, 'error_type') AS error_type,
    JSONExtractString(data, 'error') AS error
FROM naap.accepted_raw_events
WHERE event_type = 'ai_batch_request';

INSERT INTO naap.normalized_ai_llm_request
SELECT
    event_id,
    event_ts,
    org,
    gateway,
    JSONExtractString(data, 'request_id') AS request_id,
    JSONExtractString(data, 'model') AS model,
    JSONExtractString(data, 'orch_url') AS orch_url,
    lower(JSONExtractString(data, 'orch_url')) AS orch_url_norm,
    event_subtype AS subtype,
    toUInt8(JSONExtractBool(data, 'streaming')) AS streaming,
    toUInt32(JSONExtractUInt(data, 'prompt_tokens')) AS prompt_tokens,
    toUInt32(JSONExtractUInt(data, 'completion_tokens')) AS completion_tokens,
    toUInt32(JSONExtractUInt(data, 'total_tokens')) AS total_tokens,
    JSONExtractInt(data, 'total_duration_ms') AS total_duration_ms,
    JSONExtractFloat(data, 'tokens_per_second') AS tokens_per_second,
    JSONExtractFloat(data, 'latency_score') AS latency_score,
    JSONExtractFloat(data, 'price_per_unit') AS price_per_unit,
    JSONExtractInt(data, 'ttft_ms') AS ttft_ms,
    JSONExtractString(data, 'finish_reason') AS finish_reason,
    JSONExtractString(data, 'error') AS error
FROM naap.accepted_raw_events
WHERE event_type = 'ai_llm_request'
  AND event_subtype IN ('llm_request_completed', 'llm_stream_completed');

INSERT INTO naap.normalized_byoc_job
SELECT
    event_id,
    event_ts,
    org,
    gateway,
    JSONExtractString(data, 'request_id') AS request_id,
    JSONExtractString(data, 'capability') AS capability,
    event_subtype AS subtype,
    event_type AS source_event_type,
    multiIf(
        event_subtype = 'job_gateway_completed',
            toUInt8(JSONExtractBool(data, 'success')),
        CAST(NULL, 'Nullable(UInt8)')
    ) AS success,
    JSONExtractInt(data, 'duration_ms') AS duration_ms,
    toUInt16(JSONExtractUInt(data, 'http_status')) AS http_status,
    lower(JSONExtractString(data, 'orchestrator_info', 'address')) AS orch_address,
    JSONExtractString(data, 'orchestrator_info', 'url') AS orch_url,
    lower(JSONExtractString(data, 'orchestrator_info', 'url')) AS orch_url_norm,
    JSONExtractString(data, 'worker_url') AS worker_url,
    toUInt8(JSONExtractBool(data, 'charged_compute')) AS charged_compute,
    JSONExtractInt(data, 'latency_ms') AS latency_ms,
    toInt32(JSONExtractInt(data, 'available_capacity')) AS available_capacity,
    JSONExtractString(data, 'error') AS error
FROM naap.accepted_raw_events
WHERE event_type IN ('job_gateway', 'job_orchestrator');

INSERT INTO naap.normalized_byoc_auth
SELECT
    event_id,
    event_ts,
    org,
    gateway,
    JSONExtractString(data, 'request_id') AS request_id,
    JSONExtractString(data, 'capability') AS capability,
    event_subtype AS subtype,
    lower(JSONExtractString(data, 'orchestrator_info', 'address')) AS orch_address,
    JSONExtractString(data, 'orchestrator_info', 'url') AS orch_url,
    lower(JSONExtractString(data, 'orchestrator_info', 'url')) AS orch_url_norm,
    toUInt8(JSONExtractBool(data, 'success')) AS success,
    JSONExtractString(data, 'error') AS error
FROM naap.accepted_raw_events
WHERE event_type = 'job_auth';

INSERT INTO naap.normalized_worker_lifecycle
SELECT
    event_id,
    event_ts,
    org,
    gateway,
    JSONExtractString(data, 'capability') AS capability,
    lower(JSONExtractString(data, 'orchestrator_info', 'address')) AS orch_address,
    JSONExtractString(data, 'orchestrator_info', 'url') AS orch_url,
    lower(JSONExtractString(data, 'orchestrator_info', 'url')) AS orch_url_norm,
    JSONExtractString(data, 'worker_url') AS worker_url,
    JSONExtractFloat(data, 'price_per_unit') AS price_per_unit,
    JSONExtractString(data, 'worker_options', 1, 'model') AS model,
    JSONExtractRaw(data, 'worker_options') AS worker_options_raw
FROM naap.accepted_raw_events
WHERE event_type = 'worker_lifecycle';

INSERT INTO naap.normalized_byoc_payment
SELECT
    event_id,
    event_ts,
    org,
    gateway,
    JSONExtractString(data, 'request_id') AS request_id,
    JSONExtractString(data, 'capability') AS capability,
    lower(JSONExtractString(data, 'orchestrator_info', 'address')) AS orch_address,
    JSONExtractFloat(data, 'amount') AS amount,
    JSONExtractString(data, 'currency') AS currency,
    event_subtype AS payment_type
FROM naap.accepted_raw_events
WHERE event_type = 'job_payment';

INSERT INTO naap.agg_orch_state
SELECT
    lower(JSONExtractString(orch_json, 'address')) AS orch_address,
    org,
    if(
        JSONExtractString(orch_json, 'local_address') != '',
        JSONExtractString(orch_json, 'local_address'),
        JSONExtractString(orch_json, 'address')
    ) AS name,
    JSONExtractString(orch_json, 'orch_uri') AS uri,
    JSONExtractString(orch_json, 'version') AS version,
    event_ts AS last_seen,
    orch_json AS raw_capabilities
FROM (
    SELECT
        event_ts,
        org,
        arrayJoin(JSONExtractArrayRaw(data)) AS orch_json
    FROM naap.accepted_raw_events
    WHERE event_type = 'network_capabilities'
      AND data NOT IN ('', '[]', 'null')
)
WHERE JSONExtractString(orch_json, 'address') != '';

INSERT INTO naap.agg_stream_status_samples
WITH
    JSONExtractString(data, 'stream_id') AS derived_stream_id,
    JSONExtractString(data, 'pipeline') AS derived_pipeline,
    JSONExtractString(data, 'orchestrator_info', 'url') AS derived_orch_url,
    lower(JSONExtractString(data, 'orchestrator_info', 'address')) AS derived_orch_address,
    JSONExtractString(data, 'inference_status', 'state') AS derived_inference_state,
    JSONExtractString(data, 'input_status', 'state') AS derived_input_state,
    JSONExtractFloat(data, 'inference_status', 'fps') AS derived_output_fps,
    JSONExtractFloat(data, 'input_status', 'fps') AS derived_input_fps,
    JSONExtractFloat(data, 'inference_status', 'latency_ms') AS derived_e2e_latency_ms
SELECT
    event_ts AS sample_ts,
    org,
    derived_stream_id AS stream_id,
    gateway,
    ifNull(o.orch_address, derived_orch_address) AS orch_address,
    derived_pipeline AS pipeline,
    multiIf(
        derived_inference_state = 'Running', 'ONLINE',
        derived_inference_state = 'Idle', 'LOADING',
        derived_input_state = 'Degraded', 'DEGRADED_INPUT',
        derived_inference_state = 'Degraded', 'DEGRADED_INFERENCE',
        'UNKNOWN'
    ) AS state,
    derived_output_fps AS output_fps,
    derived_input_fps AS input_fps,
    derived_e2e_latency_ms AS e2e_latency_ms,
    toUInt8(ifNull(o.orch_address, '') != '') AS is_attributed
FROM naap.accepted_raw_events
LEFT JOIN (
    SELECT uri, orch_address
    FROM naap.agg_orch_state FINAL
) AS o
    ON o.uri = derived_orch_url
WHERE event_type = 'ai_stream_status'
  AND derived_stream_id != '';

INSERT INTO naap.agg_fps_hourly
SELECT
    toStartOfHour(event_ts) AS hour,
    org,
    lower(JSONExtractString(data, 'orchestrator_info', 'address')) AS orch_address,
    JSONExtractString(data, 'pipeline') AS pipeline,
    JSONExtractFloat(data, 'inference_status', 'fps') AS inference_fps_sum,
    JSONExtractFloat(data, 'input_status', 'fps') AS input_fps_sum,
    1 AS sample_count
FROM naap.accepted_raw_events
WHERE event_type = 'ai_stream_status'
  AND JSONExtractFloat(data, 'inference_status', 'fps') > 0;

INSERT INTO naap.agg_discovery_latency_hourly
SELECT
    toStartOfHour(event_ts) AS hour,
    org,
    lower(JSONExtractString(orch_json, 'address')) AS orch_address,
    toUInt64OrDefault(JSONExtractString(orch_json, 'latency_ms')) AS latency_ms_sum,
    1 AS sample_count
FROM (
    SELECT
        event_ts,
        org,
        arrayJoin(JSONExtractArrayRaw(data)) AS orch_json
    FROM naap.accepted_raw_events
    WHERE event_type = 'discovery_results'
      AND data NOT IN ('', '[]', 'null')
)
WHERE JSONExtractString(orch_json, 'address') != '';

INSERT INTO naap.agg_webrtc_hourly
WITH
    JSONExtractArrayRaw(data, 'stats', 'track_stats') AS tracks,
    arrayFirst(x -> JSONExtractString(x, 'type') = 'video', tracks) AS video_track,
    arrayFirst(x -> JSONExtractString(x, 'type') = 'audio', tracks) AS audio_track
SELECT
    toStartOfHour(event_ts) AS hour,
    org,
    JSONExtractString(data, 'stream_id') AS stream_id,
    if(video_track != '', JSONExtractFloat(video_track, 'jitter'), 0.0) AS video_jitter_sum,
    if(video_track != '', toUInt64OrDefault(toString(JSONExtractUInt(video_track, 'packets_lost'))), 0) AS video_packets_lost,
    if(video_track != '', toUInt64OrDefault(toString(JSONExtractUInt(video_track, 'packets_received'))), 0) AS video_packets_recv,
    if(audio_track != '', JSONExtractFloat(audio_track, 'jitter'), 0.0) AS audio_jitter_sum,
    if(audio_track != '', toUInt64OrDefault(toString(JSONExtractUInt(audio_track, 'packets_lost'))), 0) AS audio_packets_lost,
    if(audio_track != '', toUInt64OrDefault(toString(JSONExtractUInt(audio_track, 'packets_received'))), 0) AS audio_packets_recv,
    1 AS sample_count,
    toUInt64(JSONExtractString(data, 'stats', 'conn_quality') = 'good') AS quality_good,
    toUInt64(JSONExtractString(data, 'stats', 'conn_quality') = 'fair') AS quality_fair,
    toUInt64(JSONExtractString(data, 'stats', 'conn_quality') = 'poor') AS quality_poor
FROM naap.accepted_raw_events
WHERE event_type = 'stream_ingest_metrics';

INSERT INTO naap.agg_payment_hourly
SELECT
    toStartOfHour(event_ts) AS hour,
    org,
    replaceRegexpOne(JSONExtractString(data, 'manifestID'), '^[0-9]+_', '') AS pipeline,
    lower(JSONExtractString(data, 'recipient')) AS orch_address,
    toUInt64OrDefault(trimRight(replaceAll(JSONExtractString(data, 'faceValue'), ' WEI', ''))) AS total_wei,
    1 AS event_count
FROM naap.accepted_raw_events
WHERE event_type = 'create_new_payment'
  AND JSONExtractString(data, 'recipient') != '';

INSERT INTO naap.agg_stream_hourly
SELECT
    toStartOfHour(event_ts) AS hour,
    org,
    '' AS pipeline,
    countIf(JSONExtractString(data, 'type') = 'gateway_receive_stream_request') AS started,
    countIf(JSONExtractString(data, 'type') = 'gateway_ingest_stream_closed') AS completed,
    countIf(JSONExtractString(data, 'type') = 'gateway_no_orchestrators_available') AS no_orch,
    countIf(JSONExtractString(data, 'type') = 'orchestrator_swap') AS orch_swap
FROM naap.accepted_raw_events
WHERE event_type = 'stream_trace'
GROUP BY hour, org, pipeline;

INSERT INTO naap.agg_stream_state
SELECT
    JSONExtractString(e.data, 'stream_id') AS stream_id,
    e.org,
    if(e.event_type = 'ai_stream_status', JSONExtractString(e.data, 'pipeline'), '') AS pipeline,
    if(e.event_type = 'ai_stream_status', JSONExtractString(e.data, 'state'), 'UNKNOWN') AS state,
    e.event_ts AS last_seen,
    if(
        e.event_type = 'stream_trace' AND JSONExtractString(e.data, 'type') = 'gateway_receive_stream_request',
        e.event_ts,
        toDateTime64(0, 3, 'UTC')
    ) AS started_at,
    if(
        e.event_type = 'stream_trace' AND JSONExtractString(e.data, 'type') = 'gateway_ingest_stream_closed',
        1,
        0
    ) AS is_closed,
    if(
        e.event_type = 'stream_trace' AND JSONExtractString(e.data, 'type') = 'gateway_no_orchestrators_available',
        1,
        0
    ) AS has_failure,
    if(e.event_type = 'ai_stream_status', lower(ifNull(o.orch_address, '')), '') AS orch_address
FROM naap.accepted_raw_events AS e
LEFT JOIN (
    SELECT uri, orch_address
    FROM naap.agg_orch_state FINAL
) AS o
    ON o.uri = JSONExtractString(e.data, 'orchestrator_info', 'url')
WHERE e.event_type IN ('stream_trace', 'ai_stream_status')
  AND JSONExtractString(e.data, 'stream_id') != '';

INSERT INTO naap.agg_orch_reliability_hourly
SELECT
    toStartOfHour(e.event_ts) AS hour,
    e.org,
    lower(ifNull(o.orch_address, '')) AS orch_address,
    1 AS ai_stream_count,
    toUInt64(JSONExtractString(e.data, 'state') IN ('DEGRADED_INFERENCE', 'DEGRADED_INPUT')) AS degraded_count,
    toUInt64(JSONExtractUInt(e.data, 'inference_status', 'restart_count') > 0) AS restart_count,
    toUInt64(JSONExtractString(e.data, 'inference_status', 'last_error') NOT IN ('', 'null')) AS error_count
FROM naap.accepted_raw_events AS e
LEFT JOIN (
    SELECT uri, orch_address
    FROM naap.agg_orch_state FINAL
) AS o
    ON o.uri = JSONExtractString(e.data, 'orchestrator_info', 'url')
WHERE e.event_type = 'ai_stream_status'
  AND JSONExtractString(e.data, 'orchestrator_info', 'url') != '';
