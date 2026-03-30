-- Migration 054: canonical session-edge latency semantics and raw-MV repoint.

ALTER TABLE naap.normalized_ai_stream_status
    ADD COLUMN IF NOT EXISTS start_time Nullable(DateTime64(3, 'UTC'))
    AFTER e2e_latency_ms;

DROP VIEW IF EXISTS naap.mv_normalized_ai_stream_status;

CREATE MATERIALIZED VIEW IF NOT EXISTS naap.mv_normalized_ai_stream_status
TO naap.normalized_ai_stream_status
AS
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
    JSONExtractString(data, 'state') AS state,
    lower(JSONExtractString(data, 'orchestrator_info', 'address')) AS orch_raw_address,
    JSONExtractString(data, 'orchestrator_info', 'url') AS orch_url,
    lower(JSONExtractString(data, 'orchestrator_info', 'url')) AS orch_url_norm,
    JSONExtractFloat(data, 'inference_status', 'fps') AS output_fps,
    JSONExtractFloat(data, 'input_status', 'fps') AS input_fps,
    JSONExtractFloat(data, 'inference_status', 'latency_ms') AS e2e_latency_ms,
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

ALTER TABLE naap.normalized_session_trace_rollup_latest
    ADD COLUMN IF NOT EXISTS first_processed_at_state AggregateFunction(minIf, DateTime64(3, 'UTC'), UInt8)
    AFTER started_at_state;

ALTER TABLE naap.normalized_session_trace_rollup_latest
    ADD COLUMN IF NOT EXISTS few_processed_at_state AggregateFunction(minIf, DateTime64(3, 'UTC'), UInt8)
    AFTER first_processed_at_state;

ALTER TABLE naap.normalized_session_trace_rollup_latest
    ADD COLUMN IF NOT EXISTS first_ingest_at_state AggregateFunction(minIf, DateTime64(3, 'UTC'), UInt8)
    AFTER few_processed_at_state;

ALTER TABLE naap.normalized_session_trace_rollup_latest
    ADD COLUMN IF NOT EXISTS runner_first_processed_at_state AggregateFunction(minIf, DateTime64(3, 'UTC'), UInt8)
    AFTER first_ingest_at_state;

DROP VIEW IF EXISTS naap.mv_normalized_session_trace_rollup_latest;

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
    minIfState(event_ts, toUInt8(trace_type = 'gateway_receive_first_processed_segment')) AS first_processed_at_state,
    minIfState(event_ts, toUInt8(trace_type = 'gateway_receive_few_processed_segments')) AS few_processed_at_state,
    minIfState(event_ts, toUInt8(trace_type = 'gateway_send_first_ingest_segment')) AS first_ingest_at_state,
    minIfState(event_ts, toUInt8(trace_type = 'runner_send_first_processed_segment')) AS runner_first_processed_at_state,
    sumState(toUInt64(trace_type = 'gateway_receive_stream_request')) AS started_count_state,
    sumState(toUInt64(trace_type = 'gateway_receive_few_processed_segments')) AS playable_seen_count_state,
    sumState(toUInt64(trace_type = 'gateway_no_orchestrators_available')) AS no_orch_count_state,
    sumState(toUInt64(trace_type = 'gateway_ingest_stream_closed')) AS completed_count_state,
    sumState(toUInt64(trace_type = 'orchestrator_swap')) AS swap_count_state,
    maxState(event_ts) AS trace_last_seen_state
FROM naap.normalized_stream_trace
WHERE canonical_session_key != ''
GROUP BY org, canonical_session_key;

ALTER TABLE naap.normalized_session_status_rollup_latest
    ADD COLUMN IF NOT EXISTS start_time_state AggregateFunction(minIf, DateTime64(3, 'UTC'), UInt8)
    AFTER canonical_pipeline_state;

DROP VIEW IF EXISTS naap.mv_normalized_session_status_rollup_latest;

CREATE MATERIALIZED VIEW IF NOT EXISTS naap.mv_normalized_session_status_rollup_latest
TO naap.normalized_session_status_rollup_latest
AS
SELECT
    org,
    canonical_session_key,
    argMaxIfState(stream_id, event_ts, toUInt8(stream_id != '')) AS stream_id_state,
    argMaxIfState(request_id, event_ts, toUInt8(request_id != '')) AS request_id_state,
    argMaxIfState(raw_pipeline_hint, event_ts, toUInt8(raw_pipeline_hint != '')) AS canonical_pipeline_state,
    minIfState(coalesce(start_time, toDateTime64(0, 3, 'UTC')), toUInt8(start_time IS NOT NULL)) AS start_time_state,
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

ALTER TABLE naap.canonical_session_current_store
    ADD COLUMN IF NOT EXISTS startup_latency_ms Nullable(Float64)
    AFTER last_seen;

ALTER TABLE naap.canonical_session_current_store
    ADD COLUMN IF NOT EXISTS e2e_latency_ms Nullable(Float64)
    AFTER startup_latency_ms;

ALTER TABLE naap.canonical_session_current_store
    ADD COLUMN IF NOT EXISTS prompt_to_playable_latency_ms Nullable(Float64)
    AFTER e2e_latency_ms;

ALTER TABLE naap.canonical_status_hours_store
    ADD COLUMN IF NOT EXISTS startup_latency_ms Nullable(Float64)
    AFTER session_last_seen;

ALTER TABLE naap.canonical_status_hours_store
    ADD COLUMN IF NOT EXISTS prompt_to_playable_latency_ms Nullable(Float64)
    AFTER avg_e2e_latency_ms;

DROP VIEW IF EXISTS naap.mv_stream_status_samples;

CREATE MATERIALIZED VIEW IF NOT EXISTS naap.mv_stream_status_samples
TO naap.agg_stream_status_samples
AS
SELECT
    event_ts AS sample_ts,
    org,
    JSONExtractString(data, 'stream_id') AS stream_id,
    gateway,
    ifNull(
        o.orch_address,
        lower(JSONExtractString(data, 'orchestrator_info', 'address'))
    ) AS orch_address,
    JSONExtractString(data, 'pipeline') AS pipeline,
    multiIf(
        JSONExtractString(data, 'inference_status', 'state') = 'Running', 'ONLINE',
        JSONExtractString(data, 'inference_status', 'state') = 'Idle', 'LOADING',
        JSONExtractString(data, 'input_status', 'state') = 'Degraded', 'DEGRADED_INPUT',
        JSONExtractString(data, 'inference_status', 'state') = 'Degraded', 'DEGRADED_INFERENCE',
        'UNKNOWN'
    ) AS state,
    JSONExtractFloat(data, 'inference_status', 'fps') AS output_fps,
    JSONExtractFloat(data, 'input_status', 'fps') AS input_fps,
    JSONExtractFloat(data, 'inference_status', 'latency_ms') AS e2e_latency_ms,
    toUInt8(ifNull(o.orch_address, '') != '') AS is_attributed
FROM naap.accepted_raw_events
LEFT JOIN (SELECT uri, orch_address FROM naap.agg_orch_state FINAL) o
    ON o.uri = JSONExtractString(data, 'orchestrator_info', 'url')
WHERE event_type = 'ai_stream_status'
  AND JSONExtractString(data, 'stream_id') != '';

DROP VIEW IF EXISTS naap.mv_fps_hourly;

CREATE MATERIALIZED VIEW IF NOT EXISTS naap.mv_fps_hourly
TO naap.agg_fps_hourly
AS
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

DROP VIEW IF EXISTS naap.mv_discovery_latency_hourly;

CREATE MATERIALIZED VIEW IF NOT EXISTS naap.mv_discovery_latency_hourly
TO naap.agg_discovery_latency_hourly
AS
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

DROP VIEW IF EXISTS naap.mv_webrtc_hourly;

CREATE MATERIALIZED VIEW IF NOT EXISTS naap.mv_webrtc_hourly
TO naap.agg_webrtc_hourly
AS
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

DROP VIEW IF EXISTS naap.mv_typed_stream_ingest_metrics;

CREATE MATERIALIZED VIEW IF NOT EXISTS naap.mv_typed_stream_ingest_metrics
TO naap.typed_stream_ingest_metrics
AS
WITH
    JSONExtractArrayRaw(data, 'stats', 'track_stats') AS tracks,
    arrayFirst(x -> JSONExtractString(x, 'type') = 'video', tracks) AS video_track,
    arrayFirst(x -> JSONExtractString(x, 'type') = 'audio', tracks) AS audio_track
SELECT
    event_id,
    event_ts,
    org,
    gateway,
    JSONExtractString(data, 'stream_id') AS stream_id,
    JSONExtractString(data, 'request_id') AS request_id,
    JSONExtractString(data, 'stats', 'conn_quality') AS conn_quality,
    if(video_track != '', JSONExtractFloat(video_track, 'jitter'), 0.0) AS video_jitter_ms,
    if(video_track != '', toUInt64OrDefault(toString(JSONExtractUInt(video_track, 'packets_lost'))), 0) AS video_packets_lost,
    if(video_track != '', toUInt64OrDefault(toString(JSONExtractUInt(video_track, 'packets_received'))), 0) AS video_packets_received,
    if(audio_track != '', JSONExtractFloat(audio_track, 'jitter'), 0.0) AS audio_jitter_ms,
    if(audio_track != '', toUInt64OrDefault(toString(JSONExtractUInt(audio_track, 'packets_lost'))), 0) AS audio_packets_lost,
    if(audio_track != '', toUInt64OrDefault(toString(JSONExtractUInt(audio_track, 'packets_received'))), 0) AS audio_packets_received,
    data
FROM naap.accepted_raw_events
WHERE event_type = 'stream_ingest_metrics';
