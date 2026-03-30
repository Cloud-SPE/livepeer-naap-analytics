-- Rebuild raw-fed dashboard tables from accepted_raw_events.
--
-- Run this after migration 054 when you need historical rows repopulated for
-- the dashboard-facing tables that were repointed off the frozen legacy
-- naap.events source.

TRUNCATE TABLE naap.agg_stream_status_samples;
TRUNCATE TABLE naap.agg_fps_hourly;
TRUNCATE TABLE naap.agg_discovery_latency_hourly;
TRUNCATE TABLE naap.agg_webrtc_hourly;
TRUNCATE TABLE naap.typed_stream_ingest_metrics;

INSERT INTO naap.agg_stream_status_samples
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

INSERT INTO naap.typed_stream_ingest_metrics
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
