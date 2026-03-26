-- Migration 009: performance aggregate views (R3 — PERF-001 through PERF-004)
--
-- Source events:
--   ai_stream_status      — inference FPS, input FPS, orch address, pipeline
--   discovery_results     — per-orch discovery latency (latency_ms as string)
--   stream_ingest_metrics — WebRTC video/audio jitter, packet loss, conn_quality
--
-- Percentiles (p5, p50, p95, p99) are NOT pre-aggregated here. The Go API
-- computes them with quantile() over raw naap.events rows filtered by time
-- window. This is acceptable because ai_stream_status events are ~1–5% of
-- total volume and ClickHouse is fast at quantile on filtered datasets.
-- agg_fps_hourly covers avg FPS (PERF-001/PERF-002) and the time-series view.
--
-- Tradeoff documented: if P99 latency becomes too slow for percentile queries,
-- add AggregatingMergeTree tables with quantileState columns (Phase 8+ optimisation).

-- ── Inference FPS (PERF-001, PERF-002) ───────────────────────────────────────

CREATE TABLE IF NOT EXISTS naap.agg_fps_hourly
(
    hour               DateTime,
    org                LowCardinality(String),
    orch_address       String,
    pipeline           LowCardinality(String),
    -- SummingMergeTree sums these; divide at query time: fps_avg = sum/count.
    inference_fps_sum  Float64,
    input_fps_sum      Float64,
    sample_count       UInt64
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (org, pipeline, hour, orch_address)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS naap.mv_fps_hourly
TO naap.agg_fps_hourly
AS
SELECT
    toStartOfHour(event_ts)                                               AS hour,
    org,
    lower(JSONExtractString(data, 'orchestrator_info', 'address'))        AS orch_address,
    JSONExtractString(data, 'pipeline')                                   AS pipeline,
    -- PERF-001-c: exclude fps == 0 (stream not yet started).
    -- Filter is in WHERE; sum here is always > 0.
    JSONExtractFloat(data, 'inference_status', 'fps')                     AS inference_fps_sum,
    JSONExtractFloat(data, 'input_status', 'fps')                         AS input_fps_sum,
    1                                                                      AS sample_count
FROM naap.events
WHERE event_type = 'ai_stream_status'
  AND JSONExtractFloat(data, 'inference_status', 'fps') > 0;

-- ── Discovery latency (PERF-003) ─────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS naap.agg_discovery_latency_hourly
(
    hour            DateTime,
    org             LowCardinality(String),
    orch_address    String,
    -- latency_ms is stored as string in events ("77"); parsed to UInt64 in MV.
    -- PERF-003-b: convert at ingest time.
    latency_ms_sum  UInt64,
    sample_count    UInt64
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (org, hour, orch_address)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS naap.mv_discovery_latency_hourly
TO naap.agg_discovery_latency_hourly
AS
SELECT
    toStartOfHour(event_ts)                                               AS hour,
    org,
    lower(JSONExtractString(orch_json, 'address'))                        AS orch_address,
    -- latency_ms is a string in the event payload; parse to UInt64.
    toUInt64OrDefault(JSONExtractString(orch_json, 'latency_ms'))         AS latency_ms_sum,
    1                                                                      AS sample_count
FROM (
    -- discovery_results data is a JSON array of orch discovery objects.
    SELECT
        event_ts,
        org,
        arrayJoin(JSONExtractArrayRaw(data)) AS orch_json
    FROM naap.events
    WHERE event_type = 'discovery_results'
      AND data NOT IN ('', '[]', 'null')
)
WHERE JSONExtractString(orch_json, 'address') != '';

-- ── WebRTC quality (PERF-004) ─────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS naap.agg_webrtc_hourly
(
    hour                  DateTime,
    org                   LowCardinality(String),
    -- stream_id for per-stream lookups; '' for network-wide aggregates.
    stream_id             String,
    video_jitter_sum      Float64,
    video_packets_lost    UInt64,
    video_packets_recv    UInt64,
    audio_jitter_sum      Float64,
    audio_packets_lost    UInt64,
    audio_packets_recv    UInt64,
    sample_count          UInt64,
    -- conn_quality distribution counts (PERF-004-b).
    quality_good          UInt64,
    quality_fair          UInt64,
    quality_poor          UInt64
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (org, stream_id, hour)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS naap.mv_webrtc_hourly
TO naap.agg_webrtc_hourly
AS
WITH
    -- Extract track_stats array once per row.
    JSONExtractArrayRaw(data, 'stats', 'track_stats') AS tracks,
    -- Find video and audio track objects within the array.
    arrayFirst(x -> JSONExtractString(x, 'type') = 'video', tracks) AS video_track,
    arrayFirst(x -> JSONExtractString(x, 'type') = 'audio', tracks) AS audio_track
SELECT
    toStartOfHour(event_ts)                                               AS hour,
    org,
    JSONExtractString(data, 'stream_id')                                  AS stream_id,
    -- video metrics
    if(video_track != '',
        JSONExtractFloat(video_track, 'jitter'), 0.0)                     AS video_jitter_sum,
    if(video_track != '',
        toUInt64OrDefault(toString(JSONExtractUInt(video_track, 'packets_lost'))), 0) AS video_packets_lost,
    if(video_track != '',
        toUInt64OrDefault(toString(JSONExtractUInt(video_track, 'packets_received'))), 0) AS video_packets_recv,
    -- audio metrics
    if(audio_track != '',
        JSONExtractFloat(audio_track, 'jitter'), 0.0)                     AS audio_jitter_sum,
    if(audio_track != '',
        toUInt64OrDefault(toString(JSONExtractUInt(audio_track, 'packets_lost'))), 0) AS audio_packets_lost,
    if(audio_track != '',
        toUInt64OrDefault(toString(JSONExtractUInt(audio_track, 'packets_received'))), 0) AS audio_packets_recv,
    1                                                                      AS sample_count,
    -- conn_quality distribution
    toUInt64(JSONExtractString(data, 'stats', 'conn_quality') = 'good')   AS quality_good,
    toUInt64(JSONExtractString(data, 'stats', 'conn_quality') = 'fair')   AS quality_fair,
    toUInt64(JSONExtractString(data, 'stats', 'conn_quality') = 'poor')   AS quality_poor
FROM naap.events
WHERE event_type = 'stream_ingest_metrics';
