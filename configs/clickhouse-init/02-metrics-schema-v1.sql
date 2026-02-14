-- ============================================================
-- LIVEPEER ANALYTICS - METRICS SCHEMA V1 (PROPOSAL)
-- ============================================================
-- Scope:
-- - Keep Flink as source of truth for dedup, schema validation, correlation, sessionization.
-- - ClickHouse stores curated facts + rollups for serving dashboards/APIs.
-- - This file is additive and intentionally does not modify existing raw/fanned-out tables.

CREATE DATABASE IF NOT EXISTS livepeer_analytics;
USE livepeer_analytics;

-- ============================================================
-- 0) COMMON NOTES
-- ============================================================
-- Session identity contract (Flink-owned, deterministic):
--   workflow_session_id =
--     if(stream_id != '' and request_id != '', concat(stream_id, '|', request_id),
--     else stream_id / request_id / event_id fallback)
--
-- Dedup/idempotency contract (Flink-owned):
--   event_uid, source, version
--
-- Grain naming:
--   fact_* : queryable curated fact table with explicit grain
--   agg_*  : pre-aggregated serving table (1m/1h)
--   dim_*  : slowly-changing or latest-state dimensions for enrichment


-- ============================================================
-- 1) DIMENSIONS (DATA-DRIVEN, NO STATIC CONFIG)
-- ============================================================

-- Grain: 1 row per orchestrator_address + model_id + pipeline_id + capability snapshot timestamp.
CREATE TABLE IF NOT EXISTS dim_orchestrator_capability_snapshots
(
    snapshot_ts DateTime64(3, 'UTC'),
    snapshot_date Date MATERIALIZED toDate(snapshot_ts),

    orchestrator_address String,
    orchestrator_proxy_address String,
    orchestrator_url String,

    pipeline String,
    pipeline_id String,
    model_id String,
    capability_id Int32,
    capability_name LowCardinality(String),
    capability_group LowCardinality(String),

    gpu_id Nullable(String),
    gpu_name Nullable(String),
    gpu_memory_total Nullable(UInt64),
    runner_version Nullable(String),

    region Nullable(String),

    source_event_uid String,
    ingestion_timestamp DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(snapshot_ts)
PARTITION BY toYYYYMM(snapshot_date)
ORDER BY (orchestrator_address, pipeline_id, model_id, snapshot_ts)
TTL snapshot_date + INTERVAL 365 DAY DELETE
SETTINGS index_granularity = 8192;

-- Grain: 1 row per snapshot event x orchestrator x capability_id.
CREATE TABLE IF NOT EXISTS dim_orchestrator_capability_advertised_snapshots
(
    snapshot_ts DateTime64(3, 'UTC'),
    snapshot_date Date MATERIALIZED toDate(snapshot_ts),
    source_event_uid String,

    orchestrator_address String,
    orchestrator_proxy_address String,
    orchestrator_url String,

    capability_id Int32,
    capability_name LowCardinality(String),
    capability_group LowCardinality(String),
    capacity Nullable(Int32),
    capability_catalog_version LowCardinality(String),

    ingestion_timestamp DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(snapshot_ts)
PARTITION BY toYYYYMM(snapshot_date)
ORDER BY (orchestrator_address, capability_id, snapshot_ts, source_event_uid)
TTL snapshot_date + INTERVAL 365 DAY DELETE
SETTINGS index_granularity = 8192;

-- Grain: 1 row per snapshot event x orchestrator x capability_id x model_id.
CREATE TABLE IF NOT EXISTS dim_orchestrator_capability_model_constraints
(
    snapshot_ts DateTime64(3, 'UTC'),
    snapshot_date Date MATERIALIZED toDate(snapshot_ts),
    source_event_uid String,

    orchestrator_address String,
    orchestrator_proxy_address String,
    orchestrator_url String,

    capability_id Int32,
    capability_name LowCardinality(String),
    capability_group LowCardinality(String),
    capability_catalog_version LowCardinality(String),

    model_id String,
    runner_version Nullable(String),
    capacity Nullable(Int32),
    capacity_in_use Nullable(Int32),
    warm Nullable(UInt8),

    ingestion_timestamp DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(snapshot_ts)
PARTITION BY toYYYYMM(snapshot_date)
ORDER BY (orchestrator_address, capability_id, model_id, snapshot_ts, source_event_uid)
TTL snapshot_date + INTERVAL 365 DAY DELETE
SETTINGS index_granularity = 8192;

-- Grain: 1 row per snapshot event x orchestrator x capability_id x constraint.
CREATE TABLE IF NOT EXISTS dim_orchestrator_capability_prices
(
    snapshot_ts DateTime64(3, 'UTC'),
    snapshot_date Date MATERIALIZED toDate(snapshot_ts),
    source_event_uid String,

    orchestrator_address String,
    orchestrator_proxy_address String,
    orchestrator_url String,

    capability_id Int32,
    capability_name LowCardinality(String),
    capability_group LowCardinality(String),
    capability_catalog_version LowCardinality(String),
    constraint_name Nullable(String),
    price_per_unit Nullable(Int32),
    pixels_per_unit Nullable(Int32),

    ingestion_timestamp DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(snapshot_ts)
PARTITION BY toYYYYMM(snapshot_date)
ORDER BY (orchestrator_address, capability_id, snapshot_ts, source_event_uid)
TTL snapshot_date + INTERVAL 365 DAY DELETE
SETTINGS index_granularity = 8192;

-- Derived current dimension (latest snapshot by orchestrator + workflow + model + GPU).
CREATE VIEW IF NOT EXISTS dim_orchestrator_capability_current AS
SELECT
    orchestrator_address,
    argMax(orchestrator_proxy_address, snapshot_ts) AS orchestrator_proxy_address,
    argMax(orchestrator_url, snapshot_ts) AS orchestrator_url,
    pipeline,
    pipeline_id,
    model_id,
    capability_id,
    argMax(capability_name, snapshot_ts) AS capability_name,
    argMax(capability_group, snapshot_ts) AS capability_group,
    gpu_id,
    argMax(gpu_name, snapshot_ts) AS gpu_name,
    argMax(gpu_memory_total, snapshot_ts) AS gpu_memory_total,
    argMax(runner_version, snapshot_ts) AS runner_version,
    argMax(region, snapshot_ts) AS region,
    max(snapshot_ts) AS snapshot_ts
FROM dim_orchestrator_capability_snapshots
GROUP BY
    orchestrator_address,
    pipeline,
    pipeline_id,
    model_id,
    capability_id,
    gpu_id;


-- ============================================================
-- 2) FACT TABLES
-- ============================================================

-- Grain: 1 row per workflow session.
-- Purpose: canonical lifecycle key for reliability metrics and drill-through.
CREATE TABLE IF NOT EXISTS fact_workflow_sessions
(
    workflow_session_id String,
    workflow_type LowCardinality(String),
    workflow_id String,

    stream_id String,
    request_id String,
    session_id String,
    pipeline String,
    pipeline_id String,

    gateway String,
    orchestrator_address String,
    orchestrator_url String,
    model_id Nullable(String),
    gpu_id Nullable(String),
    region Nullable(String),
    attribution_method LowCardinality(String),
    attribution_confidence Float32,

    session_start_ts DateTime64(3, 'UTC'),
    session_end_ts Nullable(DateTime64(3, 'UTC')),

    known_stream UInt8,
    startup_success UInt8,
    startup_excused UInt8,
    startup_unexcused UInt8,

    swap_count UInt16,
    error_count UInt32,
    excusable_error_count UInt32,

    first_stream_request_ts Nullable(DateTime64(3, 'UTC')),
    first_processed_ts Nullable(DateTime64(3, 'UTC')),
    first_playable_ts Nullable(DateTime64(3, 'UTC')),

    event_count UInt32,
    version UInt64,

    source_first_event_uid String,
    source_last_event_uid String,
    ingestion_timestamp DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(toDate(session_start_ts))
ORDER BY (session_start_ts, workflow_session_id)
TTL toDate(session_start_ts) + INTERVAL 180 DAY DELETE
SETTINGS index_granularity = 8192;

-- Grain: 1 row per session segment (actor assignment window).
-- Purpose: swap analysis + actor attribution over time.
CREATE TABLE IF NOT EXISTS fact_workflow_session_segments
(
    workflow_session_id String,
    segment_index UInt16,

    segment_start_ts DateTime64(3, 'UTC'),
    segment_end_ts Nullable(DateTime64(3, 'UTC')),

    gateway String,
    orchestrator_address String,
    orchestrator_url String,
    worker_id Nullable(String),
    gpu_id Nullable(String),
    model_id Nullable(String),
    region Nullable(String),
    attribution_method LowCardinality(String),
    attribution_confidence Float32,

    reason LowCardinality(String),
    source_trace_type LowCardinality(String),
    source_event_uid String,
    version UInt64,

    ingestion_timestamp DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(toDate(segment_start_ts))
ORDER BY (segment_start_ts, workflow_session_id, segment_index)
TTL toDate(segment_start_ts) + INTERVAL 180 DAY DELETE
SETTINGS index_granularity = 8192;

-- Grain: 1 row per status sample (5s-ish cadence from status events).
-- Purpose: output FPS, FPS jitter coefficient, state timeline.
CREATE TABLE IF NOT EXISTS fact_stream_status_samples
(
    sample_ts DateTime64(3, 'UTC'),
    sample_date Date MATERIALIZED toDate(sample_ts),

    workflow_session_id String,
    stream_id String,
    request_id String,

    gateway String,
    orchestrator_address String,
    orchestrator_url String,

    pipeline String,
    pipeline_id String,
    model_id Nullable(String),
    gpu_id Nullable(String),
    region Nullable(String),
    attribution_method LowCardinality(String),
    attribution_confidence Float32,

    state LowCardinality(String),
    output_fps Float32,
    input_fps Float32,

    source_event_uid String,
    ingestion_timestamp DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(sample_date)
ORDER BY (sample_date, workflow_session_id, sample_ts)
TTL sample_date + INTERVAL 180 DAY DELETE
SETTINGS index_granularity = 8192;

-- Grain: 1 row per normalized trace edge event.
-- Purpose: latency edge pairing, startup/swap/failure classification evidence.
CREATE TABLE IF NOT EXISTS fact_stream_trace_edges
(
    edge_ts DateTime64(3, 'UTC'),
    edge_date Date MATERIALIZED toDate(edge_ts),

    workflow_session_id String,
    stream_id String,
    request_id String,

    gateway String,
    orchestrator_address String,
    orchestrator_url String,

    pipeline String,
    pipeline_id String,

    trace_type LowCardinality(String),
    trace_category LowCardinality(String),
    is_swap_event UInt8,

    source_event_uid String,
    ingestion_timestamp DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(edge_date)
ORDER BY (edge_date, workflow_session_id, edge_ts, trace_type)
TTL edge_date + INTERVAL 180 DAY DELETE
SETTINGS index_granularity = 8192;

-- Grain: 1 row per ingest metric sample.
-- Purpose: ingest-leg quality only (explicitly not orchestrator transport bandwidth).
CREATE TABLE IF NOT EXISTS fact_stream_ingest_samples
(
    sample_ts DateTime64(3, 'UTC'),
    sample_date Date MATERIALIZED toDate(sample_ts),

    workflow_session_id String,
    stream_id String,
    request_id String,
    pipeline_id String,

    connection_quality LowCardinality(String),
    video_jitter Float32,
    audio_jitter Float32,
    video_latency Float32,
    audio_latency Float32,

    bytes_received UInt64,
    bytes_sent UInt64,

    source_event_uid String,
    ingestion_timestamp DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(sample_date)
ORDER BY (sample_date, workflow_session_id, sample_ts)
TTL sample_date + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

-- Grain: 1 row per orchestrator transport bandwidth sample (future scraper source).
-- Purpose: true network KPI for gateway<->orchestrator and orchestrator<->worker links.
CREATE TABLE IF NOT EXISTS fact_orchestrator_transport_bandwidth
(
    sample_ts DateTime64(3, 'UTC'),
    sample_date Date MATERIALIZED toDate(sample_ts),

    orchestrator_address String,
    gateway String,
    workflow_session_id Nullable(String),
    stream_id Nullable(String),
    request_id Nullable(String),

    link_type LowCardinality(String),
    rx_bps UInt64,
    tx_bps UInt64,

    source_system LowCardinality(String),
    source_sample_id String,
    ingestion_timestamp DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(sample_date)
ORDER BY (sample_date, orchestrator_address, gateway, sample_ts)
TTL sample_date + INTERVAL 180 DAY DELETE
SETTINGS index_granularity = 8192;


-- ============================================================
-- 3) ROLLUP TABLES
-- ============================================================

-- Grain: 1 row per 1-minute bucket x orchestrator x workflow.
-- Join-back keys to facts: workflow_session_id (sample), stream_id, request_id.
CREATE TABLE IF NOT EXISTS agg_stream_performance_1m
(
    window_start DateTime64(3, 'UTC'),

    gateway String,
    orchestrator_address String,
    pipeline String,
    pipeline_id String,
    model_id Nullable(String),
    gpu_id Nullable(String),
    region Nullable(String),

    sessions_uniq_state AggregateFunction(uniqExact, String),
    streams_uniq_state AggregateFunction(uniqExact, String),

    output_fps_avg_state AggregateFunction(avg, Float32),
    output_fps_p95_state AggregateFunction(quantileTDigest(0.95), Float32),
    fps_jitter_num_state AggregateFunction(stddevPop, Float32),
    fps_jitter_den_state AggregateFunction(avg, Float32),

    sample_count_state AggregateFunction(count)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(toDate(window_start))
ORDER BY (window_start, orchestrator_address, pipeline_id, gpu_id)
TTL toDate(window_start) + INTERVAL 365 DAY DELETE
SETTINGS index_granularity = 8192;

-- Grain: 1 row per 1-hour bucket x gateway/orchestrator/workflow.
-- Join-back keys to facts: workflow_session_id for drill-through.
CREATE TABLE IF NOT EXISTS agg_reliability_1h
(
    window_start DateTime64(3, 'UTC'),

    gateway String,
    orchestrator_address String,
    pipeline String,
    pipeline_id String,
    model_id Nullable(String),
    gpu_id Nullable(String),
    region Nullable(String),

    known_sessions_state AggregateFunction(sum, UInt64),
    success_sessions_state AggregateFunction(sum, UInt64),
    excused_sessions_state AggregateFunction(sum, UInt64),
    unexcused_sessions_state AggregateFunction(sum, UInt64),
    swapped_sessions_state AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(toDate(window_start))
ORDER BY (window_start, gateway, orchestrator_address, pipeline_id)
TTL toDate(window_start) + INTERVAL 400 DAY DELETE
SETTINGS index_granularity = 8192;

-- Grain: 1 row per 1-minute bucket x request/session for latency validation APIs.
CREATE TABLE IF NOT EXISTS agg_latency_edges_1m
(
    window_start DateTime64(3, 'UTC'),
    workflow_session_id String,
    stream_id String,
    request_id String,

    gateway String,
    orchestrator_address String,
    pipeline String,
    pipeline_id String,

    startup_ms_avg_state AggregateFunction(avg, Float64),
    prompt_to_playable_ms_avg_state AggregateFunction(avg, Float64),
    e2e_proxy_ms_avg_state AggregateFunction(avg, Float64),

    valid_startup_pairs_state AggregateFunction(sum, UInt64),
    valid_playable_pairs_state AggregateFunction(sum, UInt64),
    valid_e2e_pairs_state AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(toDate(window_start))
ORDER BY (window_start, workflow_session_id)
TTL toDate(window_start) + INTERVAL 180 DAY DELETE
SETTINGS index_granularity = 8192;


-- ============================================================
-- 4) MATERIALIZED VIEWS (CURATED FACTS -> ROLLUPS)
-- ============================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_fact_status_to_perf_1m
TO agg_stream_performance_1m
AS
SELECT
    toStartOfInterval(sample_ts, INTERVAL 1 MINUTE) AS window_start,
    gateway,
    orchestrator_address,
    pipeline,
    pipeline_id,
    model_id,
    gpu_id,
    region,

    uniqExactState(workflow_session_id) AS sessions_uniq_state,
    uniqExactState(stream_id) AS streams_uniq_state,

    avgState(output_fps) AS output_fps_avg_state,
    quantileTDigestState(0.95)(output_fps) AS output_fps_p95_state,
    stddevPopState(output_fps) AS fps_jitter_num_state,
    avgState(output_fps) AS fps_jitter_den_state,

    countState() AS sample_count_state
FROM fact_stream_status_samples
GROUP BY
    window_start,
    gateway,
    orchestrator_address,
    pipeline,
    pipeline_id,
    model_id,
    gpu_id,
    region;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_fact_sessions_to_reliability_1h
TO agg_reliability_1h
AS
SELECT
    toStartOfInterval(session_start_ts, INTERVAL 1 HOUR) AS window_start,
    gateway,
    orchestrator_address,
    pipeline,
    pipeline_id,
    model_id,
    gpu_id,
    region,

    sumState(toUInt64(known_stream)) AS known_sessions_state,
    sumState(toUInt64(startup_success)) AS success_sessions_state,
    sumState(toUInt64(startup_excused)) AS excused_sessions_state,
    sumState(toUInt64(startup_unexcused)) AS unexcused_sessions_state,
    sumState(toUInt64(swap_count > 0)) AS swapped_sessions_state
FROM fact_workflow_sessions
GROUP BY
    window_start,
    gateway,
    orchestrator_address,
    pipeline,
    pipeline_id,
    model_id,
    gpu_id,
    region;

-- ============================================================
-- 5) API-SAFE SERVING VIEWS
-- ============================================================

-- API: /gpu/metrics
-- Backing keys: gpu_id + orchestrator_address + pipeline_id + window_start.
CREATE VIEW IF NOT EXISTS v_api_gpu_metrics AS
SELECT
    window_start,
    orchestrator_address,
    pipeline,
    pipeline_id,
    model_id,
    gpu_id,
    region,
    avgMerge(output_fps_avg_state) AS avg_output_fps,
    quantileTDigestMerge(0.95)(output_fps_p95_state) AS p95_output_fps,
    stddevPopMerge(fps_jitter_num_state) / nullIf(avgMerge(fps_jitter_den_state), 0) AS jitter_coeff_fps,
    countMerge(sample_count_state) AS status_samples
FROM agg_stream_performance_1m
GROUP BY
    window_start,
    orchestrator_address,
    pipeline,
    pipeline_id,
    model_id,
    gpu_id,
    region;

-- API: /network/demand
-- Backing keys: gateway + region + workflow + window_start.
CREATE VIEW IF NOT EXISTS v_api_network_demand AS
SELECT
    window_start,
    gateway,
    region,
    pipeline,
    pipeline_id,
    uniqExactMerge(sessions_uniq_state) AS active_sessions,
    uniqExactMerge(streams_uniq_state) AS active_streams,
    avgMerge(output_fps_avg_state) AS avg_output_fps
FROM agg_stream_performance_1m
GROUP BY
    window_start,
    gateway,
    region,
    pipeline,
    pipeline_id;

-- API: /sla/compliance
-- Backing keys: orchestrator + period; score formula remains configurable in app.
CREATE VIEW IF NOT EXISTS v_api_sla_compliance AS
SELECT
    window_start,
    orchestrator_address,
    pipeline,
    pipeline_id,
    model_id,
    gpu_id,
    region,
    sumMerge(known_sessions_state) AS known_sessions,
    sumMerge(unexcused_sessions_state) AS unexcused_sessions,
    sumMerge(swapped_sessions_state) AS swapped_sessions,
    1 - (sumMerge(unexcused_sessions_state) / nullIf(sumMerge(known_sessions_state), 0)) AS success_ratio,
    1 - (sumMerge(swapped_sessions_state) / nullIf(sumMerge(known_sessions_state), 0)) AS no_swap_ratio
FROM agg_reliability_1h
GROUP BY
    window_start,
    orchestrator_address,
    pipeline,
    pipeline_id,
    model_id,
    gpu_id,
    region;


-- ============================================================
-- 6) HEALTH CHECK VIEWS
-- ============================================================

CREATE VIEW IF NOT EXISTS v_health_rollup_freshness AS
SELECT
    'fact_stream_status_samples' AS table_name,
    max(sample_ts) AS max_event_ts,
    now64(3) AS checked_at,
    dateDiff('minute', max(sample_ts), now64(3)) AS lag_minutes
FROM fact_stream_status_samples
UNION ALL
SELECT
    'fact_workflow_sessions' AS table_name,
    max(session_start_ts) AS max_event_ts,
    now64(3) AS checked_at,
    dateDiff('minute', max(session_start_ts), now64(3)) AS lag_minutes
FROM fact_workflow_sessions;


-- ============================================================
-- 7) BACKFILL GUIDANCE (RUN MANUALLY IN CHUNKS)
-- ============================================================
-- 1) Backfill facts first from raw/fanned-out curated inputs.
-- 2) Backfill rollups from facts after each fact chunk.
-- 3) Validate coverage metrics before moving to next chunk.
--
-- Example chunk pattern:
--   WHERE sample_ts >= {from_ts} AND sample_ts < {to_ts}
--
-- Keep idempotent writes by preserving Flink event_uid + version and using ReplacingMergeTree where needed.
