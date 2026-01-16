-- ============================================
-- LIVEPEER ANALYTICS - COMPLETE SCHEMA
-- ============================================

CREATE DATABASE IF NOT EXISTS livepeer_analytics;
USE livepeer_analytics;

-- ============================================
-- CORE TABLES (Existing)
-- ============================================

-- Table 1: AI Stream Status (core performance metrics)
CREATE TABLE IF NOT EXISTS ai_stream_status
(
    event_timestamp DateTime64(3, 'UTC'),
    event_date Date MATERIALIZED toDate(event_timestamp),
    event_hour UInt8 MATERIALIZED toHour(event_timestamp),

    -- Identifiers
    stream_id String,
    request_id String,
    gateway String,

    -- Orchestrator info (WILL BE ENRICHED by Flink)
    orchestrator_address String,
    orchestrator_url String,
    gpu_id Nullable(String),
    region LowCardinality(Nullable(String)),

    -- Workflow info
    pipeline LowCardinality(String),
    pipeline_id String,

    -- Performance metrics
    output_fps Float32,
    input_fps Float32,

    -- State info
    state LowCardinality(String),
    restart_count UInt32,
    last_error Nullable(String),

    -- Prompt info
    prompt_text Nullable(String),
    prompt_width UInt16,
    prompt_height UInt16,
    params_hash String,

    -- Metadata
    ingestion_timestamp DateTime DEFAULT now()
)
    ENGINE = MergeTree()
        PARTITION BY toYYYYMM(event_date)
        ORDER BY (event_date, orchestrator_address, pipeline, stream_id, event_timestamp)
        TTL event_date + INTERVAL 90 DAY DELETE
        SETTINGS index_granularity = 8192;

-- Table 2: Stream Ingest Metrics (network performance)
CREATE TABLE IF NOT EXISTS stream_ingest_metrics
(
    event_timestamp DateTime64(3, 'UTC'),
    event_date Date MATERIALIZED toDate(event_timestamp),

    -- Identifiers
    stream_id String,
    request_id String,
    gateway String,
    orchestrator_address Nullable(String),
    pipeline_id String,

    -- Connection quality
    connection_quality LowCardinality(String),

    -- Video metrics
    video_jitter Float32,
    video_packets_received UInt32,
    video_packets_lost UInt32,
    video_packet_loss_pct Float32,

    -- Audio metrics
    audio_jitter Float32,
    audio_packets_received UInt32,
    audio_packets_lost UInt32,
    audio_packet_loss_pct Float32,

    -- Peer connection stats
    bytes_received UInt64,
    bytes_sent UInt64,

    -- Metadata
    ingestion_timestamp DateTime DEFAULT now()
)
    ENGINE = MergeTree()
        PARTITION BY toYYYYMM(event_date)
        ORDER BY (event_date, stream_id, event_timestamp)
        TTL event_date + INTERVAL 90 DAY DELETE
        SETTINGS index_granularity = 8192;

-- Table 3: Stream Trace Events (for latency calculations)
CREATE TABLE IF NOT EXISTS stream_trace_events
(
    event_timestamp DateTime64(3, 'UTC'),
    event_date Date MATERIALIZED toDate(event_timestamp),

    -- Identifiers
    stream_id String,
    request_id String,
    gateway String,
    orchestrator_address String,
    orchestrator_url String,
    pipeline_id String,

    -- Trace type
    trace_type LowCardinality(String),

    -- Data timestamp
    data_timestamp DateTime64(3, 'UTC'),

    -- Metadata
    ingestion_timestamp DateTime DEFAULT now()
)
    ENGINE = MergeTree()
        PARTITION BY toYYYYMM(event_date)
        ORDER BY (event_date, request_id, trace_type, data_timestamp)
        TTL event_date + INTERVAL 90 DAY DELETE
        SETTINGS index_granularity = 8192;

-- Table 4: Calculated Metrics (Flink output)
CREATE TABLE IF NOT EXISTS calculated_metrics
(
    metric_timestamp DateTime64(3, 'UTC'),
    metric_date Date MATERIALIZED toDate(metric_timestamp),
    window_start DateTime64(3, 'UTC'),
    window_end DateTime64(3, 'UTC'),

    -- Identifiers
    stream_id String,
    request_id Nullable(String),
    gateway String,
    orchestrator_address String,
    gpu_id Nullable(String),
    region LowCardinality(Nullable(String)),

    -- Workflow info
    pipeline LowCardinality(String),
    model_id String,

    -- Metric info
    metric_name LowCardinality(String),
    metric_category LowCardinality(String),

    -- Metric value
    metric_value Float64,
    metric_unit LowCardinality(String),

    -- Context
    metric_metadata String,

    -- Metadata
    ingestion_timestamp DateTime DEFAULT now()
)
    ENGINE = MergeTree()
        PARTITION BY toYYYYMM(metric_date)
        ORDER BY (metric_date, metric_name, orchestrator_address, pipeline, metric_timestamp)
        TTL metric_date + INTERVAL 90 DAY DELETE
        SETTINGS index_granularity = 8192;

-- ============================================
-- NEW TABLES (Missing Event Types)
-- ============================================

-- Table 5: Network Capabilities (orchestrator metadata + GPU info)
-- This is the SOURCE for gpu_id and region enrichment
-- CRITICAL: One row per (orchestrator_address, orch_uri, model_id)
--           because each URI = different GPU
CREATE TABLE IF NOT EXISTS network_capabilities
(
    event_timestamp DateTime64(3, 'UTC'),
    event_date Date MATERIALIZED toDate(event_timestamp),

    -- Orchestrator info
    orchestrator_address String,
    local_address String,
    orch_uri String,  -- CRITICAL: Part of unique key (each URI = different GPU)

    -- GPU info (flattened from first GPU in hardware array)
    gpu_id Nullable(String),
    gpu_name Nullable(String),
    gpu_memory_total Nullable(UInt64),
    gpu_memory_free Nullable(UInt64),
    gpu_major Nullable(UInt8),

    -- Model/Pipeline info
    pipeline String,
    model_id String,
    runner_version Nullable(String),
    capacity Nullable(UInt8),
    capacity_in_use Nullable(UInt8),

    -- Pricing
    price_per_unit Nullable(UInt32),

    -- Version
    orchestrator_version String,

    -- Raw JSON for full hardware array
    raw_json String,

    -- Metadata
    ingestion_timestamp DateTime DEFAULT now()
)
    ENGINE = ReplacingMergeTree(event_timestamp)
        PARTITION BY toYYYYMM(event_date)
        ORDER BY (orchestrator_address, orch_uri, model_id, event_timestamp)  -- UPDATED: orch_uri now in key
        TTL event_date + INTERVAL 90 DAY DELETE
        SETTINGS index_granularity = 8192;

-- Table 6: AI Stream Events (errors and lifecycle events)
CREATE TABLE IF NOT EXISTS ai_stream_events
(
    event_timestamp DateTime64(3, 'UTC'),
    event_date Date MATERIALIZED toDate(event_timestamp),

    -- Identifiers
    stream_id String,
    request_id String,
    gateway String,
    pipeline String,
    pipeline_id String,

    -- Event info
    event_type LowCardinality(String),  -- 'error', 'warning', 'info'
    message String,
    capability String,

    -- Metadata
    ingestion_timestamp DateTime DEFAULT now()
)
    ENGINE = MergeTree()
        PARTITION BY toYYYYMM(event_date)
        ORDER BY (event_date, stream_id, event_timestamp)
        TTL event_date + INTERVAL 90 DAY DELETE
        SETTINGS index_granularity = 8192;

-- Table 7: Discovery Results (orchestrator discovery latency)
CREATE TABLE IF NOT EXISTS discovery_results
(
    event_timestamp DateTime64(3, 'UTC'),
    event_date Date MATERIALIZED toDate(event_timestamp),

    -- Discovered orchestrator
    orchestrator_address String,
    orchestrator_url String,
    latency_ms UInt32,

    -- Metadata
    gateway String,
    ingestion_timestamp DateTime DEFAULT now()
)
    ENGINE = MergeTree()
        PARTITION BY toYYYYMM(event_date)
        ORDER BY (event_date, orchestrator_address, event_timestamp)
        TTL event_date + INTERVAL 30 DAY DELETE
        SETTINGS index_granularity = 8192;

-- Table 8: Payment Events (economics tracking)
CREATE TABLE IF NOT EXISTS payment_events
(
    event_timestamp DateTime64(3, 'UTC'),
    event_date Date MATERIALIZED toDate(event_timestamp),

    -- Payment info
    request_id String,
    session_id String,
    manifest_id String,

    -- Addresses
    sender String,
    recipient String,
    orchestrator String,

    -- Payment details
    face_value String,  -- WEI amount as string
    price String,       -- wei/pixel as string
    num_tickets String,
    win_prob String,

    -- Metadata
    gateway String,
    client_ip String,
    capability String,

    ingestion_timestamp DateTime DEFAULT now()
)
    ENGINE = MergeTree()
        PARTITION BY toYYYYMM(event_date)
        ORDER BY (event_date, recipient, orchestrator, event_timestamp)
        TTL event_date + INTERVAL 90 DAY DELETE
        SETTINGS index_granularity = 8192;

-- ============================================
-- MATERIALIZED VIEWS
-- ============================================

-- MV 1: 5-minute FPS aggregates
CREATE MATERIALIZED VIEW IF NOT EXISTS ai_stream_status_5min_mv
            ENGINE = SummingMergeTree()
                PARTITION BY toYYYYMM(metric_date)
                ORDER BY (metric_date, orchestrator_address, pipeline, time_bucket)
AS
SELECT
    toStartOfInterval(event_timestamp, INTERVAL 5 MINUTE) as time_bucket,
    toDate(time_bucket) as metric_date,
    orchestrator_address,
    pipeline,
    gpu_id,
    region,
    avg(output_fps) as avg_output_fps,
    quantile(0.5)(output_fps) as median_output_fps,
    quantile(0.95)(output_fps) as p95_output_fps,
    avg(input_fps) as avg_input_fps,
    count() as sample_count,
    sum(restart_count) as total_restarts
FROM ai_stream_status
GROUP BY time_bucket, metric_date, orchestrator_address, pipeline, gpu_id, region;

-- MV 2: Latest Orchestrator Metadata (for lookups)
-- CRITICAL: Now includes orch_uri since each URI = different GPU
CREATE MATERIALIZED VIEW IF NOT EXISTS orchestrator_latest_metadata_mv
            ENGINE = ReplacingMergeTree(event_timestamp)
                ORDER BY (orchestrator_address, orch_uri, model_id)
AS
SELECT
    orchestrator_address,
    orch_uri,  -- ADDED: Essential for multi-URI orchestrators
    model_id,
    gpu_id,
    gpu_name,
    event_timestamp,
    argMax(orchestrator_version, event_timestamp) as latest_version,
    argMax(capacity, event_timestamp) as latest_capacity
FROM network_capabilities
GROUP BY orchestrator_address, orch_uri, model_id, gpu_id, gpu_name, event_timestamp;

-- ============================================
-- INDEXES FOR COMMON QUERIES
-- ============================================

ALTER TABLE ai_stream_status
    ADD INDEX IF NOT EXISTS idx_orchestrator orchestrator_address TYPE bloom_filter GRANULARITY 1;

ALTER TABLE ai_stream_status
    ADD INDEX IF NOT EXISTS idx_pipeline pipeline TYPE bloom_filter GRANULARITY 1;

ALTER TABLE stream_trace_events
    ADD INDEX IF NOT EXISTS idx_request_id request_id TYPE bloom_filter GRANULARITY 1;

ALTER TABLE network_capabilities
    ADD INDEX IF NOT EXISTS idx_orch_address orchestrator_address TYPE bloom_filter GRANULARITY 1;