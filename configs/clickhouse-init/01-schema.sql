-- ============================================
-- LIVEPEER ANALYTICS - COMPLETE SCHEMA (UPDATED)
-- ============================================

CREATE DATABASE IF NOT EXISTS livepeer_analytics;
USE livepeer_analytics;

-- ============================================
-- RAW EVENTS TABLE (Staging/Archive)
-- ============================================

-- Raw streaming events table (all events as-is from Kafka)
-- Schema designed to work with ClickHouse Kafka Connect sink
CREATE TABLE IF NOT EXISTS streaming_events
(
    -- These field names MUST match the JSON keys from Kafka exactly
    id String,              -- Maps to "id" in JSON
    type LowCardinality(String),  -- Maps to "type" in JSON
    timestamp UInt64,       -- Maps to "timestamp" in JSON (as milliseconds)
    gateway String,         -- Maps to "gateway" in JSON
    data String,            -- Maps to "data" in JSON (stored as JSON string)

    -- Computed/materialized columns for easier querying
    event_timestamp DateTime64(3, 'UTC') MATERIALIZED fromUnixTimestamp64Milli(timestamp),
    event_date Date MATERIALIZED toDate(event_timestamp),

    -- Metadata
    ingestion_timestamp DateTime DEFAULT now()
)
    ENGINE = MergeTree()
        PARTITION BY toYYYYMM(event_date)
        ORDER BY (event_date, type, event_timestamp)
        TTL event_date + INTERVAL 90 DAY DELETE
        SETTINGS index_granularity = 8192;

-- Index for fast event type lookups
ALTER TABLE streaming_events
    ADD INDEX IF NOT EXISTS idx_event_type type TYPE bloom_filter GRANULARITY 1;

-- ============================================
-- DEAD LETTER QUEUE (Failed Processing)
-- ============================================

CREATE TABLE IF NOT EXISTS streaming_events_dlq
(
    event_id String,
    event_type Nullable(String),
    event_timestamp DateTime64(3, 'UTC'),
    event_date Date MATERIALIZED toDate(event_timestamp),
    raw_json String,
    error_message String,
    error_type LowCardinality(String),

    ingestion_timestamp DateTime DEFAULT now()
)
    ENGINE = MergeTree()
        PARTITION BY toYYYYMM(event_date)
        ORDER BY (event_date, error_type, event_timestamp)
        TTL event_date + INTERVAL 30 DAY DELETE
        SETTINGS index_granularity = 8192;

-- ============================================
-- TYPED TABLES (Flattened Events)
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

    -- Orchestrator info
    orchestrator_address String,
    orchestrator_url String,

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

    -- Start time
    start_time DateTime64(3, 'UTC'),

    -- Raw JSON for debugging
    raw_json String,

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
    pipeline_id String,

    -- Connection quality
    connection_quality LowCardinality(String),

    -- Video metrics
    video_jitter Float32,
    video_packets_received UInt32,
    video_packets_lost UInt32,
    video_packet_loss_pct Float32,
    video_rtt Float32,
    video_last_input_ts Float32,
    video_latency Float32,

    -- Audio metrics
    audio_jitter Float32,
    audio_packets_received UInt32,
    audio_packets_lost UInt32,
    audio_packet_loss_pct Float32,
    audio_rtt Float32,
    audio_last_input_ts Float32,
    audio_latency Float32,

    -- Peer connection stats
    bytes_received UInt64,
    bytes_sent UInt64,

    -- Raw JSON for debugging
    raw_json String,

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
    pipeline_id String,
    orchestrator_address String,
    orchestrator_url String,

    -- Trace type
    trace_type LowCardinality(String),

    -- Data timestamp
    data_timestamp DateTime64(3, 'UTC'),

    -- Raw JSON for debugging
    raw_json String,

    -- Metadata
    ingestion_timestamp DateTime DEFAULT now()
)
    ENGINE = MergeTree()
        PARTITION BY toYYYYMM(event_date)
        ORDER BY (event_date, request_id, trace_type, data_timestamp)
        TTL event_date + INTERVAL 90 DAY DELETE
        SETTINGS index_granularity = 8192;

-- Table 4: Network Capabilities (orchestrator metadata + GPU info)
CREATE TABLE IF NOT EXISTS network_capabilities
(
    event_timestamp DateTime64(3, 'UTC'),
    event_date Date MATERIALIZED toDate(event_timestamp),

    -- Orchestrator info
    orchestrator_address String,
    local_address String,
    orch_uri String,

    -- GPU info (flattened from first GPU in hardware array)
    gpu_id Nullable(String),
    gpu_name Nullable(String),
    gpu_memory_total Nullable(UInt64),
    gpu_memory_free Nullable(UInt64),
    gpu_major Nullable(UInt8),
    gpu_minor Nullable(UInt8),

    -- Model/Pipeline info
    pipeline String,
    model_id String,
    runner_version Nullable(String),
    capacity Nullable(UInt8),
    capacity_in_use Nullable(UInt8),
    warm Nullable(UInt8),

    -- Pricing
    price_per_unit Nullable(UInt32),
    pixels_per_unit Nullable(UInt32),

    -- Version
    orchestrator_version String,

    -- Raw JSON for full hardware array and debugging
    raw_json String,

    -- Metadata
    ingestion_timestamp DateTime DEFAULT now()
)
    ENGINE = ReplacingMergeTree(event_timestamp)
        PARTITION BY toYYYYMM(event_date)
        ORDER BY (orchestrator_address, orch_uri, model_id, event_timestamp)
        TTL event_date + INTERVAL 90 DAY DELETE
        SETTINGS index_granularity = 8192;

-- Table 5: AI Stream Events (errors and lifecycle events)
CREATE TABLE IF NOT EXISTS ai_stream_events
(
    event_timestamp DateTime64(3, 'UTC'),
    event_date Date MATERIALIZED toDate(event_timestamp),

    -- Identifiers
    stream_id String,
    request_id String,
    pipeline String,
    pipeline_id String,

    -- Event info
    event_type LowCardinality(String),  -- 'error', 'warning', 'info'
    message String,
    capability String,

    -- Raw JSON for debugging
    raw_json String,

    -- Metadata
    ingestion_timestamp DateTime DEFAULT now()
)
    ENGINE = MergeTree()
        PARTITION BY toYYYYMM(event_date)
        ORDER BY (event_date, stream_id, event_timestamp)
        TTL event_date + INTERVAL 90 DAY DELETE
        SETTINGS index_granularity = 8192;

-- Table 6: Discovery Results (orchestrator discovery latency)
CREATE TABLE IF NOT EXISTS discovery_results
(
    event_timestamp DateTime64(3, 'UTC'),
    event_date Date MATERIALIZED toDate(event_timestamp),

    -- Discovered orchestrator
    orchestrator_address String,
    orchestrator_url String,
    latency_ms UInt32,

    -- Raw JSON for debugging
    raw_json String,

    -- Metadata
    ingestion_timestamp DateTime DEFAULT now()
)
    ENGINE = MergeTree()
        PARTITION BY toYYYYMM(event_date)
        ORDER BY (event_date, orchestrator_address, event_timestamp)
        TTL event_date + INTERVAL 30 DAY DELETE
        SETTINGS index_granularity = 8192;

-- Table 7: Payment Events (economics tracking)
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
    client_ip String,
    capability String,

    -- Raw JSON for debugging
    raw_json String,

    -- Metadata
    ingestion_timestamp DateTime DEFAULT now()
)
    ENGINE = MergeTree()
        PARTITION BY toYYYYMM(event_date)
        ORDER BY (event_date, recipient, orchestrator, event_timestamp)
        TTL event_date + INTERVAL 90 DAY DELETE
        SETTINGS index_granularity = 8192;

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