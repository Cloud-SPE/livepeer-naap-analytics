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
    ingestion_timestamp DateTime64(3, 'UTC') DEFAULT now64(3)
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
    schema_version LowCardinality(String),

    source_topic String,
    source_partition UInt32,
    source_offset UInt64,
    source_record_timestamp DateTime64(3, 'UTC'),

    event_id String,
    event_type LowCardinality(String),
    event_version Nullable(String),
    event_timestamp Nullable(DateTime64(3, 'UTC')),
    event_date Date MATERIALIZED toDate(ifNull(event_timestamp, source_record_timestamp)),

    dedup_key Nullable(String),
    dedup_strategy Nullable(String),
    replay UInt8,

    failure_stage LowCardinality(String),
    failure_class LowCardinality(String),
    failure_reason String,
    failure_details String,

    orchestrator Nullable(String),
    broadcaster Nullable(String),
    region Nullable(String),

    payload_encoding LowCardinality(String),
    payload_body String,
    payload_canonical_json Nullable(String),

    ingestion_timestamp DateTime64(3, 'UTC') DEFAULT now64(3)
)
    ENGINE = MergeTree()
        PARTITION BY toYYYYMM(event_date)
        ORDER BY (event_date, failure_class, event_type, source_topic, source_offset)
        TTL event_date + INTERVAL 30 DAY DELETE
        SETTINGS index_granularity = 8192;

-- ============================================
-- QUARANTINE (Expected Rejects / Duplicates)
-- ============================================

CREATE TABLE IF NOT EXISTS streaming_events_quarantine
(
    schema_version LowCardinality(String),

    source_topic String,
    source_partition UInt32,
    source_offset UInt64,
    source_record_timestamp DateTime64(3, 'UTC'),

    event_id String,
    event_type LowCardinality(String),
    event_version Nullable(String),
    event_timestamp Nullable(DateTime64(3, 'UTC')),
    event_date Date MATERIALIZED toDate(ifNull(event_timestamp, source_record_timestamp)),

    dedup_key Nullable(String),
    dedup_strategy Nullable(String),
    replay UInt8,

    failure_stage LowCardinality(String),
    failure_class LowCardinality(String),
    failure_reason String,
    failure_details String,

    orchestrator Nullable(String),
    broadcaster Nullable(String),
    region Nullable(String),

    payload_encoding LowCardinality(String),
    payload_body String,
    payload_canonical_json Nullable(String),

    ingestion_timestamp DateTime64(3, 'UTC') DEFAULT now64(3)
)
    ENGINE = MergeTree()
        PARTITION BY toYYYYMM(event_date)
        ORDER BY (event_date, failure_class, event_type, source_topic, source_offset)
        TTL event_date + INTERVAL 7 DAY DELETE
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
    ingestion_timestamp DateTime64(3, 'UTC') DEFAULT now64(3)
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
    ingestion_timestamp DateTime64(3, 'UTC') DEFAULT now64(3)
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
    ingestion_timestamp DateTime64(3, 'UTC') DEFAULT now64(3)
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
    source_event_id String,

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
    capability_id Nullable(Int32),
    capability_name Nullable(String),
    capability_group Nullable(String),
    capability_catalog_version Nullable(String),
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
    ingestion_timestamp DateTime64(3, 'UTC') DEFAULT now64(3)
)
    ENGINE = ReplacingMergeTree(event_timestamp)
        PARTITION BY toYYYYMM(event_date)
        ORDER BY (orchestrator_address, orch_uri, model_id, event_timestamp)
        TTL event_date + INTERVAL 90 DAY DELETE
        SETTINGS index_granularity = 8192;

-- Table 4b: Network Capability Advertised (one row per capability id advertised)
CREATE TABLE IF NOT EXISTS network_capabilities_advertised
(
    event_timestamp DateTime64(3, 'UTC'),
    event_date Date MATERIALIZED toDate(event_timestamp),
    source_event_id String,

    orchestrator_address String,
    local_address String,
    orch_uri String,

    capability_id Int32,
    capability_name LowCardinality(String),
    capability_group LowCardinality(String),
    capability_catalog_version LowCardinality(String),
    capacity Nullable(Int32),

    raw_json String,
    ingestion_timestamp DateTime64(3, 'UTC') DEFAULT now64(3)
)
    ENGINE = ReplacingMergeTree(event_timestamp)
        PARTITION BY toYYYYMM(event_date)
        ORDER BY (orchestrator_address, capability_id, event_timestamp, source_event_id)
        TTL event_date + INTERVAL 180 DAY DELETE
        SETTINGS index_granularity = 8192;

-- Table 4c: Network Capability Model Constraints (one row per capability/model)
CREATE TABLE IF NOT EXISTS network_capabilities_model_constraints
(
    event_timestamp DateTime64(3, 'UTC'),
    event_date Date MATERIALIZED toDate(event_timestamp),
    source_event_id String,

    orchestrator_address String,
    local_address String,
    orch_uri String,

    capability_id Int32,
    capability_name LowCardinality(String),
    capability_group LowCardinality(String),
    capability_catalog_version LowCardinality(String),

    model_id String,
    runner_version Nullable(String),
    capacity Nullable(Int32),
    capacity_in_use Nullable(Int32),
    warm Nullable(Int32),

    raw_json String,
    ingestion_timestamp DateTime64(3, 'UTC') DEFAULT now64(3)
)
    ENGINE = ReplacingMergeTree(event_timestamp)
        PARTITION BY toYYYYMM(event_date)
        ORDER BY (orchestrator_address, capability_id, model_id, event_timestamp, source_event_id)
        TTL event_date + INTERVAL 180 DAY DELETE
        SETTINGS index_granularity = 8192;

-- Table 4d: Network Capability Prices (one row per price entry)
CREATE TABLE IF NOT EXISTS network_capabilities_prices
(
    event_timestamp DateTime64(3, 'UTC'),
    event_date Date MATERIALIZED toDate(event_timestamp),
    source_event_id String,

    orchestrator_address String,
    local_address String,
    orch_uri String,

    capability_id Int32,
    capability_name LowCardinality(String),
    capability_group LowCardinality(String),
    capability_catalog_version LowCardinality(String),
    constraint_name Nullable(String),
    price_per_unit Nullable(Int32),
    pixels_per_unit Nullable(Int32),

    raw_json String,
    ingestion_timestamp DateTime64(3, 'UTC') DEFAULT now64(3)
)
    ENGINE = ReplacingMergeTree(event_timestamp)
        PARTITION BY toYYYYMM(event_date)
        ORDER BY (orchestrator_address, capability_id, event_timestamp, source_event_id)
        TTL event_date + INTERVAL 180 DAY DELETE
        SETTINGS index_granularity = 8192;

-- Silver Fact: Stream Status Samples
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

    state LowCardinality(String),
    output_fps Float32,
    input_fps Float32,
    attribution_method LowCardinality(String),
    attribution_confidence Float32,

    source_event_uid String,
    ingestion_timestamp DateTime64(3, 'UTC') DEFAULT now64(3)
)
    ENGINE = MergeTree()
        PARTITION BY toYYYYMM(sample_date)
        ORDER BY (sample_date, workflow_session_id, sample_ts)
        TTL sample_date + INTERVAL 180 DAY DELETE
        SETTINGS index_granularity = 8192;

-- Silver Fact: Stream Trace Edges
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
    ENGINE = MergeTree()
        PARTITION BY toYYYYMM(edge_date)
        ORDER BY (edge_date, workflow_session_id, edge_ts, trace_type)
        TTL edge_date + INTERVAL 180 DAY DELETE
        SETTINGS index_granularity = 8192;

-- Silver Fact: Stream Ingest Samples
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
    ENGINE = MergeTree()
        PARTITION BY toYYYYMM(sample_date)
        ORDER BY (sample_date, workflow_session_id, sample_ts)
        TTL sample_date + INTERVAL 90 DAY DELETE
        SETTINGS index_granularity = 8192;

-- Non-stateful silver fact materialization.
-- Rationale:
-- - These facts are direct per-event projections that do not require cross-event/session state.
-- - Keeping them in ClickHouse MVs reduces Flink mapper boilerplate while preserving deterministic logic.

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_ai_stream_status_to_fact_stream_status_samples
TO fact_stream_status_samples
AS
SELECT
    event_timestamp AS sample_ts,
    multiIf(
        stream_id != '' AND request_id != '', concat(stream_id, '|', request_id),
        stream_id != '', concat(stream_id, '|_missing_request'),
        request_id != '', concat('_missing_stream|', request_id),
        concat('_missing_stream|_missing_request|', toString(cityHash64(raw_json)))
    ) AS workflow_session_id,
    stream_id,
    request_id,
    gateway,
    orchestrator_address,
    orchestrator_url,
    pipeline,
    pipeline_id,
    CAST(NULL AS Nullable(String)) AS model_id,
    CAST(NULL AS Nullable(String)) AS gpu_id,
    CAST(NULL AS Nullable(String)) AS region,
    state,
    output_fps,
    input_fps,
    'none' AS attribution_method,
    toFloat32(0) AS attribution_confidence,
    toString(cityHash64(raw_json)) AS source_event_uid
FROM ai_stream_status;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_stream_trace_events_to_fact_stream_trace_edges
TO fact_stream_trace_edges
AS
SELECT
    data_timestamp AS edge_ts,
    multiIf(
        stream_id != '' AND request_id != '', concat(stream_id, '|', request_id),
        stream_id != '', concat(stream_id, '|_missing_request'),
        request_id != '', concat('_missing_stream|', request_id),
        concat('_missing_stream|_missing_request|', toString(cityHash64(raw_json)))
    ) AS workflow_session_id,
    stream_id,
    request_id,
    '' AS gateway,
    orchestrator_address,
    orchestrator_url,
    '' AS pipeline,
    pipeline_id,
    trace_type,
    multiIf(
        startsWith(trace_type, 'gateway_'), 'gateway',
        startsWith(trace_type, 'orchestrator_'), 'orchestrator',
        startsWith(trace_type, 'runner_'), 'runner',
        startsWith(trace_type, 'app_'), 'app',
        'other'
    ) AS trace_category,
    toUInt8(trace_type = 'orchestrator_swap') AS is_swap_event,
    toString(cityHash64(raw_json)) AS source_event_uid
FROM stream_trace_events;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_stream_ingest_metrics_to_fact_stream_ingest_samples
TO fact_stream_ingest_samples
AS
SELECT
    event_timestamp AS sample_ts,
    multiIf(
        stream_id != '' AND request_id != '', concat(stream_id, '|', request_id),
        stream_id != '', concat(stream_id, '|_missing_request'),
        request_id != '', concat('_missing_stream|', request_id),
        concat('_missing_stream|_missing_request|', toString(cityHash64(raw_json)))
    ) AS workflow_session_id,
    stream_id,
    request_id,
    pipeline_id,
    connection_quality,
    video_jitter,
    audio_jitter,
    video_latency,
    audio_latency,
    bytes_received,
    bytes_sent,
    toString(cityHash64(raw_json)) AS source_event_uid
FROM stream_ingest_metrics;

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
    ingestion_timestamp DateTime64(3, 'UTC') DEFAULT now64(3)
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
    ingestion_timestamp DateTime64(3, 'UTC') DEFAULT now64(3)
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
    ingestion_timestamp DateTime64(3, 'UTC') DEFAULT now64(3)
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



--jitter coefficient data
CREATE MATERIALIZED VIEW IF NOT EXISTS livepeer_analytics.mv_jitter_stats
            ENGINE = SummingMergeTree()
                ORDER BY (orchestrator_address, window_start)
AS SELECT
       toStartOfInterval(event_timestamp, INTERVAL 5 MINUTE) as window_start,
       orchestrator_address,
       avgState(output_fps) as avg_fps_state,
       stddevPopState(output_fps) as stddev_fps_state
   FROM livepeer_analytics.ai_stream_status
   GROUP BY window_start, orchestrator_address;

--back fill the jitter data
INSERT INTO livepeer_analytics.mv_jitter_stats
SELECT
    toStartOfInterval(event_timestamp, INTERVAL 5 MINUTE) as window_start,
    orchestrator_address,
    avgState(output_fps) as avg_fps_state,
    stddevPopState(output_fps) as stddev_fps_state
FROM livepeer_analytics.ai_stream_status
GROUP BY window_start, orchestrator_address;


SELECT
    -- Use the official URI from capabilities, fallback to the event URL
    coalesce(nullIf(nc.orch_uri, ''), s.orchestrator_url) AS display_uri,
    count(DISTINCT s.stream_id) AS active_streams,
    avg(s.output_fps) AS avg_fps,
    quantile(0.95)(s.output_fps) AS p95_fps,
    sum(s.restart_count) AS total_restarts
FROM livepeer_analytics.ai_stream_status AS s
         LEFT JOIN livepeer_analytics.network_capabilities AS nc
                   ON s.orchestrator_url = nc.orch_uri
WHERE s.event_timestamp >= now() - INTERVAL 24 HOUR
GROUP BY display_uri
ORDER BY active_streams DESC
LIMIT 10
