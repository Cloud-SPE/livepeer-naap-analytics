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

    -- Performance metrics
    output_fps Float32,
    input_fps Float32,

    -- State info
    state LowCardinality(String),
    restart_count UInt32,
    last_error Nullable(String),
    last_error_time Nullable(DateTime64(3, 'UTC')),

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
        -- Preserve per-GPU capability rows emitted for the same orchestrator/model event.
        ORDER BY (orchestrator_address, orch_uri, pipeline, model_id, ifNull(gpu_id, ''), ifNull(capability_id, 0), event_timestamp, source_event_id)
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
    model_id Nullable(String),
    gpu_id Nullable(String),
    region Nullable(String),

    state LowCardinality(String),
    output_fps Float32,
    input_fps Float32,
    gpu_attribution_method LowCardinality(String),
    gpu_attribution_confidence Float32,

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
    model_id Nullable(String),

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
WITH cap_lookup AS
(
    SELECT
        lower(local_address) AS hot_wallet,
        argMax(pipeline, event_timestamp) AS capability_pipeline,
        model_id AS capability_model_id,
        argMax(lower(orchestrator_address), event_timestamp) AS canonical_orchestrator_address,
        argMax(orch_uri, event_timestamp) AS canonical_orchestrator_url,
        argMax(gpu_id, event_timestamp) AS gpu_id,
        max(event_timestamp) AS latest_snapshot_ts
    FROM network_capabilities
    WHERE local_address != ''
      AND model_id != ''
    GROUP BY hot_wallet, capability_model_id
)
SELECT
    s.event_timestamp AS sample_ts,
    multiIf(
        s.stream_id != '' AND s.request_id != '', concat(s.stream_id, '|', s.request_id),
        s.stream_id != '', concat(s.stream_id, '|_missing_request'),
        s.request_id != '', concat('_missing_stream|', s.request_id),
        concat('_missing_stream|_missing_request|', toString(cityHash64(s.raw_json)))
    ) AS workflow_session_id,
    s.stream_id,
    s.request_id,
    s.gateway,
    ifNull(nullIf(canonical_orchestrator_address, ''), '') AS orchestrator_address,
    if(
        nullIf(canonical_orchestrator_url, '') IS NOT NULL,
        nullIf(canonical_orchestrator_url, ''),
        s.orchestrator_url
    ) AS orchestrator_url,
    if(
        nullIf(latest_snapshot_ts, toDateTime64(0, 3, 'UTC')) IS NOT NULL
            AND abs(dateDiff('millisecond', nullIf(latest_snapshot_ts, toDateTime64(0, 3, 'UTC')), s.event_timestamp)) <= 86400000,
        ifNull(nullIf(capability_pipeline, ''), ''),
        ''
    ) AS pipeline,
    nullIf(
        if(
            nullIf(latest_snapshot_ts, toDateTime64(0, 3, 'UTC')) IS NOT NULL
                AND abs(dateDiff('millisecond', nullIf(latest_snapshot_ts, toDateTime64(0, 3, 'UTC')), s.event_timestamp)) <= 86400000
                AND nullIf(capability_model_id, '') IS NOT NULL,
            capability_model_id,
            s.pipeline
        ),
        ''
    ) AS model_id,
    if(
        nullIf(latest_snapshot_ts, toDateTime64(0, 3, 'UTC')) IS NOT NULL
            AND abs(dateDiff('millisecond', nullIf(latest_snapshot_ts, toDateTime64(0, 3, 'UTC')), s.event_timestamp)) <= 86400000,
        nullIf(gpu_id, ''),
        CAST(NULL AS Nullable(String))
    ) AS gpu_id,
    CAST(NULL AS Nullable(String)) AS region,
    s.state,
    s.output_fps,
    s.input_fps,
    multiIf(
        nullIf(latest_snapshot_ts, toDateTime64(0, 3, 'UTC')) IS NULL, 'none',
        dateDiff('millisecond', nullIf(latest_snapshot_ts, toDateTime64(0, 3, 'UTC')), s.event_timestamp) = 0, 'exact_orchestrator_time_match',
        nullIf(latest_snapshot_ts, toDateTime64(0, 3, 'UTC')) <= s.event_timestamp
            AND abs(dateDiff('millisecond', nullIf(latest_snapshot_ts, toDateTime64(0, 3, 'UTC')), s.event_timestamp)) <= 86400000, 'nearest_prior_snapshot',
        abs(dateDiff('millisecond', nullIf(latest_snapshot_ts, toDateTime64(0, 3, 'UTC')), s.event_timestamp)) <= 86400000, 'nearest_snapshot_within_ttl',
        'proxy_address_join'
    ) AS gpu_attribution_method,
    multiIf(
        nullIf(latest_snapshot_ts, toDateTime64(0, 3, 'UTC')) IS NULL, toFloat32(0),
        dateDiff('millisecond', nullIf(latest_snapshot_ts, toDateTime64(0, 3, 'UTC')), s.event_timestamp) = 0, toFloat32(1.0),
        nullIf(latest_snapshot_ts, toDateTime64(0, 3, 'UTC')) <= s.event_timestamp
            AND abs(dateDiff('millisecond', nullIf(latest_snapshot_ts, toDateTime64(0, 3, 'UTC')), s.event_timestamp)) <= 86400000, toFloat32(0.9),
        abs(dateDiff('millisecond', nullIf(latest_snapshot_ts, toDateTime64(0, 3, 'UTC')), s.event_timestamp)) <= 86400000, toFloat32(0.7),
        toFloat32(0.4)
    ) AS gpu_attribution_confidence,
    toString(cityHash64(s.raw_json)) AS source_event_uid
FROM ai_stream_status s
LEFT JOIN cap_lookup c
  ON lower(s.orchestrator_address) = c.hot_wallet
 AND ifNull(nullIf(s.pipeline, ''), '') = ifNull(nullIf(c.capability_model_id, ''), '');

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_stream_trace_events_to_fact_stream_trace_edges
TO fact_stream_trace_edges
AS
WITH cap_lookup AS
(
    SELECT
        lower(local_address) AS hot_wallet,
        argMax(lower(orchestrator_address), event_timestamp) AS canonical_orchestrator_address,
        argMax(pipeline, event_timestamp) AS capability_pipeline,
        argMax(model_id, event_timestamp) AS capability_model_id
    FROM network_capabilities
    WHERE local_address != ''
      AND orchestrator_address != ''
    GROUP BY hot_wallet
)
SELECT
    t.data_timestamp AS edge_ts,
    multiIf(
        t.stream_id != '' AND t.request_id != '', concat(t.stream_id, '|', t.request_id),
        t.stream_id != '', concat(t.stream_id, '|_missing_request'),
        t.request_id != '', concat('_missing_stream|', t.request_id),
        concat('_missing_stream|_missing_request|', toString(cityHash64(t.raw_json)))
    ) AS workflow_session_id,
    t.stream_id,
    t.request_id,
    '' AS gateway,
    ifNull(nullIf(c.canonical_orchestrator_address, ''), '') AS orchestrator_address,
    t.orchestrator_url,
    ifNull(nullIf(c.capability_pipeline, ''), '') AS pipeline,
    nullIf(c.capability_model_id, '') AS model_id,
    t.trace_type,
    multiIf(
        startsWith(t.trace_type, 'gateway_'), 'gateway',
        startsWith(t.trace_type, 'orchestrator_'), 'orchestrator',
        startsWith(t.trace_type, 'runner_'), 'runner',
        startsWith(t.trace_type, 'app_'), 'app',
        'other'
    ) AS trace_category,
    toUInt8(t.trace_type = 'orchestrator_swap') AS is_swap_event,
    toString(cityHash64(t.raw_json)) AS source_event_uid
FROM stream_trace_events t
LEFT JOIN cap_lookup c
  ON lower(t.orchestrator_address) = c.hot_wallet;

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
    connection_quality,
    video_jitter,
    audio_jitter,
    video_latency,
    audio_latency,
    bytes_received,
    bytes_sent,
    toString(cityHash64(raw_json)) AS source_event_uid
FROM stream_ingest_metrics;

-- Silver Fact: Workflow Sessions (stateful, Flink-generated)
CREATE TABLE IF NOT EXISTS fact_workflow_sessions
(
    workflow_session_id String,
    workflow_type LowCardinality(String),
    workflow_id String,

    stream_id String,
    request_id String,
    session_id String,
    pipeline String,

    gateway String,
    orchestrator_address String,
    orchestrator_url String,
    model_id Nullable(String),
    gpu_id Nullable(String),
    region Nullable(String),
    gpu_attribution_method LowCardinality(String),
    gpu_attribution_confidence Float32,

    session_start_ts DateTime64(3, 'UTC'),
    session_end_ts Nullable(DateTime64(3, 'UTC')),

    known_stream UInt8,
    startup_success UInt8,
    startup_excused UInt8,
    startup_unexcused UInt8,

    confirmed_swap_count UInt16,
    inferred_orchestrator_change_count UInt16,
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

-- Silver Fact: Workflow Session Segments (stateful, Flink-generated)
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
    gpu_attribution_method LowCardinality(String),
    gpu_attribution_confidence Float32,

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

-- Silver Fact: Workflow parameter update markers (stateful, Flink-generated)
CREATE TABLE IF NOT EXISTS fact_workflow_param_updates
(
    update_ts DateTime64(3, 'UTC'),
    update_date Date MATERIALIZED toDate(update_ts),

    workflow_session_id String,
    stream_id String,
    request_id String,
    pipeline String,

    gateway String,
    orchestrator_address String,
    orchestrator_url String,
    model_id Nullable(String),
    gpu_id Nullable(String),
    gpu_attribution_method LowCardinality(String),
    gpu_attribution_confidence Float32,

    update_type LowCardinality(String),
    message String,
    source_event_uid String,
    version UInt64,

    ingestion_timestamp DateTime64(3, 'UTC') DEFAULT now64(3)
)
    ENGINE = ReplacingMergeTree(version)
        PARTITION BY toYYYYMM(update_date)
        ORDER BY (update_date, workflow_session_id, update_ts, source_event_uid)
        TTL update_date + INTERVAL 180 DAY DELETE
        SETTINGS index_granularity = 8192;

-- Lifecycle edge coverage diagnostics (signal grain)
CREATE TABLE IF NOT EXISTS fact_lifecycle_edge_coverage
(
    signal_ts DateTime64(3, 'UTC'),
    signal_date Date MATERIALIZED toDate(signal_ts),

    workflow_session_id String,
    stream_id String,
    request_id String,
    pipeline String,
    model_id Nullable(String),
    gateway String,
    orchestrator_address String,
    trace_type LowCardinality(String),
    source_event_uid String,

    known_stream UInt8,
    has_first_processed_edge UInt8,
    has_first_playable_edge UInt8,
    startup_edge_matched UInt8,
    playable_edge_matched UInt8,
    is_terminal_signal UInt8,
    unmatched_reason LowCardinality(String),
    version UInt64,

    ingestion_timestamp DateTime64(3, 'UTC') DEFAULT now64(3)
)
    ENGINE = ReplacingMergeTree(version)
        PARTITION BY toYYYYMM(signal_date)
        ORDER BY (signal_date, workflow_session_id, signal_ts, source_event_uid)
        TTL signal_date + INTERVAL 180 DAY DELETE
        SETTINGS index_granularity = 8192;

-- Derived latency KPIs at session snapshot grain (Flink-owned edge semantics).
CREATE TABLE IF NOT EXISTS fact_workflow_latency_samples
(
    sample_ts DateTime64(3, 'UTC'),
    sample_date Date MATERIALIZED toDate(sample_ts),

    workflow_session_id String,
    stream_id String,
    request_id String,
    gateway String,
    orchestrator_address String,
    pipeline String,
    model_id Nullable(String),
    gpu_id Nullable(String),
    region Nullable(String),

    prompt_to_first_frame_ms Nullable(Float64),
    startup_time_ms Nullable(Float64),
    e2e_latency_ms Nullable(Float64),

    has_prompt_to_first_frame UInt8,
    has_startup_time UInt8,
    has_e2e_latency UInt8,
    edge_semantics_version LowCardinality(String),
    version UInt64,

    ingestion_timestamp DateTime64(3, 'UTC') DEFAULT now64(3)
)
    ENGINE = ReplacingMergeTree(version)
        PARTITION BY toYYYYMM(sample_date)
        ORDER BY (sample_date, workflow_session_id, sample_ts)
        TTL sample_date + INTERVAL 180 DAY DELETE
        SETTINGS index_granularity = 8192;

-- ============================================================
-- DIMENSIONS (METRICS SERVING)
-- ============================================================

-- Grain: 1 row per orchestrator + workflow/model capability snapshot timestamp.
CREATE TABLE IF NOT EXISTS dim_orchestrator_capability_snapshots
(
    snapshot_ts DateTime64(3, 'UTC'),
    snapshot_date Date MATERIALIZED toDate(snapshot_ts),

    orchestrator_address String,
    orchestrator_proxy_address String,
    orchestrator_url String,

    pipeline String,
    model_id String,
    capability_id Int32,
    capability_name LowCardinality(String),
    capability_group LowCardinality(String),

    gpu_id Nullable(String),
    gpu_name Nullable(String),
    gpu_memory_total Nullable(UInt64),
    gpu_major Nullable(UInt8),
    gpu_minor Nullable(UInt8),
    runner_version Nullable(String),

    region Nullable(String),

    source_event_uid String,
    ingestion_timestamp DateTime64(3, 'UTC') DEFAULT now64(3)
)
    ENGINE = ReplacingMergeTree(snapshot_ts)
        PARTITION BY toYYYYMM(snapshot_date)
        -- Preserve per-GPU snapshot rows; do not collapse multiple GPUs that share
        -- orchestrator/model/timestamp in the same capability event.
        ORDER BY (orchestrator_address, pipeline, model_id, capability_id, ifNull(gpu_id, ''), snapshot_ts, source_event_uid)
        TTL snapshot_date + INTERVAL 365 DAY DELETE
        SETTINGS index_granularity = 8192;

-- Grain: 1 row per capability snapshot event x capability id.
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

-- Grain: 1 row per capability snapshot event x capability id x model id.
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

-- Grain: 1 row per capability snapshot event x capability id x constraint.
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

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_network_capabilities_to_dim_orchestrator_capability_snapshots
TO dim_orchestrator_capability_snapshots
AS
SELECT
    event_timestamp AS snapshot_ts,
    lower(orchestrator_address) AS orchestrator_address,
    lower(local_address) AS orchestrator_proxy_address,
    orch_uri AS orchestrator_url,
    pipeline,
    model_id,
    ifNull(capability_id, 0) AS capability_id,
    ifNull(capability_name, 'unknown') AS capability_name,
    ifNull(capability_group, 'unknown') AS capability_group,
    gpu_id,
    gpu_name,
    gpu_memory_total,
    gpu_major,
    gpu_minor,
    runner_version,
    CAST(NULL AS Nullable(String)) AS region,
    source_event_id AS source_event_uid
FROM network_capabilities;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_network_capabilities_advertised_to_dim_orchestrator_capability_advertised_snapshots
TO dim_orchestrator_capability_advertised_snapshots
AS
SELECT
    event_timestamp AS snapshot_ts,
    source_event_id AS source_event_uid,
    lower(orchestrator_address) AS orchestrator_address,
    lower(local_address) AS orchestrator_proxy_address,
    orch_uri AS orchestrator_url,
    capability_id,
    capability_name,
    capability_group,
    capacity,
    capability_catalog_version
FROM network_capabilities_advertised;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_network_capabilities_model_constraints_to_dim_orchestrator_capability_model_constraints
TO dim_orchestrator_capability_model_constraints
AS
SELECT
    event_timestamp AS snapshot_ts,
    source_event_id AS source_event_uid,
    lower(orchestrator_address) AS orchestrator_address,
    lower(local_address) AS orchestrator_proxy_address,
    orch_uri AS orchestrator_url,
    capability_id,
    capability_name,
    capability_group,
    capability_catalog_version,
    model_id,
    runner_version,
    capacity,
    capacity_in_use,
    warm
FROM network_capabilities_model_constraints;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_network_capabilities_prices_to_dim_orchestrator_capability_prices
TO dim_orchestrator_capability_prices
AS
SELECT
    event_timestamp AS snapshot_ts,
    source_event_id AS source_event_uid,
    lower(orchestrator_address) AS orchestrator_address,
    lower(local_address) AS orchestrator_proxy_address,
    orch_uri AS orchestrator_url,
    capability_id,
    capability_name,
    capability_group,
    capability_catalog_version,
    constraint_name,
    price_per_unit,
    pixels_per_unit
FROM network_capabilities_prices;

-- Derived current dimension (latest snapshot by orchestrator + workflow + model + GPU).
CREATE OR REPLACE VIEW dim_orchestrator_capability_current AS
SELECT
    orchestrator_address,
    argMax(orchestrator_proxy_address, snapshot_ts) AS orchestrator_proxy_address,
    argMax(orchestrator_url, snapshot_ts) AS orchestrator_url,
    pipeline,
    model_id,
    capability_id,
    argMax(capability_name, snapshot_ts) AS capability_name,
    argMax(capability_group, snapshot_ts) AS capability_group,
    gpu_id,
    argMax(gpu_name, snapshot_ts) AS gpu_name,
    argMax(gpu_memory_total, snapshot_ts) AS gpu_memory_total,
    argMax(gpu_major, snapshot_ts) AS gpu_major,
    argMax(gpu_minor, snapshot_ts) AS gpu_minor,
    argMax(runner_version, snapshot_ts) AS runner_version,
    argMax(region, snapshot_ts) AS region,
    max(snapshot_ts) AS latest_snapshot_ts
FROM dim_orchestrator_capability_snapshots
GROUP BY
    orchestrator_address,
    pipeline,
    model_id,
    capability_id,
    gpu_id;

-- Future fact for true orchestrator transport bandwidth telemetry.
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
-- ROLLUP TABLES
-- ============================================================

CREATE TABLE IF NOT EXISTS agg_stream_performance_1m
(
    window_start DateTime64(3, 'UTC'),

    gateway String,
    orchestrator_address String,
    pipeline String,
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
        ORDER BY (window_start, orchestrator_address, ifNull(gpu_id, ''))
        TTL toDate(window_start) + INTERVAL 365 DAY DELETE
        SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS agg_reliability_1h
(
    window_start DateTime64(3, 'UTC'),

    gateway String,
    orchestrator_address String,
    pipeline String,
    model_id Nullable(String),
    gpu_id Nullable(String),
    region Nullable(String),

    known_sessions_state AggregateFunction(sum, UInt64),
    startup_success_sessions_state AggregateFunction(sum, UInt64),
    excused_sessions_state AggregateFunction(sum, UInt64),
    unexcused_sessions_state AggregateFunction(sum, UInt64),
    confirmed_swapped_sessions_state AggregateFunction(sum, UInt64),
    inferred_orchestrator_change_sessions_state AggregateFunction(sum, UInt64),
    swapped_sessions_state AggregateFunction(sum, UInt64)
)
    ENGINE = AggregatingMergeTree
        PARTITION BY toYYYYMM(toDate(window_start))
        ORDER BY (window_start, gateway, orchestrator_address)
        TTL toDate(window_start) + INTERVAL 400 DAY DELETE
        SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS agg_latency_kpis_1m
(
    window_start DateTime64(3, 'UTC'),

    orchestrator_address String,
    pipeline String,
    model_id Nullable(String),
    gpu_id Nullable(String),
    region Nullable(String),

    prompt_to_first_frame_ms_avg_state AggregateFunction(avg, Float64),
    startup_time_ms_avg_state AggregateFunction(avg, Float64),
    e2e_latency_ms_avg_state AggregateFunction(avg, Float64),

    prompt_to_first_frame_ms_p95_state AggregateFunction(quantileTDigest(0.95), Float64),
    startup_time_ms_p95_state AggregateFunction(quantileTDigest(0.95), Float64),
    e2e_latency_ms_p95_state AggregateFunction(quantileTDigest(0.95), Float64),

    valid_prompt_to_first_frame_state AggregateFunction(sum, UInt64),
    valid_startup_time_state AggregateFunction(sum, UInt64),
    valid_e2e_latency_state AggregateFunction(sum, UInt64)
)
    ENGINE = AggregatingMergeTree
        PARTITION BY toYYYYMM(toDate(window_start))
        ORDER BY (window_start, orchestrator_address, ifNull(gpu_id, ''))
        TTL toDate(window_start) + INTERVAL 365 DAY DELETE
        SETTINGS index_granularity = 8192;

-- ============================================================
-- ROLLUP MATERIALIZED VIEWS
-- ============================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_fact_status_to_perf_1m
TO agg_stream_performance_1m
AS
SELECT
    toStartOfInterval(sample_ts, INTERVAL 1 MINUTE) AS window_start,
    gateway,
    orchestrator_address,
    pipeline,
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
    model_id,
    gpu_id,
    region,

    sumState(toUInt64(known_stream)) AS known_sessions_state,
    sumState(toUInt64(startup_success)) AS startup_success_sessions_state,
    sumState(toUInt64(startup_excused)) AS excused_sessions_state,
    sumState(toUInt64(startup_unexcused)) AS unexcused_sessions_state,
    sumState(toUInt64(confirmed_swap_count > 0)) AS confirmed_swapped_sessions_state,
    sumState(toUInt64(inferred_orchestrator_change_count > 0)) AS inferred_orchestrator_change_sessions_state,
    sumState(toUInt64((confirmed_swap_count > 0) OR (inferred_orchestrator_change_count > 0))) AS swapped_sessions_state
FROM fact_workflow_sessions
GROUP BY
    window_start,
    gateway,
    orchestrator_address,
    pipeline,
    model_id,
    gpu_id,
    region;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_fact_workflow_latency_to_latency_kpis_1m
TO agg_latency_kpis_1m
AS
SELECT
    toStartOfInterval(sample_ts, INTERVAL 1 MINUTE) AS window_start,
    orchestrator_address,
    pipeline,
    model_id,
    gpu_id,
    region,

    avgStateIf(prompt_to_first_frame_ms, has_prompt_to_first_frame = 1) AS prompt_to_first_frame_ms_avg_state,
    avgStateIf(startup_time_ms, has_startup_time = 1) AS startup_time_ms_avg_state,
    avgStateIf(e2e_latency_ms, has_e2e_latency = 1) AS e2e_latency_ms_avg_state,

    quantileTDigestStateIf(0.95)(prompt_to_first_frame_ms, has_prompt_to_first_frame = 1) AS prompt_to_first_frame_ms_p95_state,
    quantileTDigestStateIf(0.95)(startup_time_ms, has_startup_time = 1) AS startup_time_ms_p95_state,
    quantileTDigestStateIf(0.95)(e2e_latency_ms, has_e2e_latency = 1) AS e2e_latency_ms_p95_state,

    sumState(toUInt64(has_prompt_to_first_frame)) AS valid_prompt_to_first_frame_state,
    sumState(toUInt64(has_startup_time)) AS valid_startup_time_state,
    sumState(toUInt64(has_e2e_latency)) AS valid_e2e_latency_state
FROM fact_workflow_latency_samples
GROUP BY
    window_start,
    orchestrator_address,
    pipeline,
    model_id,
    gpu_id,
    region;

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

-- ============================================================
-- API VIEWS
-- ============================================================

CREATE OR REPLACE VIEW v_api_gpu_metrics AS
-- Grain: 1 row per (hour, orchestrator, pipeline, model_id, gpu_id, region).
-- Purpose: primary serving view for /gpu/metrics.
WITH latest_session_keys AS
(
    -- Session-level fallback keys used when status samples are missing model/gpu/region.
    SELECT
        workflow_session_id,
        argMax(orchestrator_address, version) AS orchestrator_address,
        argMax(pipeline, version) AS pipeline,
        argMax(model_id, version) AS model_id,
        argMax(gpu_id, version) AS gpu_id,
        argMax(region, version) AS region
    FROM fact_workflow_sessions
    GROUP BY workflow_session_id
),
perf_1h AS
(
    -- Performance KPIs from status samples at 1-hour grain.
    SELECT
        toStartOfInterval(s.sample_ts, INTERVAL 1 HOUR) AS window_start,
        if(s.orchestrator_address != '', s.orchestrator_address, ifNull(k.orchestrator_address, '')) AS orchestrator_address,
        if(s.pipeline != '', s.pipeline, ifNull(k.pipeline, '')) AS pipeline,
        nullIf(
            if(
                ifNull(s.model_id, '') != '',
                s.model_id,
                ifNull(k.model_id, '')
            ),
            ''
        ) AS model_id,
        nullIf(if(ifNull(s.gpu_id, '') != '', s.gpu_id, ifNull(k.gpu_id, '')), '') AS gpu_id,
        nullIf(if(ifNull(s.region, '') != '', s.region, ifNull(k.region, '')), '') AS region,
        avg(s.output_fps) AS avg_output_fps,
        quantileTDigest(0.95)(s.output_fps) AS p95_output_fps,
        stddevPop(s.output_fps) / nullIf(avg(s.output_fps), 0) AS jitter_coeff_fps,
        count() AS status_samples
    FROM fact_stream_status_samples s
    LEFT JOIN latest_session_keys k
        ON k.workflow_session_id = s.workflow_session_id
    GROUP BY
        window_start,
        orchestrator_address,
        pipeline,
        model_id,
        gpu_id,
        region
),
rel_1h AS
(
    -- Reliability/session outcomes at 1-hour grain.
    -- Kept local (instead of referencing v_api_sla_compliance) so this view can be created
    -- regardless of statement order during schema bootstrap.
    WITH latest_sessions AS
    (
        SELECT
            workflow_session_id,
            argMax(session_start_ts, version) AS session_start_ts,
            argMax(orchestrator_address, version) AS orchestrator_address,
            argMax(pipeline, version) AS pipeline,
            argMax(model_id, version) AS model_id,
            argMax(gpu_id, version) AS gpu_id,
            argMax(region, version) AS region,
            argMax(known_stream, version) AS known_stream,
            argMax(startup_success, version) AS startup_success,
            argMax(startup_excused, version) AS startup_excused,
            argMax(startup_unexcused, version) AS startup_unexcused,
            argMax(confirmed_swap_count, version) AS confirmed_swap_count,
            argMax(inferred_orchestrator_change_count, version) AS inferred_orchestrator_change_count,
            argMax(swap_count, version) AS swap_count
        FROM fact_workflow_sessions
        GROUP BY workflow_session_id
    )
    SELECT
        toStartOfInterval(session_start_ts, INTERVAL 1 HOUR) AS rel_window_start,
        orchestrator_address,
        pipeline,
        model_id,
        gpu_id,
        region,
        sum(toUInt64(known_stream)) AS known_sessions,
        sum(toUInt64(startup_success)) AS startup_success_sessions,
        sum(toUInt64(startup_excused)) AS excused_sessions,
        sum(toUInt64(startup_unexcused)) AS unexcused_sessions,
        sum(toUInt64(confirmed_swap_count > 0)) AS confirmed_swapped_sessions,
        sum(toUInt64(inferred_orchestrator_change_count > 0)) AS inferred_orchestrator_change_sessions,
        sum(toUInt64((confirmed_swap_count > 0) OR (inferred_orchestrator_change_count > 0))) AS swapped_sessions
    FROM latest_sessions
    WHERE session_start_ts > toDateTime64('2000-01-01 00:00:00', 3, 'UTC')
    GROUP BY
        rel_window_start,
        orchestrator_address,
        pipeline,
        model_id,
        gpu_id,
        region
),
lat_1h AS
(
    -- Latency KPIs from latest version per session to avoid duplicate aggregate state accumulation.
    WITH latest_latency_sessions AS
    (
        SELECT
            workflow_session_id,
            argMax(sample_ts, version) AS sample_ts,
            argMax(orchestrator_address, version) AS orchestrator_address,
            argMax(pipeline, version) AS pipeline,
            argMax(model_id, version) AS model_id,
            argMax(gpu_id, version) AS gpu_id,
            argMax(region, version) AS region,
            argMax(prompt_to_first_frame_ms, version) AS prompt_to_first_frame_ms,
            argMax(startup_time_ms, version) AS startup_time_ms,
            argMax(e2e_latency_ms, version) AS e2e_latency_ms,
            argMax(has_prompt_to_first_frame, version) AS has_prompt_to_first_frame,
            argMax(has_startup_time, version) AS has_startup_time,
            argMax(has_e2e_latency, version) AS has_e2e_latency
        FROM fact_workflow_latency_samples
        GROUP BY workflow_session_id
    )
    SELECT
        toStartOfInterval(l.sample_ts, INTERVAL 1 HOUR) AS window_start,
        if(ifNull(l.orchestrator_address, '') != '', l.orchestrator_address, ifNull(k.orchestrator_address, '')) AS orchestrator_address,
        if(ifNull(l.pipeline, '') != '', l.pipeline, ifNull(k.pipeline, '')) AS pipeline,
        nullIf(
            if(
                ifNull(l.model_id, '') != '',
                l.model_id,
                ifNull(k.model_id, '')
            ),
            ''
        ) AS model_id,
        nullIf(if(ifNull(l.gpu_id, '') != '', l.gpu_id, ifNull(k.gpu_id, '')), '') AS gpu_id,
        nullIf(if(ifNull(l.region, '') != '', l.region, ifNull(k.region, '')), '') AS region,
        avgIf(l.prompt_to_first_frame_ms, l.has_prompt_to_first_frame = 1) AS prompt_to_first_frame_ms,
        avgIf(l.startup_time_ms, l.has_startup_time = 1) AS startup_time_ms,
        avgIf(l.e2e_latency_ms, l.has_e2e_latency = 1) AS e2e_latency_ms,
        quantileTDigestIf(0.95)(l.prompt_to_first_frame_ms, l.has_prompt_to_first_frame = 1) AS p95_prompt_to_first_frame_ms,
        quantileTDigestIf(0.95)(l.startup_time_ms, l.has_startup_time = 1) AS p95_startup_time_ms,
        quantileTDigestIf(0.95)(l.e2e_latency_ms, l.has_e2e_latency = 1) AS p95_e2e_latency_ms,
        sum(toUInt64(l.has_prompt_to_first_frame = 1)) AS valid_prompt_to_first_frame_count,
        sum(toUInt64(l.has_startup_time = 1)) AS valid_startup_time_count,
        sum(toUInt64(l.has_e2e_latency = 1)) AS valid_e2e_latency_count
    FROM latest_latency_sessions l
    LEFT JOIN latest_session_keys k
        ON k.workflow_session_id = l.workflow_session_id
    GROUP BY
        window_start,
        orchestrator_address,
        pipeline,
        model_id,
        gpu_id,
        region
),
base_keys AS
(
    -- Union of keyspaces ensures latency-only windows are not dropped when perf data is sparse.
    SELECT
        window_start,
        orchestrator_address,
        pipeline,
        model_id,
        gpu_id,
        region
    FROM perf_1h
    UNION DISTINCT
    SELECT
        window_start,
        orchestrator_address,
        pipeline,
        model_id,
        gpu_id,
        region
    FROM lat_1h
),
dim_current AS
(
    -- Latest descriptive GPU metadata for serving responses.
    SELECT
        orchestrator_address,
        model_id,
        gpu_id,
        argMax(gpu_name, latest_snapshot_ts) AS gpu_name,
        argMax(gpu_memory_total, latest_snapshot_ts) AS gpu_memory_total,
        argMax(gpu_major, latest_snapshot_ts) AS gpu_major,
        argMax(gpu_minor, latest_snapshot_ts) AS gpu_minor,
        argMax(runner_version, latest_snapshot_ts) AS runner_version
    FROM dim_orchestrator_capability_current
    GROUP BY orchestrator_address, model_id, gpu_id
)
SELECT
    -- Primary serving keys
    b.window_start AS window_start,
    b.orchestrator_address AS orchestrator_address,
    b.pipeline AS pipeline,
    b.model_id AS model_id,
    b.gpu_id AS gpu_id,
    b.region AS region,

    d.gpu_name,
    d.gpu_memory_total,
    d.runner_version,
    if(d.gpu_major IS NOT NULL AND d.gpu_minor IS NOT NULL, concat(toString(d.gpu_major), '.', toString(d.gpu_minor)), CAST(NULL AS Nullable(String))) AS cuda_version,

    p.avg_output_fps,
    p.p95_output_fps,
    p.jitter_coeff_fps,
    ifNull(p.status_samples, toUInt64(0)) AS status_samples,
    if(l.valid_prompt_to_first_frame_count > 0, l.prompt_to_first_frame_ms, CAST(NULL AS Nullable(Float64))) AS prompt_to_first_frame_ms,
    if(l.valid_startup_time_count > 0, l.startup_time_ms, CAST(NULL AS Nullable(Float64))) AS startup_time_ms,
    if(l.valid_startup_time_count > 0, l.startup_time_ms / 1000.0, CAST(NULL AS Nullable(Float64))) AS startup_time_s,
    if(l.valid_e2e_latency_count > 0, l.e2e_latency_ms, CAST(NULL AS Nullable(Float64))) AS e2e_latency_ms,
    if(l.valid_prompt_to_first_frame_count > 0, l.p95_prompt_to_first_frame_ms, CAST(NULL AS Nullable(Float32))) AS p95_prompt_to_first_frame_ms,
    if(l.valid_startup_time_count > 0, l.p95_startup_time_ms, CAST(NULL AS Nullable(Float32))) AS p95_startup_time_ms,
    if(l.valid_e2e_latency_count > 0, l.p95_e2e_latency_ms, CAST(NULL AS Nullable(Float32))) AS p95_e2e_latency_ms,
    ifNull(l.valid_prompt_to_first_frame_count, toUInt64(0)) AS valid_prompt_to_first_frame_count,
    ifNull(l.valid_startup_time_count, toUInt64(0)) AS valid_startup_time_count,
    ifNull(l.valid_e2e_latency_count, toUInt64(0)) AS valid_e2e_latency_count,

    ifNull(r.known_sessions, toUInt64(0)) AS known_sessions,
    ifNull(r.startup_success_sessions, toUInt64(0)) AS startup_success_sessions,
    ifNull(r.excused_sessions, toUInt64(0)) AS excused_sessions,
    ifNull(r.unexcused_sessions, toUInt64(0)) AS unexcused_sessions,
    ifNull(r.confirmed_swapped_sessions, toUInt64(0)) AS confirmed_swapped_sessions,
    ifNull(r.inferred_orchestrator_change_sessions, toUInt64(0)) AS inferred_orchestrator_change_sessions,
    ifNull(r.swapped_sessions, toUInt64(0)) AS swapped_sessions,

    -- Derived rates for API response convenience.
    ifNull(r.unexcused_sessions / nullIf(r.known_sessions, 0), 0) AS failure_rate,
    ifNull(r.swapped_sessions / nullIf(r.known_sessions, 0), 0) AS swap_rate
FROM perf_1h p
RIGHT JOIN base_keys b
    -- RIGHT JOIN keeps all key combinations from base_keys (perf-only and latency-only windows).
    ON b.window_start = p.window_start
   AND b.orchestrator_address = p.orchestrator_address
   AND b.pipeline = p.pipeline
   AND ifNull(b.model_id, '') = ifNull(p.model_id, '')
   AND ifNull(b.gpu_id, '') = ifNull(p.gpu_id, '')
   AND ifNull(b.region, '') = ifNull(p.region, '')
LEFT JOIN rel_1h r
    ON r.rel_window_start = b.window_start
   AND r.orchestrator_address = b.orchestrator_address
   AND r.pipeline = b.pipeline
   AND ifNull(r.model_id, '') = ifNull(b.model_id, '')
   AND ifNull(r.gpu_id, '') = ifNull(b.gpu_id, '')
   AND ifNull(r.region, '') = ifNull(b.region, '')
LEFT JOIN lat_1h l
    ON l.window_start = b.window_start
   AND l.orchestrator_address = b.orchestrator_address
   AND l.pipeline = b.pipeline
   AND ifNull(l.model_id, '') = ifNull(b.model_id, '')
   AND ifNull(l.gpu_id, '') = ifNull(b.gpu_id, '')
   AND ifNull(l.region, '') = ifNull(b.region, '')
LEFT JOIN dim_current d
    ON d.orchestrator_address = b.orchestrator_address
   AND ifNull(d.model_id, '') = ifNull(b.model_id, '')
   AND ifNull(d.gpu_id, '') = ifNull(b.gpu_id, '')
WHERE b.orchestrator_address != ''
  -- GPU view intentionally serves only attributable orchestrator+GPU rows.
  AND ifNull(b.gpu_id, '') != '';

CREATE OR REPLACE VIEW v_api_network_demand AS
-- Grain: 1 row per (hour, gateway, region, pipeline, model_id).
-- Purpose: primary serving view for /network/demand.
WITH latest_sessions AS
(
    -- Last-write-wins session snapshot for demand/reliability counters.
    SELECT
        workflow_session_id,
        argMax(session_start_ts, version) AS session_start_ts,
        argMax(gateway, version) AS gateway,
        argMax(region, version) AS region,
        argMax(pipeline, version) AS pipeline,
        argMax(model_id, version) AS model_id,
        argMax(orchestrator_address, version) AS orchestrator_address,
        argMax(known_stream, version) AS known_stream,
        argMax(startup_unexcused, version) AS startup_unexcused,
        argMax(confirmed_swap_count, version) AS confirmed_swap_count,
        argMax(inferred_orchestrator_change_count, version) AS inferred_orchestrator_change_count,
        argMax(swap_count, version) AS swap_count
    FROM fact_workflow_sessions
    GROUP BY workflow_session_id
),
perf_pipeline_fallback AS
(
    -- For empty status pipeline values, only backfill when session evidence yields
    -- a single unambiguous pipeline at (hour,gateway,region,model_id) grain.
    SELECT
        toStartOfInterval(session_start_ts, INTERVAL 1 HOUR) AS window_start,
        gateway,
        ifNull(region, '') AS region,
        ifNull(model_id, '') AS model_id,
        if(
            countDistinctIf(pipeline, pipeline != '') = 1,
            anyIf(pipeline, pipeline != ''),
            ''
        ) AS pipeline_fallback
    FROM latest_sessions
    WHERE session_start_ts > toDateTime64('2000-01-01 00:00:00', 3, 'UTC')
    GROUP BY window_start, gateway, region, model_id
),
perf_1h AS
(
    -- Hourly usage/performance rollup with canonical pipeline fallback for empty status rows.
    SELECT
        toStartOfInterval(p.window_start, INTERVAL 1 HOUR) AS window_start,
        p.gateway,
        ifNull(p.region, '') AS region,
        if(p.pipeline != '', p.pipeline, ifNull(f.pipeline_fallback, '')) AS pipeline,
        ifNull(p.model_id, '') AS model_id,
        uniqExactMerge(p.sessions_uniq_state) AS total_sessions,
        uniqExactMerge(p.streams_uniq_state) AS total_streams,
        countMerge(p.sample_count_state) / 60.0 AS total_inference_minutes,
        avgMerge(p.output_fps_avg_state) AS avg_output_fps
    FROM agg_stream_performance_1m p
    LEFT JOIN perf_pipeline_fallback f
        ON f.window_start = toStartOfInterval(p.window_start, INTERVAL 1 HOUR)
       AND f.gateway = p.gateway
       AND f.region = ifNull(p.region, '')
       AND f.model_id = ifNull(p.model_id, '')
    GROUP BY
        window_start,
        p.gateway,
        region,
        pipeline,
        model_id
),
demand_1h AS
(
    -- Served vs unserved demand split from session facts.
    SELECT
        toStartOfInterval(session_start_ts, INTERVAL 1 HOUR) AS window_start,
        gateway,
        region,
        pipeline,
        model_id,
        sum(toUInt64(known_stream)) AS known_sessions,
        sum(toUInt64(known_stream AND orchestrator_address != '')) AS served_sessions,
        sum(toUInt64(known_stream AND orchestrator_address = '')) AS unserved_sessions,
        sum(toUInt64(startup_unexcused)) AS unexcused_sessions,
        sum(toUInt64(confirmed_swap_count > 0)) AS confirmed_swapped_sessions,
        sum(toUInt64(inferred_orchestrator_change_count > 0)) AS inferred_orchestrator_change_sessions,
        sum(toUInt64((confirmed_swap_count > 0) OR (inferred_orchestrator_change_count > 0))) AS swapped_sessions
    FROM latest_sessions
    WHERE session_start_ts > toDateTime64('2000-01-01 00:00:00', 3, 'UTC')
    GROUP BY
        window_start,
        gateway,
        region,
        pipeline,
        model_id
),
latest_session_by_request AS
(
    SELECT
        request_id,
        argMax(gateway, version) AS gateway,
        argMax(region, version) AS region,
        argMax(pipeline, version) AS pipeline,
        argMax(model_id, version) AS model_id
    FROM fact_workflow_sessions
    WHERE request_id != ''
    GROUP BY request_id
),
fees_1h AS
(
    -- Fees attributed to session dimensions via request_id linkage.
    SELECT
        toStartOfInterval(p.event_timestamp, INTERVAL 1 HOUR) AS window_start,
        s.gateway,
        s.region,
        s.pipeline,
        s.model_id,
        sum(toFloat64OrZero(p.face_value)) / 1000000000000000000.0 AS fee_payment_eth
    FROM payment_events p
    INNER JOIN latest_session_by_request s ON s.request_id = p.request_id
    GROUP BY
        window_start,
        s.gateway,
        s.region,
        s.pipeline,
        s.model_id
)
SELECT
    -- Core serving keys
    p.window_start AS window_start,
    p.gateway AS gateway,
    p.region AS region,
    p.pipeline AS pipeline,
    p.model_id AS model_id,

    p.total_streams,
    p.total_sessions,
    p.total_inference_minutes,
    p.avg_output_fps,

    ifNull(d.known_sessions, toUInt64(0)) AS known_sessions,
    ifNull(d.served_sessions, toUInt64(0)) AS served_sessions,
    ifNull(d.unserved_sessions, toUInt64(0)) AS unserved_sessions,
    ifNull(d.served_sessions, toUInt64(0)) + ifNull(d.unserved_sessions, toUInt64(0)) AS total_demand_sessions,
    ifNull(d.unexcused_sessions, toUInt64(0)) AS unexcused_sessions,
    ifNull(d.confirmed_swapped_sessions, toUInt64(0)) AS confirmed_swapped_sessions,
    ifNull(d.inferred_orchestrator_change_sessions, toUInt64(0)) AS inferred_orchestrator_change_sessions,
    ifNull(d.swapped_sessions, toUInt64(0)) AS swapped_sessions,
    -- Proxy for unmet demand (known sessions without orchestrator attribution).
    ifNull(d.unserved_sessions, toUInt64(0)) AS missing_capacity_count,
    ifNull(1 - (d.unexcused_sessions / nullIf(d.known_sessions, 0)), 0) AS success_ratio,
    ifNull(f.fee_payment_eth, 0.0) AS fee_payment_eth
FROM perf_1h p
LEFT JOIN demand_1h d
    ON d.window_start = p.window_start
   AND d.gateway = p.gateway
   AND ifNull(d.region, '') = ifNull(p.region, '')
   AND d.pipeline = p.pipeline
   AND ifNull(d.model_id, '') = ifNull(p.model_id, '')
LEFT JOIN fees_1h f
    ON f.window_start = p.window_start
   AND f.gateway = p.gateway
   AND ifNull(f.region, '') = ifNull(p.region, '')
   AND f.pipeline = p.pipeline
   AND ifNull(f.model_id, '') = ifNull(p.model_id, '');

CREATE OR REPLACE VIEW v_api_network_demand_by_gpu AS
-- Grain: 1 row per (hour, gateway, orchestrator, region, pipeline, model_id, gpu_id).
-- Purpose: GPU-sliced demand/capacity companion view.
WITH perf_gpu_1h_raw AS
(
    -- Used minutes and demand volume at GPU key grain.
    SELECT
        toStartOfInterval(window_start, INTERVAL 1 HOUR) AS window_start,
        gateway,
        orchestrator_address,
        region,
        pipeline,
        model_id,
        gpu_id,
        uniqExactMerge(streams_uniq_state) AS total_streams,
        uniqExactMerge(sessions_uniq_state) AS total_sessions,
        countMerge(sample_count_state) / 60.0 AS used_inference_minutes
    FROM agg_stream_performance_1m
    GROUP BY
        window_start,
        gateway,
        orchestrator_address,
        region,
        pipeline,
        model_id,
        gpu_id
),
latest_gpu_by_key AS
(
    -- Hourly fallback GPU attribution from latest session snapshots.
    WITH latest_sessions AS
    (
        SELECT
            workflow_session_id,
            argMax(session_start_ts, version) AS session_start_ts,
            argMax(gateway, version) AS gateway,
            argMax(orchestrator_address, version) AS orchestrator_address,
            argMax(region, version) AS region,
            argMax(pipeline, version) AS pipeline,
            argMax(model_id, version) AS model_id,
            argMax(gpu_id, version) AS gpu_id
        FROM fact_workflow_sessions
        GROUP BY workflow_session_id
    )
    SELECT
        toStartOfInterval(session_start_ts, INTERVAL 1 HOUR) AS window_start,
        gateway,
        orchestrator_address,
        region,
        pipeline,
        model_id,
        argMaxIf(gpu_id, session_start_ts, ifNull(gpu_id, '') != '') AS gpu_id
    FROM latest_sessions
    GROUP BY
        window_start,
        gateway,
        orchestrator_address,
        region,
        pipeline,
        model_id
),
perf_gpu_1h AS
(
    SELECT
        p.window_start,
        p.gateway,
        p.orchestrator_address,
        p.region,
        p.pipeline,
        p.model_id,
        nullIf(if(ifNull(p.gpu_id, '') != '', p.gpu_id, ifNull(k.gpu_id, '')), '') AS gpu_id,
        p.total_streams,
        p.total_sessions,
        p.used_inference_minutes
    FROM perf_gpu_1h_raw p
    LEFT JOIN latest_gpu_by_key k
        ON k.window_start = p.window_start
       AND k.gateway = p.gateway
       AND k.orchestrator_address = p.orchestrator_address
       AND ifNull(k.region, '') = ifNull(p.region, '')
       AND k.pipeline = p.pipeline
       AND ifNull(k.model_id, '') = ifNull(p.model_id, '')
),
rel_gpu_1h_raw AS
(
    -- Reliability counters aligned to GPU key grain.
    SELECT
        window_start,
        gateway,
        orchestrator_address,
        region,
        pipeline,
        model_id,
        gpu_id,
        sumMerge(known_sessions_state) AS known_sessions,
        sumMerge(unexcused_sessions_state) AS unexcused_sessions,
        sumMerge(confirmed_swapped_sessions_state) AS confirmed_swapped_sessions,
        sumMerge(inferred_orchestrator_change_sessions_state) AS inferred_orchestrator_change_sessions,
        sumMerge(swapped_sessions_state) AS swapped_sessions
    FROM agg_reliability_1h
    GROUP BY
        window_start,
        gateway,
        orchestrator_address,
        region,
        pipeline,
        model_id,
        gpu_id
),
rel_gpu_1h AS
(
    SELECT
        r.window_start,
        r.gateway,
        r.orchestrator_address,
        r.region,
        r.pipeline,
        r.model_id,
        nullIf(if(ifNull(r.gpu_id, '') != '', r.gpu_id, ifNull(k.gpu_id, '')), '') AS gpu_id,
        r.known_sessions,
        r.unexcused_sessions,
        r.confirmed_swapped_sessions,
        r.inferred_orchestrator_change_sessions,
        r.swapped_sessions
    FROM rel_gpu_1h_raw r
    LEFT JOIN latest_gpu_by_key k
        ON k.window_start = r.window_start
       AND k.gateway = r.gateway
       AND k.orchestrator_address = r.orchestrator_address
       AND ifNull(k.region, '') = ifNull(r.region, '')
       AND k.pipeline = r.pipeline
       AND ifNull(k.model_id, '') = ifNull(r.model_id, '')
),
dim_gpu_type AS
(
    -- Map GPU IDs to stable display type/name.
    SELECT
        orchestrator_address,
        model_id,
        gpu_id,
        argMax(gpu_name, latest_snapshot_ts) AS gpu_type
    FROM dim_orchestrator_capability_current
    GROUP BY orchestrator_address, model_id, gpu_id
),
capacity_current AS
(
    -- Current capacity snapshot used as denominator for capacity_rate proxy.
    SELECT
        orchestrator_address,
        model_id,
        argMax(capacity, snapshot_ts) AS capacity
    FROM dim_orchestrator_capability_model_constraints
    GROUP BY orchestrator_address, model_id
),
latest_session_by_request AS
(
    SELECT
        request_id,
        argMax(gateway, version) AS gateway,
        argMax(orchestrator_address, version) AS orchestrator_address,
        argMax(region, version) AS region,
        argMax(pipeline, version) AS pipeline,
        argMax(model_id, version) AS model_id,
        argMax(gpu_id, version) AS gpu_id
    FROM fact_workflow_sessions
    WHERE request_id != ''
    GROUP BY request_id
),
fees_gpu_1h_raw AS
(
    -- Fees attributed at GPU key grain.
    SELECT
        toStartOfInterval(p.event_timestamp, INTERVAL 1 HOUR) AS window_start,
        s.gateway,
        s.orchestrator_address,
        s.region,
        s.pipeline,
        s.model_id,
        s.gpu_id,
        sum(toFloat64OrZero(p.face_value)) / 1000000000000000000.0 AS fee_payment_eth
    FROM payment_events p
    INNER JOIN latest_session_by_request s ON s.request_id = p.request_id
    GROUP BY
        window_start,
        s.gateway,
        s.orchestrator_address,
        s.region,
        s.pipeline,
        s.model_id,
        s.gpu_id
),
fees_gpu_1h AS
(
    SELECT
        f.window_start,
        f.gateway,
        f.orchestrator_address,
        f.region,
        f.pipeline,
        f.model_id,
        nullIf(if(ifNull(f.gpu_id, '') != '', f.gpu_id, ifNull(k.gpu_id, '')), '') AS gpu_id,
        f.fee_payment_eth
    FROM fees_gpu_1h_raw f
    LEFT JOIN latest_gpu_by_key k
        ON k.window_start = f.window_start
       AND k.gateway = f.gateway
       AND k.orchestrator_address = f.orchestrator_address
       AND ifNull(k.region, '') = ifNull(f.region, '')
       AND k.pipeline = f.pipeline
       AND ifNull(k.model_id, '') = ifNull(f.model_id, '')
)
SELECT
    p.window_start AS window_start,
    p.gateway AS gateway,
    p.orchestrator_address AS orchestrator_address,
    p.region AS region,
    p.pipeline AS pipeline,
    p.model_id AS model_id,
    p.gpu_id AS gpu_id,
    ifNull(d.gpu_type, 'unknown') AS gpu_type,

    p.total_streams,
    p.total_sessions,
    p.used_inference_minutes AS inference_minutes_by_gpu_type,
    p.used_inference_minutes,
    -- Capacity proxy: capacity slots * 60 minutes in the hour.
    ifNull(c.capacity, 0) * 60.0 AS available_capacity_minutes,
    ifNull(p.used_inference_minutes / nullIf(ifNull(c.capacity, 0) * 60.0, 0), 0) AS capacity_rate,

    ifNull(r.known_sessions, toUInt64(0)) AS known_sessions,
    ifNull(r.unexcused_sessions, toUInt64(0)) AS unexcused_sessions,
    ifNull(r.confirmed_swapped_sessions, toUInt64(0)) AS confirmed_swapped_sessions,
    ifNull(r.inferred_orchestrator_change_sessions, toUInt64(0)) AS inferred_orchestrator_change_sessions,
    ifNull(r.swapped_sessions, toUInt64(0)) AS swapped_sessions,
    ifNull(r.unexcused_sessions, toUInt64(0)) AS missing_capacity_count,

    ifNull(f.fee_payment_eth, 0.0) AS fee_payment_eth
FROM perf_gpu_1h p
LEFT JOIN rel_gpu_1h r
    ON r.window_start = p.window_start
   AND r.gateway = p.gateway
   AND r.orchestrator_address = p.orchestrator_address
   AND ifNull(r.region, '') = ifNull(p.region, '')
   AND r.pipeline = p.pipeline
   AND ifNull(r.model_id, '') = ifNull(p.model_id, '')
   AND ifNull(r.gpu_id, '') = ifNull(p.gpu_id, '')
LEFT JOIN dim_gpu_type d
    ON d.orchestrator_address = p.orchestrator_address
   AND ifNull(d.model_id, '') = ifNull(p.model_id, '')
   AND ifNull(d.gpu_id, '') = ifNull(p.gpu_id, '')
LEFT JOIN capacity_current c
    ON c.orchestrator_address = p.orchestrator_address
   AND ifNull(c.model_id, '') = ifNull(p.model_id, '')
LEFT JOIN fees_gpu_1h f
    ON f.window_start = p.window_start
   AND f.gateway = p.gateway
   AND f.orchestrator_address = p.orchestrator_address
   AND ifNull(f.region, '') = ifNull(p.region, '')
   AND f.pipeline = p.pipeline
   AND ifNull(f.model_id, '') = ifNull(p.model_id, '')
   AND ifNull(f.gpu_id, '') = ifNull(p.gpu_id, '');

CREATE OR REPLACE VIEW v_api_jitter_5m AS
SELECT
    toStartOfInterval(window_start, INTERVAL 5 MINUTE) AS window_start_5m,
    orchestrator_address,
    pipeline,
    model_id,
    gpu_id,
    region,
    avgMerge(output_fps_avg_state) AS avg_output_fps,
    stddevPopMerge(fps_jitter_num_state) / nullIf(avgMerge(fps_jitter_den_state), 0) AS jitter_coeff_fps,
    countMerge(sample_count_state) AS status_samples
FROM agg_stream_performance_1m
GROUP BY
    window_start_5m,
    orchestrator_address,
    pipeline,
    model_id,
    gpu_id,
    region
HAVING status_samples >= 10;

CREATE OR REPLACE VIEW v_api_sla_compliance AS
-- Grain: 1 row per attributed (hour, orchestrator, pipeline, model_id, gpu_id, region).
-- Purpose: serving view for /sla/compliance and reliability joins.
WITH latest_sessions AS
(
    -- Last-write-wins session snapshot before hourly rollup.
    SELECT
        workflow_session_id,
        argMax(session_start_ts, version) AS session_start_ts,
        argMax(orchestrator_address, version) AS orchestrator_address,
        argMax(pipeline, version) AS pipeline,
        argMax(model_id, version) AS model_id,
        argMax(gpu_id, version) AS gpu_id,
        argMax(region, version) AS region,
        argMax(known_stream, version) AS known_stream,
        argMax(startup_success, version) AS startup_success,
        argMax(startup_excused, version) AS startup_excused,
        argMax(startup_unexcused, version) AS startup_unexcused,
        argMax(confirmed_swap_count, version) AS confirmed_swap_count,
        argMax(inferred_orchestrator_change_count, version) AS inferred_orchestrator_change_count,
        argMax(swap_count, version) AS swap_count
    FROM fact_workflow_sessions
    GROUP BY workflow_session_id
)
SELECT
    toStartOfInterval(session_start_ts, INTERVAL 1 HOUR) AS window_start,
    orchestrator_address,
    pipeline,
    model_id,
    gpu_id,
    region,
    sum(toUInt64(known_stream)) AS known_sessions,
    sum(toUInt64(startup_success)) AS startup_success_sessions,
    sum(toUInt64(startup_excused)) AS excused_sessions,
    sum(toUInt64(startup_unexcused)) AS unexcused_sessions,
    sum(toUInt64(confirmed_swap_count > 0)) AS confirmed_swapped_sessions,
    sum(toUInt64(inferred_orchestrator_change_count > 0)) AS inferred_orchestrator_change_sessions,
    sum(toUInt64((confirmed_swap_count > 0) OR (inferred_orchestrator_change_count > 0))) AS swapped_sessions,
    1 - (sum(toUInt64(startup_unexcused)) / nullIf(sum(toUInt64(known_stream)), 0)) AS success_ratio,
    1 - (sum(toUInt64((confirmed_swap_count > 0) OR (inferred_orchestrator_change_count > 0))) / nullIf(sum(toUInt64(known_stream)), 0)) AS no_swap_ratio,
    (
        (1 - (sum(toUInt64(startup_unexcused)) / nullIf(sum(toUInt64(known_stream)), 0))) * 0.7
        +
        (1 - (sum(toUInt64((confirmed_swap_count > 0) OR (inferred_orchestrator_change_count > 0))) / nullIf(sum(toUInt64(known_stream)), 0))) * 0.3
    ) * 100 AS sla_score
FROM latest_sessions
WHERE session_start_ts > toDateTime64('2000-01-01 00:00:00', 3, 'UTC')
  AND orchestrator_address != ''
GROUP BY
    window_start,
    orchestrator_address,
    pipeline,
    model_id,
    gpu_id,
    region;

-- ============================================================
-- HEALTH CHECK VIEWS
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

-- Session identity drilldown view (canonical wallet identity)
CREATE VIEW IF NOT EXISTS v_api_session_identity AS
SELECT
    workflow_session_id,
    argMax(orchestrator_address, version) AS orchestrator_address,
    argMax(stream_id, version) AS stream_id,
    argMax(request_id, version) AS request_id,
    argMax(session_start_ts, version) AS session_start_ts,
    argMax(session_end_ts, version) AS session_end_ts,
    max(version) AS latest_version
FROM fact_workflow_sessions
GROUP BY workflow_session_id;

-- Table 5: AI Stream Events (errors and lifecycle events)
CREATE TABLE IF NOT EXISTS ai_stream_events
(
    event_timestamp DateTime64(3, 'UTC'),
    event_date Date MATERIALIZED toDate(event_timestamp),

    -- Identifiers
    stream_id String,
    request_id String,
    pipeline String,

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
