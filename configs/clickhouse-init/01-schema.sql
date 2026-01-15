-- Create database
CREATE DATABASE IF NOT EXISTS livepeer_analytics;

-- Use the database
USE livepeer_analytics;

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

-- Materialized View 1: 5-minute FPS aggregates
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