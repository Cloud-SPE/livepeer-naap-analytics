Perfect! Let's design the ClickHouse schema. I'll base this on your actual data and the NaaP requirements.

## ClickHouse Schema Design Strategy

### Key Design Principles

1. **Denormalization**: ClickHouse loves wide tables - avoid joins in queries
2. **Partitioning**: By date for efficient data management and TTL
3. **Ordering Key**: For query performance (most common filters first)
4. **Materialized Views**: Pre-aggregate common metrics
5. **Codec Compression**: Reduce storage costs

---

## Core Tables Design

### 1. Raw Events Table (Optional but Recommended)

This is a "catch-all" for raw events before processing. Useful for debugging.

```sql
CREATE TABLE raw_events
(
    event_id UUID DEFAULT generateUUIDv4(),
    event_timestamp DateTime64(3, 'UTC'),
    event_date Date MATERIALIZED toDate(event_timestamp),
    event_type LowCardinality(String),  -- 'ai_stream_status', 'stream_ingest_metrics', etc.
    
    -- Identifiers
    stream_id String,
    request_id String,
    gateway String,  -- Currently empty in your data, but will be populated
    
    -- Orchestrator info
    orchestrator_address String,
    orchestrator_url String,
    
    -- Raw JSON payload
    raw_data String,  -- Full JSON for replay/debugging
    
    -- Metadata
    ingestion_timestamp DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, event_type, stream_id, event_timestamp)
TTL event_date + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;
```

**Why this design:**
- `LowCardinality(String)` for `event_type`: Only ~10 distinct values, massive compression
- `PARTITION BY toYYYYMM`: Drops entire partitions when TTL expires (fast)
- `ORDER BY (event_date, event_type, stream_id, event_timestamp)`: Common query patterns
- `raw_data String`: Keep full JSON for replay, compress well with ZSTD codec

---

### 2. AI Stream Status Table

This captures the core performance metrics from `ai_stream_status` events.

```sql
CREATE TABLE ai_stream_status
(
    -- Time dimensions
    event_timestamp DateTime64(3, 'UTC'),
    event_date Date MATERIALIZED toDate(event_timestamp),
    event_hour UInt8 MATERIALIZED toHour(event_timestamp),
    
    -- Identifiers
    stream_id String,
    request_id String,
    gateway String,
    
    -- Orchestrator info (denormalized for fast queries)
    orchestrator_address String,
    orchestrator_url String,
    gpu_id String,  -- Will be added when data gap is fixed
    region LowCardinality(String),  -- Will be added when data gap is fixed
    
    -- Workflow info
    pipeline LowCardinality(String),  -- 'streamdiffusion-sdxl-faceid'
    pipeline_id String,
    model_id String,  -- For workload-specific queries
    
    -- Performance metrics
    output_fps Float32,
    input_fps Float32,
    
    -- Inference status
    state LowCardinality(String),  -- 'ONLINE', 'DEGRADED_INFERENCE'
    restart_count UInt32,
    last_error Nullable(String),
    last_error_time Nullable(DateTime64(3)),
    
    -- Prompt parameters (for test load correlation)
    prompt_text Nullable(String),
    prompt_width UInt16,
    prompt_height UInt16,
    params_hash String,  -- For grouping identical workloads
    
    -- Cold start indicator (when data gap is fixed)
    is_cold_start Boolean DEFAULT false,
    startup_time_ms Nullable(UInt32),
    
    -- Metadata
    ingestion_timestamp DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, orchestrator_address, pipeline, stream_id, event_timestamp)
TTL event_date + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;
```

**Key design choices:**

- **Materialized columns** (`event_date`, `event_hour`): No storage cost, enables fast filtering
- **Denormalized orchestrator info**: Avoid joins with orchestrator lookup table
- **`ORDER BY`**: Most common query = "Show me orchestrator X's performance for pipeline Y"
- **Nullable fields**: For data gaps (e.g., `gpu_id` missing now, but will exist later)

**Common queries this optimizes:**

```sql
-- Query 1: Orchestrator performance over time
SELECT 
    toStartOfInterval(event_timestamp, INTERVAL 5 MINUTE) as time_bucket,
    avg(output_fps) as avg_fps,
    quantile(0.95)(output_fps) as p95_fps
FROM ai_stream_status
WHERE event_date >= today() - 7
  AND orchestrator_address = '0x52cf2968b3dc6016778742d3539449a6597d5954'
  AND pipeline = 'streamdiffusion-sdxl-faceid'
GROUP BY time_bucket
ORDER BY time_bucket;

-- Query 2: Per-GPU performance (when gpu_id populated)
SELECT 
    gpu_id,
    avg(output_fps) as avg_fps,
    sum(restart_count) as total_restarts
FROM ai_stream_status
WHERE event_date = today()
GROUP BY gpu_id;
```

---

### 3. Stream Ingest Metrics Table

Network performance metrics from `stream_ingest_metrics` events.

```sql
CREATE TABLE stream_ingest_metrics
(
    -- Time dimensions
    event_timestamp DateTime64(3, 'UTC'),
    event_date Date MATERIALIZED toDate(event_timestamp),
    
    -- Identifiers
    stream_id String,
    request_id String,
    gateway String,
    orchestrator_address String,
    pipeline_id String,
    
    -- Connection quality
    connection_quality LowCardinality(String),  -- 'good', 'poor', etc.
    
    -- Network metrics (aggregated from track_stats array)
    video_jitter Float32,
    video_packets_received UInt32,
    video_packets_lost UInt32,
    video_packet_loss_pct Float32,
    video_last_input_ts Float32,
    
    audio_jitter Float32,
    audio_packets_received UInt32,
    audio_packets_lost UInt32,
    audio_packet_loss_pct Float32,
    audio_last_input_ts Float32,
    
    -- Peer connection stats
    bytes_received UInt64,
    bytes_sent UInt64,
    transport_id String,
    
    -- Metadata
    ingestion_timestamp DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, stream_id, event_timestamp)
TTL event_date + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;
```

**Design notes:**

- **Flattened `track_stats` array**: Split video/audio into separate columns for easier querying
- **Bandwidth calculation**: `bytes_received`/`bytes_sent` can be diffed for rates
- No `gpu_id` here because this is network-layer data (before GPU processing)

---

### 4. Stream Trace Events Table

For latency calculations (prompt-to-first-frame, E2E, etc.)

```sql
CREATE TABLE stream_trace_events
(
    -- Time dimensions
    event_timestamp DateTime64(3, 'UTC'),
    event_date Date MATERIALIZED toDate(event_timestamp),
    
    -- Identifiers
    stream_id String,
    request_id String,
    gateway String,
    orchestrator_address String,
    orchestrator_url String,
    pipeline_id String,
    
    -- Trace event type
    trace_type LowCardinality(String),  -- 'gateway_receive_stream_request', 'runner_send_first_processed_segment', etc.
    
    -- Data timestamp (from the event payload)
    data_timestamp DateTime64(3, 'UTC'),
    
    -- Metadata
    ingestion_timestamp DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, request_id, trace_type, data_timestamp)
TTL event_date + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;
```

**Why separate table:**

- Trace events are 1:N with streams (multiple traces per request)
- Optimized for correlation queries (join by `request_id`)
- Can be queried independently for "network topology" analysis

**Flink will join these to calculate:**

```sql
-- This SQL would run in Flink, output goes to metrics table below
SELECT 
    request_id,
    max(CASE WHEN trace_type = 'gateway_receive_first_processed_segment' THEN data_timestamp END) -
    max(CASE WHEN trace_type = 'gateway_receive_stream_request' THEN data_timestamp END) as prompt_to_first_frame_ms
FROM stream_trace_events
WHERE request_id = 'ce6ee475'
GROUP BY request_id;
```

---

### 5. Calculated Metrics Table

**This is where Flink outputs derived metrics.**

```sql
CREATE TABLE calculated_metrics
(
    -- Time dimensions
    metric_timestamp DateTime64(3, 'UTC'),
    metric_date Date MATERIALIZED toDate(metric_timestamp),
    window_start DateTime64(3, 'UTC'),  -- For windowed metrics
    window_end DateTime64(3, 'UTC'),
    
    -- Identifiers
    stream_id String,
    request_id Nullable(String),  -- Some metrics are per-stream, not per-request
    gateway String,
    orchestrator_address String,
    gpu_id Nullable(String),
    region LowCardinality(String),
    
    -- Workflow info
    pipeline LowCardinality(String),
    model_id String,
    
    -- Metric metadata
    metric_name LowCardinality(String),  -- 'prompt_to_first_frame_latency', 'jitter_coefficient', etc.
    metric_category LowCardinality(String),  -- 'performance', 'reliability', 'economics'
    
    -- Metric value (polymorphic)
    metric_value Float64,
    metric_unit LowCardinality(String),  -- 'ms', 'fps', 'percent', etc.
    
    -- Additional context (JSON for flexibility)
    metric_metadata String,  -- e.g., {"sample_count": 100, "stddev": 2.5}
    
    -- Metadata
    ingestion_timestamp DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(metric_date)
ORDER BY (metric_date, metric_name, orchestrator_address, pipeline, metric_timestamp)
TTL metric_date + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;
```

**This is your "golden table" for dashboards and APIs.**

**Why this design:**

- **Polymorphic value column**: All metrics in one table (simpler queries)
- **Metric metadata**: Flexible JSON for metric-specific fields
- **Pre-aggregated**: Flink does heavy lifting, ClickHouse just stores/queries

**Example inserts from Flink:**

```sql
-- Prompt-to-first-frame latency (per request)
INSERT INTO calculated_metrics VALUES (
    now(), today(), '2026-01-14 10:00:00', '2026-01-14 10:00:05',
    'stream-374e1e', 'ce6ee475', 'gateway-daydream-us-east',
    '0x52cf...', 'GPU-a8d13655...', 'us-east-1',
    'streamdiffusion-sdxl-faceid', 'sdxl-v1.0',
    'prompt_to_first_frame_latency', 'performance',
    3300.0, 'ms',
    '{"request_timestamp": "2026-01-14T10:00:00Z"}',
    now()
);

-- Jitter coefficient (per 5-minute window)
INSERT INTO calculated_metrics VALUES (
    now(), today(), '2026-01-14 10:00:00', '2026-01-14 10:05:00',
    'stream-374e1e', NULL, 'gateway-daydream-us-east',
    '0x52cf...', 'GPU-a8d13655...', 'us-east-1',
    'streamdiffusion-sdxl-faceid', 'sdxl-v1.0',
    'jitter_coefficient', 'performance',
    0.08, 'ratio',
    '{"sample_count": 60, "mean_fps": 19.98, "stddev_fps": 1.6}',
    now()
);
```

---

### 6. Orchestrator Metadata Table

Slowly-changing dimension (SCD) for orchestrator info.

```sql
CREATE TABLE orchestrator_metadata
(
    -- Identifiers
    orchestrator_address String,
    
    -- Hardware info (from network_capabilities event)
    gpu_id String,
    gpu_name String,  -- 'NVIDIA GeForce RTX 5090'
    gpu_memory_total UInt64,
    gpu_memory_free UInt64,
    gpu_major_version UInt8,  -- CUDA compute capability
    
    -- Software info
    runner_version String,  -- '0.14.0'
    orchestrator_version String,  -- '0.8.8-3a5678ec-dirty'
    
    -- Capabilities
    supported_pipelines Array(String),  -- ['streamdiffusion-sdxl-faceid', ...]
    capacity_in_use UInt8,  -- From capabilities.constraints.PerCapability
    
    -- Pricing
    price_per_pixel Float64,  -- From capabilities_prices
    
    -- Location (when data gap fixed)
    region LowCardinality(String),
    
    -- Validity period (SCD Type 2)
    valid_from DateTime DEFAULT now(),
    valid_to Nullable(DateTime),  -- NULL = current
    is_current Boolean MATERIALIZED (valid_to IS NULL),
    
    -- Metadata
    last_seen DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(last_seen)
PARTITION BY orchestrator_address
ORDER BY (orchestrator_address, valid_from)
SETTINGS index_granularity = 8192;
```

**Why `ReplacingMergeTree`:**

- Automatically deduplicates rows with same `ORDER BY` key
- Keeps latest version based on `last_seen`
- Handles orchestrator updates (e.g., GPU upgrade) gracefully

**Query to get current metadata:**

```sql
SELECT *
FROM orchestrator_metadata
WHERE is_current = true;
```

---

## Materialized Views for Pre-Aggregation

### 1. 5-Minute FPS Averages

```sql
CREATE MATERIALIZED VIEW ai_stream_status_5min_mv
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
    
    -- Aggregated metrics
    avg(output_fps) as avg_output_fps,
    quantile(0.5)(output_fps) as median_output_fps,
    quantile(0.95)(output_fps) as p95_output_fps,
    
    avg(input_fps) as avg_input_fps,
    
    count() as sample_count,
    sum(restart_count) as total_restarts
FROM ai_stream_status
GROUP BY time_bucket, metric_date, orchestrator_address, pipeline, gpu_id, region;
```

**Why materialized views:**

- Queries on pre-aggregated data are 10-100x faster
- Data is incrementally updated as new rows arrive
- Dashboard queries hit the MV, not raw table

### 2. Hourly Network Stats

```sql
CREATE MATERIALIZED VIEW stream_ingest_metrics_hourly_mv
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(metric_date)
ORDER BY (metric_date, stream_id, hour_bucket)
AS
SELECT
    toStartOfHour(event_timestamp) as hour_bucket,
    toDate(hour_bucket) as metric_date,
    stream_id,
    orchestrator_address,
    
    -- Network aggregates
    avg(video_jitter) as avg_video_jitter,
    avg(audio_jitter) as avg_audio_jitter,
    sum(video_packets_lost) as total_video_packets_lost,
    sum(audio_packets_lost) as total_audio_packets_lost,
    
    -- Bandwidth
    sum(bytes_received) as total_bytes_received,
    sum(bytes_sent) as total_bytes_sent,
    
    count() as sample_count
FROM stream_ingest_metrics
GROUP BY hour_bucket, metric_date, stream_id, orchestrator_address;
```

---

## Schema for MinIO/S3 (Parquet)

**Partitioning strategy:**

```
s3://livepeer-events/
├── raw_events/
│   ├── event_date=2026-01-14/
│   │   ├── event_type=ai_stream_status/
│   │   │   └── part-00000.parquet
│   │   ├── event_type=stream_ingest_metrics/
│   │   │   └── part-00000.parquet
│   │   └── event_type=stream_trace/
│   │       └── part-00000.parquet
│   └── event_date=2026-01-15/
│       └── ...
└── calculated_metrics/
    ├── metric_date=2026-01-14/
    │   ├── metric_category=performance/
    │   │   └── part-00000.parquet
    │   └── metric_category=reliability/
    │       └── part-00000.parquet
    └── metric_date=2026-01-15/
        └── ...
```

**Parquet schema mirrors ClickHouse tables** for easy replay.

---

## Sample Flink-to-ClickHouse Insert

In Flink, use the JDBC sink:

```java
// Java pseudocode
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// Read from Kafka
DataStream<AiStreamStatusEvent> events = env
    .addSource(new FlinkKafkaConsumer<>(...))
    .assignTimestampsAndWatermarks(...);

// Insert into ClickHouse
events.addSink(JdbcSink.sink(
    "INSERT INTO ai_stream_status (event_timestamp, stream_id, orchestrator_address, output_fps, ...) VALUES (?, ?, ?, ?, ...)",
    (ps, event) -> {
        ps.setTimestamp(1, event.getTimestamp());
        ps.setString(2, event.getStreamId());
        ps.setString(3, event.getOrchestratorAddress());
        ps.setFloat(4, event.getOutputFps());
        // ... set other fields
    },
    JdbcExecutionOptions.builder()
        .withBatchSize(1000)
        .withBatchIntervalMs(200)
        .build(),
    new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .withUrl("jdbc:clickhouse://clickhouse:8123/default")
        .withDriverName("com.clickhouse.jdbc.ClickHouseDriver")
        .build()
));
```

---

## Schema Evolution Strategy

**Handling data gaps (e.g., `gpu_id` added later):**

1. **Phase 1 (MVP)**: Create columns as `Nullable(String)`
2. **Phase 2 (data gap fixed)**: Backfill existing rows via:
   ```sql
   ALTER TABLE ai_stream_status UPDATE gpu_id = 'GPU-unknown' WHERE gpu_id IS NULL;
   ```
3. **Phase 3 (enforce)**: Make column `NOT NULL` via schema migration

**Adding new metrics:**

- Add row to `calculated_metrics` with new `metric_name`
- No schema changes needed (polymorphic design)

---

## Next Steps

1. **Review this schema** - any changes needed for your use case?
2. **Prioritize tables** - which tables to build first for MVP?
    - My suggestion: `ai_stream_status` + `calculated_metrics` (skip raw_events for now)
3. **Define first Flink job** - let's pick one metric to calculate (e.g., jitter coefficient)

**Questions:**

1. Do you want to keep `raw_events` table, or skip it and rely on Kafka + S3 for raw data?
2. Should materialized views be part of MVP, or add later?
3. Any specific metrics from the NaaP docs you want to prioritize?

Let me know what you think of this design!