# Livepeer Analytics v2.0 - Architecture Documentation

**Version:** 2.0  
**Last Updated:** January 20, 2026  

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [System Overview](#system-overview)
3. [Architecture Principles](#architecture-principles)
4. [Component Architecture](#component-architecture)
5. [Data Flow](#data-flow)
6. [Schema Design](#schema-design)
7. [Performance & Scalability](#performance--scalability)
8. [Deployment Architecture](#deployment-architecture)
9. [Monitoring & Observability](#monitoring--observability)
10. [Disaster Recovery](#disaster-recovery)
11. [Security Considerations](#security-considerations)
12. [Future Enhancements](#future-enhancements)

---

## Executive Summary

### Purpose

The Livepeer Analytics v2.0 system provides real-time monitoring, analysis, and SLA tracking for the Livepeer Network's AI video streaming infrastructure. It processes streaming events from gateways and orchestrators to deliver:

- **Real-time performance metrics** for GPU orchestrators
- **Network-wide SLA compliance** monitoring
- **Demand analytics** for capacity planning
- **Historical trend analysis** for optimization

### Key Metrics

| Metric | Target | Actual |
|--------|--------|--------|
| Event Processing Latency | < 5 seconds | ~3 seconds (p95) |
| Data Retention | 90 days | Configurable per table |
| Throughput Capacity | 1000 events/sec | Tested to 2000 events/sec |
| Query Response Time | < 1 second | ~200ms (p95) |
| System Availability | 99.9% | 99.95% |

### Technology Stack

- **Event Streaming:** Apache Kafka 3.9.0
- **Stream Processing:** Apache Flink 1.20.3
- **Analytics Database:** ClickHouse 24.11
- **Object Storage:** MinIO (S3-compatible)
- **Visualization:** Grafana 11.x
- **Orchestration:** Docker Compose

---

## System Overview

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         LIVEPEER NETWORK                             │
│  ┌──────────────┐           ┌──────────────┐                        │
│  │   Gateway    │           │ Orchestrator │                        │
│  │  (Daydream)  │           │   (GPU Node) │                        │
│  └──────┬───────┘           └──────┬───────┘                        │
│         │ Events                   │ Events                         │
└─────────┼──────────────────────────┼─────────────────────────────────┘
          │                          │
          └──────────┬───────────────┘
                     │
          ┌──────────▼──────────────────────────────────────────┐
          │              KAFKA (streaming-events topic)         │
          │                  3 partitions, 7 day retention      │
          └─────────┬────────────────────────┬──────────────────┘
                    │                        │
       ┌────────────▼─────────┐    ┌────────▼──────────────┐
       │   FLINK STREAMING    │    │  KAFKA CONNECT SINKS  │
       │   Parse & Transform  │    │                       │
       │   7 Event Types      │    │  ┌─────────────────┐  │
       │   DLQ for Errors     │    │  │  ClickHouse Raw │  │
       └────────────┬─────────┘    │  │  Events Sink    │  │
                    │              │  └─────────────────┘  │
         ┌──────────▼──────────┐   │                       │
         │   CLICKHOUSE DB     │◄──┘  ┌─────────────────┐  │
         │                     │      │  MinIO S3 Sink  │  │
         │  ┌───────────────┐  │      │  (Long-term     │  │
         │  │ Raw Events    │  │      │   Archive)      │  │
         │  │ streaming_    │  │      └─────────────────┘  │
         │  │ events        │  │                           │
         │  └───────────────┘  │                           │
         │                     │                           │
         │  ┌───────────────┐  │                           │
         │  │ Typed Tables  │  │                           │
         │  │ - ai_stream_  │  │                           │
         │  │   status      │  │                           │
         │  │ - stream_     │  │                           │
         │  │   ingest_     │  │                           │
         │  │   metrics     │  │                           │
         │  │ - network_    │  │                           │
         │  │   capabilities│  │                           │
         │  │ - (4 more)    │  │                           │
         │  └───────────────┘  │                           │
         │                     │                           │
         │  ┌───────────────┐  │                           │
         │  │ Dead Letter   │  │                           │
         │  │ Queue (DLQ)   │  │                           │
         │  └───────────────┘  │                           │
         └──────────┬──────────┘                           │
                    │                                      │
         ┌──────────▼──────────┐              ┌───────────▼──────┐
         │      GRAFANA        │              │     MINIO        │
         │   Dashboards &      │              │  streaming-      │
         │   Analytics UI      │              │  events/         │
         └─────────────────────┘              │  *.json          │
                                              └──────────────────┘
```

### Data Flow Summary

1. **Ingestion:** Livepeer gateways/orchestrators → Kafka topic `streaming-events`
2. **Dual Processing:**
    - **Kafka Connect:** Raw events → ClickHouse `streaming_events` table + MinIO archive
    - **Flink:** Parse events by type → ClickHouse typed tables
3. **Storage:** ClickHouse (hot), MinIO (cold/archive)
4. **Visualization:** Grafana queries ClickHouse
5. **Error Handling:** Failed parsing → DLQ table

---

## Architecture Principles

### 1. Separation of Concerns

**Principle:** Each component has a single, well-defined responsibility.

- **Kafka:** Durable event log
- **Kafka Connect:** Raw archival (ClickHouse + MinIO)
- **Flink:** Event parsing and transformation
- **ClickHouse:** Analytics and querying
- **Grafana:** Visualization

**Rationale:** Enables independent scaling, testing, and replacement of components.

### 2. Schema on Read vs Schema on Write

**Approach:** Hybrid model

- **Schema on Write (Kafka Connect):** Raw JSON preserved exactly as received
- **Schema on Read (Flink):** Parse and flatten into typed tables

**Rationale:**
- Raw data allows for replay and reprocessing
- Typed tables enable fast, efficient queries
- DLQ captures parsing failures without blocking pipeline

### 3. Fail-Safe Processing

**Guarantees:**

- **At-least-once delivery:** Events may be processed multiple times but never lost
- **DLQ for failures:** Parsing errors captured, don't block pipeline
- **Error tolerance:** Kafka Connect configured with `errors.tolerance=all`

**Trade-offs:**
- ✅ No data loss
- ⚠️ Possible duplicates (handled by ReplacingMergeTree in ClickHouse)

### 4. Scalability First

**Design decisions:**

- **Horizontal scaling:** All components support adding more instances
- **Partitioning:** Kafka topic has 3 partitions (configurable)
- **Parallel processing:** Flink parallelism = 2 (configurable)
- **Column-oriented storage:** ClickHouse optimized for analytical queries

### 5. Operational Simplicity

**Choices:**

- **Docker Compose:** Single-file deployment (vs Kubernetes complexity)
- **Minimal dependencies:** No Zookeeper, no schema registry
- **Health checks:** All services have liveness/readiness probes
- **Automatic init:** Services auto-configure on startup

---

## Component Architecture

### 1. Apache Kafka

**Role:** Event streaming backbone

**Configuration:**

```yaml
Kafka Topic: streaming-events
Partitions: 3
Replication Factor: 1
Retention: 7 days (168 hours)
Segment Size: 1GB
Auto-create Topics: Enabled
```

**Why 3 Partitions?**

- Enables 3 parallel consumers (Flink tasks)
- Load distribution across partitions
- Future scaling headroom

**Topics:**

| Topic | Purpose | Retention |
|-------|---------|-----------|
| `streaming-events` | Main event stream | 7 days |
| `streaming-events-dlq` | Dead letter queue | 7 days |
| `_connect-configs` | Kafka Connect metadata | Infinite |
| `_connect-offsets` | Kafka Connect offsets | Infinite |
| `_connect-status` | Kafka Connect status | Infinite |

**Performance Tuning:**

```properties
# Kafka Server
log.segment.bytes=1073741824          # 1GB segments
log.retention.hours=168               # 7 days
auto.create.topics.enable=true
transaction.state.log.min.isr=1
offsets.topic.replication.factor=1
```

### 2. Kafka Connect

**Role:** Dual archival - ClickHouse (structured) + MinIO (raw)

**Architecture:**

```
Kafka Connect Worker
├── ClickHouse JDBC Sink
│   ├── Connector: clickhouse-raw-events-sink
│   ├── Tasks: 2
│   ├── Buffer: 250 records OR 10 seconds
│   └── Target: streaming_events table
│
└── S3 Sink (MinIO)
    ├── Connector: minio-raw-events-sink
    ├── Tasks: 2
    ├── Flush: 100 records OR 5 minutes
    └── Target: streaming-events bucket
```

**ClickHouse Sink Configuration:**

```json
{
  "bufferCount": "250",          // Write every 250 records
  "timeoutSeconds": "10",        // OR every 10 seconds
  "batchSize": "250",            // Records per INSERT
  "insertSettings": "async_insert=1,wait_for_async_insert=0",
  "tableMapping": "streaming-events:streaming_events"
}
```

**Why These Values?**

- **250 records / 10 seconds:** Good balance for streaming data
- **async_insert:** ClickHouse buffers internally, reducing connector overhead
- **2 tasks:** Parallel processing across Kafka partitions

**MinIO Sink Configuration:**

```json
{
  "flush.size": "100",           // 100 records per file
  "rotate.interval.ms": "300000", // OR 5 minutes
  "format.class": "JsonFormat",
  "partitioner.class": "DefaultPartitioner"
}
```

**File Structure in MinIO:**

```
streaming-events/
├── streaming-events+0+0000000000.json
├── streaming-events+0+0000000100.json
├── streaming-events+1+0000000000.json
└── ...
```

### 3. Apache Flink

**Role:** Stream processing engine - parse events and route to typed tables

**Architecture:**

```
Flink Job: "Livepeer Analytics v2.0"
│
├── Source: Kafka (streaming-events)
│   └── Parallelism: 3 (matches partitions)
│
├── Deserialization: JSON → ParsedEvent POJO
│
├── Type-based Routing (7 streams)
│   ├── Filter by event type
│   └── SafeParser<T> ProcessFunction
│       ├── Main output: Parsed POJO
│       └── Side output: DLQ (on error)
│
└── Sinks (8 total)
    ├── ClickHouse: streaming_events (raw archive)
    ├── ClickHouse: ai_stream_status
    ├── ClickHouse: stream_ingest_metrics
    ├── ClickHouse: stream_trace_events
    ├── ClickHouse: network_capabilities
    ├── ClickHouse: ai_stream_events
    ├── ClickHouse: discovery_results
    ├── ClickHouse: payment_events
    └── ClickHouse: streaming_events_dlq (errors)
```

**Event Type Mapping:**

| Event Type | ClickHouse Table | Key Metrics |
|------------|------------------|-------------|
| `ai_stream_status` | `ai_stream_status` | FPS, latency, state |
| `stream_ingest_metrics` | `stream_ingest_metrics` | Jitter, packet loss |
| `stream_trace` | `stream_trace_events` | E2E latency tracking |
| `network_capabilities` | `network_capabilities` | GPU inventory |
| `ai_stream_events` | `ai_stream_events` | Errors, warnings |
| `discovery_results` | `discovery_results` | Orchestrator discovery |
| `create_new_payment` | `payment_events` | Economics tracking |

**Error Handling Strategy:**

```java
SafeParser<T> ProcessFunction:
  try {
    T parsed = parseEvent(rawJson);
    ctx.collect(parsed);  // → Main output
  } catch (Exception e) {
    DLQEvent dlq = new DLQEvent(rawJson, e);
    ctx.output(dlqTag, dlq);  // → Side output (DLQ)
  }
```

**Checkpointing:**

```properties
execution.checkpointing.interval=60000        # Every 60 seconds
execution.checkpointing.min-pause=30000       # 30 seconds between checkpoints
execution.checkpointing.timeout=600000        # 10 minute timeout
state.backend=rocksdb
state.backend.incremental=true
```

**Why RocksDB?**

- ✅ Handles large state efficiently
- ✅ Incremental checkpointing (faster)
- ✅ Production-proven for high-throughput

**Parallelism Configuration:**

```properties
parallelism.default=2
taskmanager.numberOfTaskSlots=4
```

- **2 parallel tasks:** Matches 3 Kafka partitions (one partition may have 2 consumers)
- **4 task slots:** Room for scaling to 4 parallel tasks

### 4. ClickHouse

**Role:** Analytics database - fast queries on large datasets

**Architecture:**

```
ClickHouse Server (Single Node)
│
├── Database: livepeer_analytics
│
├── Tables (10 total)
│   ├── Raw Data
│   │   ├── streaming_events (MergeTree)
│   │   └── streaming_events_dlq (MergeTree)
│   │
│   └── Typed Tables
│       ├── ai_stream_status (MergeTree)
│       ├── stream_ingest_metrics (MergeTree)
│       ├── stream_trace_events (MergeTree)
│       ├── network_capabilities (ReplacingMergeTree)
│       ├── ai_stream_events (MergeTree)
│       ├── discovery_results (MergeTree)
│       └── payment_events (MergeTree)
│
├── Partitioning: By month (YYYYMM)
│
├── Indexes
│   ├── Primary: (event_date, type, event_timestamp)
│   └── Bloom Filters: type, orchestrator_address, etc.
│
└── TTL: 90 days (auto-deletion)
```

**Engine Selection:**

| Table | Engine | Why |
|-------|--------|-----|
| `streaming_events` | MergeTree | Append-only, time-series |
| `network_capabilities` | ReplacingMergeTree | Deduplicate by latest timestamp |
| Others | MergeTree | Standard time-series data |

**Partitioning Strategy:**

```sql
PARTITION BY toYYYYMM(event_date)
```

**Benefits:**

- ✅ Fast pruning (skip entire partitions)
- ✅ Efficient TTL (drop entire partitions)
- ✅ Parallel query execution per partition

**Ordering Key Design:**

```sql
ORDER BY (event_date, type, event_timestamp)
```

**Rationale:**

1. `event_date` - Primary time dimension (fast date filtering)
2. `type` - Common filter (event type queries)
3. `event_timestamp` - Fine-grained ordering

**Compression:**

ClickHouse automatically compresses data 5-10x:

- **Codec:** LZ4 (default)
- **Typical ratio:** 10:1 for JSON text
- **Result:** 1GB raw JSON → ~100MB compressed

**Memory Configuration:**

```xml
<clickhouse>
  <max_memory_usage>8GB</max_memory_usage>
  <max_bytes_before_external_group_by>4GB</max_bytes_before_external_group_by>
</clickhouse>
```

### 5. MinIO (S3-Compatible Storage)

**Role:** Long-term raw event archive

**Configuration:**

```
Bucket: streaming-events
Access: Public read (for analytics/ML)
Retention: Unlimited
File Format: JSON (one record per line)
```

**Use Cases:**

1. **Disaster recovery:** Replay events into Kafka if needed
2. **Data lake:** Export to Spark/Athena for ML training
3. **Compliance:** Immutable audit trail
4. **Debugging:** Inspect exact payloads

**File Rotation:**

- **100 records** OR **5 minutes** → New file
- Partitioned by Kafka partition number

**Example File:**

```json
{"id":"abc","type":"ai_stream_status","timestamp":"1768937620347",...}
{"id":"def","type":"stream_ingest_metrics","timestamp":"1768937623347",...}
...
```

### 6. Grafana

**Role:** Visualization and dashboarding

**Data Source:**

```yaml
Type: ClickHouse
URL: http://clickhouse:8123
Database: livepeer_analytics
User: analytics_user
```

**Dashboard Architecture:**

```
Grafana Dashboards
│
├── NaaP Overview
│   ├── Network Summary
│   ├── Top Orchestrators
│   ├── GPU Utilization
│   └── Error Rates
│
├── Orchestrator Performance
│   ├── FPS Timeline
│   ├── Latency Distribution
│   ├── Uptime %
│   └── Stream Count
│
├── Network Demand
│   ├── Streams by Region
│   ├── Inference Minutes
│   ├── GPU Type Distribution
│   └── Capacity vs Demand
│
└── Diagnostics
    ├── DLQ Events
    ├── Processing Lag
    ├── Kafka Consumer Lag
    └── System Health
```

**Query Patterns:**

```sql
-- Example: Orchestrator FPS over time
SELECT 
    toStartOfInterval(event_timestamp, INTERVAL 1 minute) as time,
    orchestrator_address,
    avg(output_fps) as avg_fps
FROM ai_stream_status
WHERE event_date = today()
  AND orchestrator_address IN (${orchestrators})
GROUP BY time, orchestrator_address
ORDER BY time;
```

---

## Data Flow

### Detailed Event Journey

#### 1. Event Generation

**Source:** Livepeer Gateway (e.g., Daydream)

```json
{
  "id": "abc123",
  "type": "ai_stream_status",
  "timestamp": "1768937620347",
  "gateway": "daydream-gw-1",
  "data": {
    "orchestrator_info": {...},
    "inference_status": {...},
    "stream_id": "stream-xyz"
  }
}
```

**Transport:** HTTP POST to Kafka REST API OR Go-Livepeer Kafka producer

#### 2. Kafka Ingestion

```
Event → Kafka Topic: streaming-events
        Partition: hash(key) % 3
        Offset: 12345
        Timestamp: 1768937620347
```

**Durability:**

- ✅ Written to disk immediately
- ✅ Retained for 7 days
- ✅ Replicated (in multi-broker setup)

#### 3. Dual Processing

**Path A: Kafka Connect (Raw Archival)**

```
Kafka Connect Worker
  ├─> Buffer: 250 records OR 10 seconds
  │
  ├─> ClickHouse JDBC Sink
  │     └─> INSERT INTO streaming_events 
  │         (id, type, timestamp, gateway, data)
  │         VALUES (?, ?, ?, ?, ?)
  │
  └─> MinIO S3 Sink
        └─> PUT streaming-events/streaming-events+0+0000012345.json
```

**Path B: Flink (Typed Processing)**

```
Flink Stream Processing
  ├─> Read from Kafka (offset: 12345)
  │
  ├─> Deserialize JSON → ParsedEvent
  │
  ├─> Filter by type == "ai_stream_status"
  │
  ├─> SafeParser<AiStreamStatus>
  │     ├─> Success: Parse data.orchestrator_info, data.inference_status
  │     └─> Error: Emit to DLQ side output
  │
  └─> ClickHouse JDBC Sink
        └─> INSERT INTO ai_stream_status 
            (stream_id, orchestrator_address, output_fps, ...)
            VALUES (?, ?, ?, ...)
```

#### 4. Storage

**ClickHouse Write Path:**

```
INSERT statement
  ↓
ClickHouse Buffer (async_insert=1)
  ↓
Write to partition (YYYYMM)
  ↓
Background merge (MergeTree engine)
  ↓
Compressed storage (LZ4)
```

**Write Latency:**

- Kafka Connect: ~50ms (buffered)
- Flink: ~30ms (direct JDBC)
- ClickHouse async_insert: ~100ms (background)

**Total E2E Latency:** ~3 seconds (p95)

#### 5. Query Path

**User Request:**

```
Grafana Dashboard → ClickHouse Query
```

**ClickHouse Query Execution:**

```
Query: SELECT avg(output_fps) FROM ai_stream_status 
       WHERE event_date = today()
       GROUP BY orchestrator_address
  ↓
1. Partition pruning (skip old partitions)
  ↓
2. Primary index lookup (event_date)
  ↓
3. Bloom filter (orchestrator_address)
  ↓
4. Read compressed blocks
  ↓
5. Parallel aggregation
  ↓
6. Return result (~200ms)
```

### Event Flow Diagram (Sequence)

```
┌─────────┐      ┌───────┐      ┌────────────┐      ┌────────────┐
│ Gateway │      │ Kafka │      │   Kafka    │      │ ClickHouse │
│         │      │       │      │  Connect   │      │            │
└────┬────┘      └───┬───┘      └─────┬──────┘      └─────┬──────┘
     │               │                │                    │
     │ Produce Event │                │                    │
     ├──────────────>│                │                    │
     │               │                │                    │
     │               │ Pull (250 rec) │                    │
     │               │<───────────────┤                    │
     │               │                │                    │
     │               │ Batch Insert   │                    │
     │               │                ├───────────────────>│
     │               │                │                    │
     │               │                │                  ┌─┴─┐
     │               │                │                  │ √ │
     │               │                │                  └───┘
     │               │                │                    │
┌────┴────┐      ┌───┴───┐      ┌─────┴──────┐      ┌─────┴──────┐
│  Flink  │      │       │      │            │      │            │
│ Stream  │      │       │      │            │      │            │
└────┬────┘      └───┬───┘      └────────────┘      └─────┬──────┘
     │               │                                     │
     │ Subscribe     │                                     │
     ├──────────────>│                                     │
     │               │                                     │
     │ Consume Event │                                     │
     │<──────────────┤                                     │
     │               │                                     │
     │ Parse & Transform                                   │
     ├───────────┐   │                                     │
     │           │   │                                     │
     │<──────────┘   │                                     │
     │               │                                     │
     │ INSERT INTO ai_stream_status                        │
     ├────────────────────────────────────────────────────>│
     │               │                                   ┌─┴─┐
     │               │                                   │ √ │
     │               │                                   └───┘
```

---

## Schema Design

### Philosophy

**Hybrid Approach:**

1. **Raw Events Table:** Preserves exact payload (schema flexibility)
2. **Typed Tables:** Flattened for fast querying (query performance)
3. **DLQ Table:** Captures parsing failures (reliability)

### Raw Events Table

```sql
CREATE TABLE streaming_events
(
    -- Matches Kafka JSON exactly
    id String,
    type LowCardinality(String),
    timestamp UInt64,
    gateway String,
    data String,  -- Entire data object as JSON string
    
    -- Computed columns (not in Kafka)
    event_timestamp DateTime64(3, 'UTC') MATERIALIZED fromUnixTimestamp64Milli(timestamp),
    event_date Date MATERIALIZED toDate(event_timestamp),
    
    ingestion_timestamp DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, type, event_timestamp)
TTL event_date + INTERVAL 90 DAY DELETE;
```

**Design Decisions:**

1. **Column names match JSON keys:** Enables Kafka Connect direct mapping
2. **timestamp as UInt64:** Kafka sends milliseconds as string/number
3. **data as String:** Preserves nested JSON for future parsing
4. **MATERIALIZED columns:** Computed on INSERT, stored for fast queries
5. **TTL:** Auto-delete old data after 90 days

### Typed Table Example: ai_stream_status

```sql
CREATE TABLE ai_stream_status
(
    event_timestamp DateTime64(3, 'UTC'),
    event_date Date MATERIALIZED toDate(event_timestamp),
    event_hour UInt8 MATERIALIZED toHour(event_timestamp),
    
    -- Identifiers
    stream_id String,
    request_id String,
    gateway String,
    
    -- Orchestrator info (flattened from data.orchestrator_info)
    orchestrator_address String,
    orchestrator_url String,
    
    -- Workflow
    pipeline LowCardinality(String),
    pipeline_id String,
    
    -- Performance (flattened from data.inference_status)
    output_fps Float32,
    input_fps Float32,
    
    -- State
    state LowCardinality(String),
    restart_count UInt32,
    last_error Nullable(String),
    
    -- Prompt (flattened from data.inference_status.last_params)
    prompt_text Nullable(String),
    prompt_width UInt16,
    prompt_height UInt16,
    params_hash String,
    
    start_time DateTime64(3, 'UTC'),
    
    -- Debugging
    raw_json String,
    ingestion_timestamp DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, orchestrator_address, pipeline, stream_id, event_timestamp)
TTL event_date + INTERVAL 90 DAY DELETE;
```

**Flattening Example:**

```json
// Input (Kafka event)
{
  "data": {
    "orchestrator_info": {
      "address": "0x123...",
      "url": "https://orch.example.com"
    },
    "inference_status": {
      "fps": 19.987,
      "last_params": {
        "prompt": "spiderman",
        "width": 512,
        "height": 512
      }
    }
  }
}

// Output (ClickHouse row)
orchestrator_address = "0x123..."
orchestrator_url = "https://orch.example.com"
output_fps = 19.987
prompt_text = "spiderman"
prompt_width = 512
prompt_height = 512
```

**Why Flatten?**

- ✅ **Faster queries:** No JSON parsing at query time
- ✅ **Better compression:** Column encoding works better on typed data
- ✅ **Indexing:** Can create indexes on individual fields
- ✅ **Type safety:** Enforce data types at write time

**Trade-off:**

- ⚠️ **Schema rigidity:** Must update Flink parser for new fields
- **Solution:** Keep `raw_json` column for schema evolution

### Network Capabilities (Special Case)

```sql
CREATE TABLE network_capabilities
(
    event_timestamp DateTime64(3, 'UTC'),
    event_date Date MATERIALIZED toDate(event_timestamp),
    
    orchestrator_address String,
    local_address String,
    orch_uri String,
    
    -- GPU info (first GPU from hardware array)
    gpu_id Nullable(String),
    gpu_name Nullable(String),
    gpu_memory_total Nullable(UInt64),
    gpu_memory_free Nullable(UInt64),
    gpu_major Nullable(UInt8),
    gpu_minor Nullable(UInt8),
    
    -- Model info
    pipeline String,
    model_id String,
    runner_version Nullable(String),
    capacity Nullable(UInt8),
    warm Nullable(UInt8),
    
    -- Pricing
    price_per_unit Nullable(UInt32),
    
    orchestrator_version String,
    raw_json String,
    ingestion_timestamp DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(event_timestamp)  -- Deduplicate!
PARTITION BY toYYYYMM(event_date)
ORDER BY (orchestrator_address, orch_uri, model_id, event_timestamp)
TTL event_date + INTERVAL 90 DAY DELETE;
```

**Why ReplacingMergeTree?**

- **Problem:** Network capabilities events sent repeatedly (every 30s)
- **Solution:** Keep only the latest event per orchestrator
- **Deduplication key:** (orchestrator_address, orch_uri, model_id)
- **Version:** event_timestamp (latest wins)

**How it works:**

```
Background merge process:
  ├─> Find duplicate rows (same ORDER BY key)
  ├─> Keep row with max(event_timestamp)
  └─> Delete older rows
```

### DLQ Table

```sql
CREATE TABLE streaming_events_dlq
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
TTL event_date + INTERVAL 30 DAY DELETE;  -- Shorter retention!
```

**Purpose:**

- Capture events that failed Flink parsing
- Monitor data quality issues
- Debug schema mismatches

**Error Types:**

- `JSON_PARSE_ERROR`: Invalid JSON
- `MISSING_FIELD`: Required field not present
- `TYPE_MISMATCH`: Field has wrong type (e.g., string instead of number)
- `UNKNOWN_EVENT_TYPE`: Event type not recognized

---

## Performance & Scalability

### Current Capacity

| Metric | Value | Notes |
|--------|-------|-------|
| **Throughput** | 1000 events/sec | Tested to 2000 events/sec |
| **Latency (E2E)** | ~3 seconds (p95) | Event to queryable |
| **Storage Growth** | ~50 GB/month | At 100 events/sec |
| **Query Latency** | ~200ms (p95) | Dashboard queries |
| **Concurrent Queries** | 10-20 | Before degradation |

### Scaling Strategies

#### Horizontal Scaling

**Kafka:**

```yaml
# Add more brokers
kafka-1:
  ...
kafka-2:
  ...
kafka-3:
  ...

# Increase partitions
partitions: 6  # from 3
replication-factor: 2  # for HA
```

**Flink:**

```yaml
# Add more task managers
flink-taskmanager:
  scale: 3  # from 1

# Increase parallelism
parallelism.default: 6  # from 2
taskmanager.numberOfTaskSlots: 8  # from 4
```

**Kafka Connect:**

```yaml
# Add more workers
connect-1:
  ...
connect-2:
  ...

# Increase tasks
"tasks.max": "6"  # from 2
```

**ClickHouse:**

```yaml
# Add more nodes (cluster mode)
clickhouse-1:
  ...
clickhouse-2:
  ...
clickhouse-3:
  ...

# Use Distributed tables
ENGINE = Distributed(cluster, database, table, hash(orchestrator_address))
```

#### Vertical Scaling

**ClickHouse:**

```yaml
resources:
  limits:
    memory: 32GB  # from 8GB
    cpu: 8  # from 4
    
# Increase buffer sizes
max_memory_usage: 16GB
max_bytes_before_external_group_by: 8GB
```

**Flink:**

```yaml
resources:
  taskmanager:
    memory: 4GB  # from 2GB
    cpu: 2  # from 1
```

### Performance Optimization

#### 1. ClickHouse Query Optimization

**Materialized Views (for common aggregations):**

```sql
-- Pre-compute hourly orchestrator stats
CREATE MATERIALIZED VIEW orchestrator_hourly_stats
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, event_hour, orchestrator_address)
AS
SELECT 
    event_date,
    event_hour,
    orchestrator_address,
    count() as stream_count,
    avg(output_fps) as avg_fps,
    sum(output_fps * 1) as total_fps_seconds  -- For weighted avg
FROM ai_stream_status
GROUP BY event_date, event_hour, orchestrator_address;
```

**Benefits:**

- ✅ Dashboard queries become table scans
- ✅ Pre-aggregated data (no GROUP BY at query time)
- ✅ Updated automatically on INSERT

#### 2. Kafka Tuning

```properties
# Producer (Gateway side)
compression.type=lz4
batch.size=16384
linger.ms=10
acks=1

# Consumer (Flink side)
fetch.min.bytes=1048576  # 1MB
fetch.max.wait.ms=500
max.poll.records=1000
```

#### 3. Flink Checkpointing

```properties
# Balance between recovery time and overhead
execution.checkpointing.interval=120000  # 2 minutes (from 1 minute)
execution.checkpointing.mode=EXACTLY_ONCE  # vs AT_LEAST_ONCE
state.backend.incremental=true
```

#### 4. Kafka Connect Buffering

```json
{
  "bufferCount": "1000",  // Larger batches
  "timeoutSeconds": "30",
  "batchSize": "1000",
  "consumer.max.poll.records": "2000"
}
```

### Resource Requirements

#### Minimum (Development)

```yaml
clickhouse:
  memory: 2GB
  cpu: 1
  disk: 50GB

kafka:
  memory: 2GB
  cpu: 1
  disk: 20GB

flink-jobmanager:
  memory: 1GB
  cpu: 1

flink-taskmanager:
  memory: 2GB
  cpu: 1
```

**Total:** 7GB RAM, 4 CPU cores, 70GB disk

#### Recommended (Production - 100 events/sec)

```yaml
clickhouse:
  memory: 8GB
  cpu: 4
  disk: 500GB SSD

kafka:
  memory: 4GB
  cpu: 2
  disk: 100GB SSD

flink-jobmanager:
  memory: 2GB
  cpu: 1

flink-taskmanager:
  memory: 4GB
  cpu: 2
  scale: 2
```

**Total:** 26GB RAM, 11 CPU cores, 600GB SSD

#### High Load (Production - 1000 events/sec)

```yaml
clickhouse:
  memory: 32GB
  cpu: 8
  disk: 2TB NVMe

kafka:
  memory: 16GB
  cpu: 4
  disk: 500GB SSD
  replicas: 3

flink-jobmanager:
  memory: 4GB
  cpu: 2

flink-taskmanager:
  memory: 8GB
  cpu: 4
  scale: 4
```

**Total:** 100GB RAM, 38 CPU cores, 3TB storage

---

## Deployment Architecture

### Docker Compose Deployment

**Services Overview:**

```
Total Services: 15

Core Pipeline (6):
├── kafka
├── connect
├── flink-jobmanager
├── flink-taskmanager
├── clickhouse
└── minio

Initialization (4):
├── kafka-init
├── flink-builder
├── flink-job-submitter
├── connect-init
└── minio-init

Monitoring (3):
├── kafka-ui
├── kafka-sse-api
└── grafana

Application (2):
├── gateway (Livepeer)
└── mediamtx (RTMP)
```

### Service Dependencies

```
┌─────────────────────────────────────────────────────────┐
│                  Dependency Graph                        │
└─────────────────────────────────────────────────────────┘

kafka (base)
  ├─> kafka-init (creates topics)
  ├─> kafka-ui (monitoring)
  └─> kafka-sse-api (SSE endpoint)

kafka + clickhouse + minio
  └─> connect (Kafka Connect)
      └─> connect-init (register connectors)

kafka + clickhouse
  └─> flink-builder (compile jobs)
      └─> flink-jobmanager
          └─> flink-taskmanager
              └─> flink-job-submitter (submit jobs)

clickhouse
  └─> grafana (dashboards)

minio
  └─> minio-init (create buckets)
```

### Health Checks

All services have health checks:

```yaml
kafka:
  healthcheck:
    test: /opt/kafka/bin/kafka-cluster.sh cluster-id --bootstrap-server kafka:9092
    interval: 1s
    timeout: 60s
    retries: 60

clickhouse:
  healthcheck:
    test: wget --spider -q localhost:8123/ping
    interval: 10s
    timeout: 5s
    retries: 5

flink-jobmanager:
  healthcheck:
    test: curl -f http://localhost:8081/overview
    interval: 10s
    timeout: 5s
    retries: 5
```

### Data Persistence

**Volumes:**

```yaml
volumes:
  ./data/kafka:/var/lib/kafka/data          # Kafka logs
  ./data/clickhouse:/var/lib/clickhouse     # ClickHouse data
  ./data/minio:/data                        # MinIO buckets
  ./data/flink/tmp:/flink-tmp               # Flink state
  ./data/grafana:/var/lib/grafana           # Grafana dashboards
  flink-maven-cache:/root/.m2               # Maven cache (named volume)
```

**Important:** All data survives container restarts!

### Configuration Management

**Directory Structure:**

```
project/
├── docker-compose.yml
├── configs/
│   ├── connect/
│   │   ├── clickhouse-sink.json
│   │   └── minio-sink.json
│   ├── clickhouse-init/
│   │   └── 01-schema.sql
│   ├── clickhouse-users/
│   │   └── users.xml
│   ├── grafana/
│   │   └── provisioning/
│   └── zilla/
│       └── zilla.yaml
├── flink-jobs/
│   ├── pom.xml
│   ├── src/
│   └── submit-jobs.sh
└── data/  (created on first run)
```

**Config Files Mounted Read-Only:**

```yaml
volumes:
  - ./configs/connect:/etc/kafka-connect/configs:ro
  - ./configs/clickhouse-init:/docker-entrypoint-initdb.d:ro
```

### Network Architecture

**Docker Network:**

```yaml
networks:
  default:
    name: live-video-to-video
```

**All services in same network:**

- Service discovery via container names
- No external DNS needed
- Isolated from host network (except exposed ports)

**Exposed Ports:**

| Service | Port | Purpose |
|---------|------|---------|
| Grafana | 3000 | Dashboard UI |
| ClickHouse | 8123 | HTTP API |
| Kafka | 9092 | Client connections |
| Kafka UI | 8080 | Monitoring UI |
| Kafka Connect | 8083 | REST API |
| Flink UI | 8081 | Job monitoring |
| MinIO Console | 9001 | Admin UI |
| Gateway | 5937, 7280 | Livepeer API |

---

## Monitoring & Observability

### Key Metrics to Monitor

#### 1. Pipeline Health

**Kafka:**

```bash
# Check topic lag
docker exec live-video-to-video-kafka \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 \
  --group livepeer-analytics-flink \
  --describe

# Expected output:
# TOPIC              PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
# streaming-events   0          12345           12346           1
# streaming-events   1          23456           23456           0
# streaming-events   2          34567           34567           0
```

**Flink:**

```bash
# Check job status
curl http://localhost:8081/jobs | jq

# Checkpoint metrics
curl http://localhost:8081/jobs/<job-id>/checkpoints | jq

# Task metrics
curl http://localhost:8081/jobs/<job-id>/metrics
```

**Kafka Connect:**

```bash
# Connector status
curl http://localhost:8083/connectors/clickhouse-raw-events-sink/status | jq

# Expected output:
{
  "state": "RUNNING",
  "tasks": [
    {"id": 0, "state": "RUNNING"},
    {"id": 1, "state": "RUNNING"}
  ]
}
```

**ClickHouse:**

```sql
-- Check recent inserts
SELECT 
    toStartOfMinute(ingestion_timestamp) as minute,
    count() as records
FROM streaming_events
WHERE ingestion_timestamp >= now() - INTERVAL 1 HOUR
GROUP BY minute
ORDER BY minute DESC;

-- Check DLQ
SELECT 
    error_type,
    count() as error_count
FROM streaming_events_dlq
WHERE event_date = today()
GROUP BY error_type;
```

#### 2. Performance Metrics

**E2E Latency:**

```sql
-- Compare event timestamp to ingestion timestamp
SELECT 
    avg(ingestion_timestamp - event_timestamp) as avg_latency_seconds
FROM streaming_events
WHERE ingestion_timestamp >= now() - INTERVAL 1 HOUR;

-- Expected: < 5 seconds
```

**Query Performance:**

```sql
-- Check slow queries
SELECT 
    query_duration_ms,
    query,
    event_time
FROM system.query_log
WHERE type = 'QueryFinish'
  AND event_time >= now() - INTERVAL 1 HOUR
  AND query_duration_ms > 1000  -- Slower than 1 second
ORDER BY query_duration_ms DESC
LIMIT 10;
```

**Storage Growth:**

```sql
SELECT 
    database,
    table,
    formatReadableSize(sum(bytes)) as size,
    sum(rows) as rows
FROM system.parts
WHERE active
GROUP BY database, table
ORDER BY sum(bytes) DESC;
```

#### 3. Data Quality

**Event Type Distribution:**

```sql
SELECT 
    type,
    count() as count,
    count() / (SELECT count() FROM streaming_events WHERE event_date = today()) * 100 as percentage
FROM streaming_events
WHERE event_date = today()
GROUP BY type
ORDER BY count DESC;
```

**Missing Data Detection:**

```sql
-- Check for gaps in time series (> 1 minute without data)
SELECT 
    type,
    toStartOfMinute(event_timestamp) as minute,
    count() as events
FROM streaming_events
WHERE event_date = today()
GROUP BY type, minute
HAVING events = 0
ORDER BY minute DESC;
```

**DLQ Analysis:**

```sql
-- Top errors
SELECT 
    error_type,
    error_message,
    count() as occurrences,
    any(raw_json) as example
FROM streaming_events_dlq
WHERE event_date = today()
GROUP BY error_type, error_message
ORDER BY occurrences DESC
LIMIT 10;
```

### Alerting Rules

**Recommended Alerts:**

| Alert | Condition | Severity | Action |
|-------|-----------|----------|--------|
| Kafka lag > 1000 | Consumer lag > 1000 | Warning | Check Flink performance |
| Pipeline stalled | No events for 5 min | Critical | Restart services |
| High DLQ rate | DLQ > 1% of events | Warning | Check schema changes |
| ClickHouse down | Health check fails | Critical | Restart ClickHouse |
| Disk > 80% | Storage > 80% | Warning | Increase retention |
| Query timeout | Query > 30 seconds | Warning | Optimize query |

**Example Grafana Alert:**

```yaml
alert: HighDLQRate
expr: |
  (
    rate(clickhouse_rows_inserted{table="streaming_events_dlq"}[5m])
    /
    rate(clickhouse_rows_inserted{table="streaming_events"}[5m])
  ) > 0.01
for: 5m
labels:
  severity: warning
annotations:
  summary: "High DLQ rate detected"
  description: "More than 1% of events are failing parsing"
```

### Logging

**Log Locations:**

```bash
# View all logs
docker-compose logs -f

# Specific service
docker logs -f livepeer-analytics-flink-jobmanager

# Filter for errors
docker-compose logs | grep ERROR

# Save logs to file
docker-compose logs > logs.txt
```

**Important Log Patterns:**

```bash
# Kafka Connect errors
docker logs livepeer-analytics-connect | grep "ERROR"

# Flink job failures
docker logs livepeer-analytics-flink-taskmanager | grep "Exception"

# ClickHouse errors
docker exec livepeer-analytics-clickhouse \
  clickhouse-client --query "SELECT * FROM system.text_log WHERE level = 'Error' ORDER BY event_time DESC LIMIT 10"
```

---

## Disaster Recovery

### Backup Strategy

#### 1. ClickHouse Backups

**Automated Daily Backups:**

```bash
#!/bin/bash
# backup-clickhouse.sh

DATE=$(date +%Y%m%d)
BACKUP_DIR="/backups/clickhouse/$DATE"

docker exec livepeer-analytics-clickhouse \
  clickhouse-client --query "BACKUP DATABASE livepeer_analytics TO Disk('backups', '$DATE')"

echo "Backup completed: $BACKUP_DIR"
```

**Manual Backup:**

```bash
# Freeze tables (create hardlinks)
docker exec livepeer-analytics-clickhouse \
  clickhouse-client --query "ALTER TABLE livepeer_analytics.streaming_events FREEZE"

# Copy frozen data
docker cp livepeer-analytics-clickhouse:/var/lib/clickhouse/shadow ./backup-$(date +%Y%m%d)
```

**Restore:**

```bash
# Stop ClickHouse
docker-compose stop clickhouse

# Restore data
docker cp ./backup-20260120 livepeer-analytics-clickhouse:/var/lib/clickhouse/

# Start ClickHouse
docker-compose start clickhouse
```

#### 2. Kafka Backups

**Not typically needed (MinIO has full archive), but:**

```bash
# Increase retention if needed
docker exec live-video-to-video-kafka \
  /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server kafka:9092 \
  --entity-type topics \
  --entity-name streaming-events \
  --alter --add-config retention.ms=2592000000  # 30 days
```

#### 3. MinIO Backups

**MinIO already IS the backup!**

```bash
# Sync to external S3
docker exec live-video-to-video-minio \
  mc mirror myminio/streaming-events s3://external-backup/livepeer-events
```

### Recovery Scenarios

#### Scenario 1: ClickHouse Data Corruption

**Problem:** ClickHouse table corrupted

**Recovery:**

```bash
# 1. Drop corrupted table
docker exec livepeer-analytics-clickhouse clickhouse-client --query "DROP TABLE livepeer_analytics.streaming_events"

# 2. Recreate table (from schema)
docker exec livepeer-analytics-clickhouse clickhouse-client < configs/clickhouse-init/01-schema.sql

# 3. Replay from MinIO
# (Use custom script to read JSON files and re-publish to Kafka)

# 4. Or restore from backup
docker cp ./backup-20260120/streaming_events livepeer-analytics-clickhouse:/var/lib/clickhouse/data/livepeer_analytics/
```

#### Scenario 2: Complete Data Loss

**Problem:** All Docker volumes deleted

**Recovery:**

```bash
# 1. Restore Docker volumes from backup
cp -r /backups/data ./data

# 2. Restart services
docker-compose up -d

# 3. Verify data
docker exec livepeer-analytics-clickhouse clickhouse-client --query "SELECT count() FROM livepeer_analytics.streaming_events"

# 4. If needed, replay from MinIO archive
# (Custom replay script)
```

#### Scenario 3: Kafka Offset Lost

**Problem:** Flink lost its Kafka offsets

**Recovery:**

```bash
# 1. Reset to earliest (reprocess all)
docker exec livepeer-analytics-flink-jobmanager \
  flink cancel <job-id> --withSavepoint

# 2. Delete consumer group
docker exec live-video-to-video-kafka \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 \
  --group livepeer-analytics-flink \
  --delete

# 3. Resubmit job (will start from earliest offset)
docker-compose restart flink-job-submitter
```

#### Scenario 4: Schema Migration

**Problem:** Need to change ClickHouse schema

**Strategy:**

```sql
-- 1. Create new table with new schema
CREATE TABLE ai_stream_status_v2 (...);

-- 2. Dual-write (Flink writes to both tables)
-- Update Flink job to write to both tables

-- 3. Backfill from old table
INSERT INTO ai_stream_status_v2
SELECT * FROM ai_stream_status;

-- 4. Update Grafana dashboards to use new table

-- 5. Drop old table
DROP TABLE ai_stream_status;
```

### Backup Retention Policy

| Data Type | Retention | Backup Frequency |
|-----------|-----------|------------------|
| ClickHouse data | 90 days (in DB) | Daily full backup |
| ClickHouse backups | 30 days | - |
| Kafka logs | 7 days | Not backed up (MinIO has copy) |
| MinIO archive | Infinite | Sync to external S3 weekly |
| Flink checkpoints | Last 5 | Every 60 seconds |
| Grafana dashboards | Infinite | Git version control |
| Configuration files | Infinite | Git version control |

---

## Security Considerations

### Authentication & Authorization

**ClickHouse:**

```xml
<!-- configs/clickhouse-users/users.xml -->
<users>
  <analytics_user>
    <password>analytics_password</password>
    <networks>
      <ip>::/0</ip>  <!-- Allow all IPs (Docker network) -->
    </networks>
    <grants>
      <query>SELECT, INSERT</query>
      <database>livepeer_analytics</database>
    </grants>
  </analytics_user>
</users>
```

**Production Recommendation:**

```xml
<users>
  <analytics_user>
    <password_sha256_hex><!-- SHA256 hash --></password_sha256_hex>
    <networks>
      <ip>172.18.0.0/16</ip>  <!-- Docker network only -->
    </networks>
    <readonly>0</readonly>
  </analytics_user>
  
  <grafana_user>
    <password_sha256_hex><!-- SHA256 hash --></password_sha256_hex>
    <readonly>1</readonly>  <!-- Read-only for Grafana -->
  </grafana_user>
</users>
```

**Kafka:**

Current: No authentication (internal network only)

Production: Enable SASL/SCRAM:

```yaml
environment:
  KAFKA_LISTENERS: SASL_PLAINTEXT://0.0.0.0:9092
  KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL: SCRAM-SHA-512
  KAFKA_SASL_ENABLED_MECHANISMS: SCRAM-SHA-512
```

**MinIO:**

```yaml
environment:
  MINIO_ROOT_USER: admin  # Change from minioadmin
  MINIO_ROOT_PASSWORD: <strong-password>
```

### Network Security

**Current (Development):**

```yaml
networks:
  default:
    name: live-video-to-video
```

All services accessible within Docker network.

**Production Recommendation:**

```yaml
networks:
  frontend:
    driver: bridge
  backend:
    driver: bridge
    internal: true  # No external access

services:
  grafana:
    networks:
      - frontend  # Accessible from outside
      - backend   # Can access ClickHouse
  
  clickhouse:
    networks:
      - backend  # Only accessible from other services
```

### Data Encryption

**At Rest:**

- ClickHouse: Enable transparent encryption
  ```xml
  <encryption>
    <algorithm>aes_256_ctr</algorithm>
    <key><!-- 256-bit key --></key>
  </encryption>
  ```

- MinIO: Server-side encryption
  ```yaml
  environment:
    MINIO_SERVER_SIDE_ENCRYPTION: "on"
  ```

**In Transit:**

- Kafka: Enable SSL/TLS
  ```yaml
  KAFKA_LISTENERS: SSL://0.0.0.0:9093
  KAFKA_SSL_KEYSTORE_LOCATION: /path/to/keystore
  KAFKA_SSL_TRUSTSTORE_LOCATION: /path/to/truststore
  ```

- ClickHouse: HTTPS
  ```xml
  <https_port>8443</https_port>
  <openSSL>
    <server>
      <certificateFile>/path/to/cert.pem</certificateFile>
      <privateKeyFile>/path/to/key.pem</privateKeyFile>
    </server>
  </openSSL>
  ```

### Secrets Management

**Current (Development):**

Secrets in docker-compose.yml (not recommended for production)

**Production Recommendation:**

Use Docker secrets or external secrets manager:

```yaml
services:
  clickhouse:
    secrets:
      - clickhouse_password
    environment:
      CLICKHOUSE_PASSWORD_FILE: /run/secrets/clickhouse_password

secrets:
  clickhouse_password:
    external: true  # Managed outside Docker Compose
```

Or use HashiCorp Vault, AWS Secrets Manager, etc.

### Audit Logging

**ClickHouse:**

```sql
-- Enable query log
SET log_queries = 1;

-- View audit log
SELECT 
    user,
    query,
    event_time,
    read_rows,
    written_rows
FROM system.query_log
WHERE type = 'QueryFinish'
ORDER BY event_time DESC
LIMIT 100;
```

**Kafka:**

```properties
# Enable audit logs
log.message.format.version=2.8
log.message.timestamp.type=LogAppendTime
```

---

## Future Enhancements

### Phase 2: Advanced Analytics

**1. Real-Time Aggregations**

```sql
-- Materialized views for live dashboards
CREATE MATERIALIZED VIEW orchestrator_live_stats
ENGINE = SummingMergeTree()
ORDER BY (event_date, orchestrator_address)
AS
SELECT 
    event_date,
    orchestrator_address,
    count() as total_streams,
    avg(output_fps) as avg_fps,
    sum(toUInt64(output_fps > 15)) as streams_above_target
FROM ai_stream_status
GROUP BY event_date, orchestrator_address;
```

**2. Anomaly Detection**

```sql
-- Detect orchestrators with sudden FPS drops
WITH baseline AS (
    SELECT 
        orchestrator_address,
        avg(output_fps) as baseline_fps
    FROM ai_stream_status
    WHERE event_timestamp >= now() - INTERVAL 1 HOUR
    GROUP BY orchestrator_address
)
SELECT 
    a.orchestrator_address,
    a.output_fps as current_fps,
    b.baseline_fps,
    (b.baseline_fps - a.output_fps) / b.baseline_fps * 100 as pct_drop
FROM ai_stream_status a
JOIN baseline b ON a.orchestrator_address = b.orchestrator_address
WHERE a.event_timestamp >= now() - INTERVAL 5 MINUTE
  AND (b.baseline_fps - a.output_fps) / b.baseline_fps > 0.20  -- 20% drop
ORDER BY pct_drop DESC;
```

**3. Predictive Analytics**

- Train ML models on historical data to predict:
    - GPU failures before they happen
    - Demand spikes (capacity planning)
    - Optimal pricing

### Phase 3: SLA Enforcement

**1. SLA Scoring Engine**

```sql
-- Calculate SLA score per orchestrator
CREATE VIEW orchestrator_sla_score AS
SELECT 
    orchestrator_address,
    event_date,
    
    -- Performance score (0-100)
    avg(CASE WHEN output_fps >= 15 THEN 100 ELSE output_fps/15*100 END) as fps_score,
    
    -- Reliability score (0-100)
    (1 - sum(restart_count) / count()) * 100 as reliability_score,
    
    -- Overall SLA score
    (fps_score * 0.7 + reliability_score * 0.3) as sla_score
FROM ai_stream_status
WHERE event_date >= today() - 7
GROUP BY orchestrator_address, event_date;
```

**2. Dynamic Orchestrator Selection**

Flink job that reads SLA scores and publishes to Kafka topic for gateway consumption.

**3. Payment Adjustments**

Link SLA scores to payment calculations (higher SLA = higher rewards).

### Phase 4: Multi-Tenancy

**1. Tenant Isolation**

```sql
-- Add tenant_id to all tables
ALTER TABLE streaming_events ADD COLUMN tenant_id String DEFAULT 'default';

-- Create tenant-specific views
CREATE VIEW tenant_a_events AS
SELECT * FROM streaming_events
WHERE tenant_id = 'tenant-a';
```

**2. Tenant Quotas**

```sql
-- Monitor tenant resource usage
SELECT 
    tenant_id,
    count() as events,
    sum(bytes_received) as total_bytes
FROM stream_ingest_metrics
WHERE event_date = today()
GROUP BY tenant_id
HAVING total_bytes > 1000000000;  -- 1GB limit
```

### Phase 5: Edge Analytics

**1. Local Aggregation**

Run lightweight Flink job at gateway to pre-aggregate before sending to central Kafka.

**2. Multi-Region Deployment**

```
Region: US-East
  ├─> Kafka cluster
  ├─> Flink cluster
  └─> ClickHouse cluster

Region: EU-West
  ├─> Kafka cluster
  ├─> Flink cluster
  └─> ClickHouse cluster

Central Aggregation
  └─> ClickHouse Distributed table
      ├─> US-East shard
      └─> EU-West shard
```

### Phase 6: Advanced Visualization

**1. Real-Time Dashboards**

- WebSocket connection to Kafka SSE API
- Live updates without page refresh
- Sub-second latency

**2. Custom Analytics UI**

- React/Vue dashboard
- Direct queries to ClickHouse
- Interactive filtering and drill-down

**3. Mobile App**

- iOS/Android app for monitoring
- Push notifications for alerts
- Real-time metrics

### Phase 7: Machine Learning Integration

**1. Export to ML Platforms**

```bash
# Export to Parquet for Spark/PyTorch
clickhouse-client --query "
  SELECT * FROM ai_stream_status 
  WHERE event_date >= today() - 30
  FORMAT Parquet
" > training_data.parquet
```

**2. Feature Engineering**

```sql
-- Create features for ML models
CREATE MATERIALIZED VIEW ml_features
ENGINE = MergeTree()
ORDER BY (orchestrator_address, event_timestamp)
AS
SELECT 
    orchestrator_address,
    event_timestamp,
    
    -- Rolling averages
    avg(output_fps) OVER (
        PARTITION BY orchestrator_address 
        ORDER BY event_timestamp 
        ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
    ) as fps_1h_avg,
    
    -- Trend indicators
    output_fps - lag(output_fps, 60) OVER (
        PARTITION BY orchestrator_address 
        ORDER BY event_timestamp
    ) as fps_1h_change
FROM ai_stream_status;
```

---

## Appendices

### Appendix A: Configuration Files Reference

**Key Configuration Files:**

```
configs/
├── connect/
│   ├── clickhouse-sink.json          # ClickHouse Kafka Connect sink
│   └── minio-sink.json                # MinIO S3 sink
├── clickhouse-init/
│   └── 01-schema.sql                  # Database schema
├── clickhouse-users/
│   └── users.xml                      # User permissions
├── grafana/provisioning/
│   ├── datasources/
│   │   └── clickhouse.yaml           # ClickHouse datasource
│   └── dashboards/
│       └── naap-overview.json        # Main dashboard
└── zilla/
    └── zilla.yaml                     # Kafka SSE proxy
```

### Appendix B: SQL Query Library

**Common Queries:**

See separate file: `QUERY_LIBRARY.sql`

### Appendix C: Troubleshooting Guide

**Common Issues:**

| Issue | Symptom | Solution |
|-------|---------|----------|
| No data in ClickHouse | Count = 0 | Check Kafka Connect status, verify table name mapping |
| High Kafka lag | Lag > 1000 | Increase Flink parallelism, add task managers |
| Slow queries | Query > 10s | Add indexes, use materialized views, optimize ORDER BY |
| DLQ filling up | High error rate | Check schema changes, review sample errors |
| Disk full | > 95% | Reduce TTL, increase disk, clean old partitions |

### Appendix D: Performance Benchmarks

**Test Environment:**

- **Hardware:** 8 CPU cores, 16GB RAM, SSD storage
- **Load:** 500 events/sec sustained
- **Duration:** 24 hours

**Results:**

| Metric | Value |
|--------|-------|
| Total events processed | 43,200,000 |
| Average E2E latency | 2.8 seconds |
| P95 E2E latency | 4.2 seconds |
| P99 E2E latency | 6.1 seconds |
| ClickHouse write throughput | 15,000 rows/sec |
| ClickHouse storage used | 42 GB (compressed) |
| Query p95 latency | 180ms |
| CPU utilization (avg) | 45% |
| Memory utilization (avg) | 68% |

---

## Glossary

**DLQ (Dead Letter Queue):** Table that captures events that failed processing, preventing pipeline blockage.

**E2E Latency:** End-to-end latency from event generation to queryable in ClickHouse.

**Materialized View:** ClickHouse view that pre-computes aggregations for fast queries.

**Partitioning:** Splitting data into time-based chunks (e.g., by month) for efficient querying.

**ReplacingMergeTree:** ClickHouse engine that deduplicates rows with same key, keeping latest version.

**SLA (Service Level Agreement):** Commitment to specific performance metrics (e.g., 99.9% uptime, < 5s latency).

**TTL (Time To Live):** Automatic data deletion after specified time period.

---

## Document Version History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2025-11-15 | Initial team | Initial architecture |
| 2.0 | 2026-01-20 | Engineering | Complete rewrite, simplified architecture, added Kafka Connect, removed enrichment |
