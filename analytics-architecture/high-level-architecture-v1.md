Excellent! With that context, I think **Flink is absolutely the right choice** for your use case. Let me break down why and address each of your points:

## 1. Flink for Stream Processing

### Why Flink Makes Sense for Your Use Case

**Strong Pros:**
- **Event-time processing with watermarks**: Critical for your out-of-order events (network delays, batch scraping)
- **Complex event correlation**: Perfect for joining `stream_trace` events by `request_id` to calculate prompt-to-first-frame latency
- **Stateful windowing**: Built-in support for calculating metrics like jitter coefficient (σ/μ over time windows)
- **Exactly-once semantics**: With Kafka source/sink, guarantees no duplicates or data loss
- **SQL + DataStream API**: Can start with SQL for simple transforms, drop to DataStream for complex correlation logic
- **Savepoints/checkpointing**: Replayability and auditability built-in
- **Scalability**: Handles 10s → 100s → 1000s events/sec seamlessly

**Cons to Consider:**
- **Operational complexity**: More moving parts than Kafka Streams (JobManager, TaskManagers, state backend)
- **Learning curve**: Windowing, watermarks, and state management have nuances
- **Resource overhead**: Needs more memory/CPU than simpler options for small workloads

**Mitigation**: You have the skills, and the MVP can run on modest hardware. The complexity pays off immediately with your correlation needs.

### Kafka Streams vs Flink for Your Case

| Aspect | Kafka Streams | Flink |
|--------|--------------|-------|
| Event correlation | Possible but awkward with joins | Natural with CoProcessFunction, interval joins |
| Complex windowing | Limited (tumbling, hopping, session) | Rich (sliding, custom triggers, event-time) |
| Late data handling | Basic grace period | Sophisticated watermarks + allowed lateness |
| Multi-stream joins | Verbose KStream/KTable joins | Clean CoProcessFunction or SQL JOINs |
| Operational model | Embedded library (simpler) | Separate cluster (more powerful) |

**Verdict**: For calculating metrics like "prompt-to-first-frame latency" (requires correlating 2+ events from `stream_trace` by `request_id`), Flink's `CoProcessFunction` or interval joins are far more natural than Kafka Streams.

---

## 2. Storage Layer: ClickHouse + MinIO (S3/Parquet)

This is an **excellent production pattern**. Here's the architecture:

### Best Practices Architecture

```
Flink → ClickHouse (hot data, 60-90 days)
  ↓
MinIO/S3 (cold data, Parquet, indefinite retention)
  ↑
ClickHouse (can query S3 via S3 table function)
```

### ClickHouse Pros:
- **Columnar storage**: Compress time-series data 10-100x
- **Fast aggregations**: Sub-second queries on billions of rows
- **SQL interface**: Easy for dashboard/API teams
- **MaterializedViews**: Pre-aggregate metrics (e.g., 5-min averages) automatically
- **TTL policies**: Auto-move old data to cold storage or delete

### ClickHouse Cons:
- **No transactions**: Can't do multi-table ACID (not needed for append-only analytics)
- **Updates are expensive**: Designed for immutable data (your streaming events are immutable ✓)
- **Eventual consistency**: Replica lag in distributed setups (not a concern for single-node MVP)

### MinIO/S3 + Parquet Best Practices:

**Pattern 1: Dual-write from Flink**
```
Flink Job
├─ Sink 1: ClickHouse (hot path, fast queries)
└─ Sink 2: MinIO/S3 as Parquet (cold path, archival, replay)
```

**Pattern 2: ClickHouse TTL to S3**
```
ClickHouse → (after 90 days) → Export to MinIO as Parquet
                                 ↓
                    ClickHouse can query via S3 table function
```

**Recommendation**: Start with **Pattern 1** (dual-write) because:
- Guarantees you have raw events in S3 for replay/audit from day 1
- Flink controls partitioning (e.g., by date/hour for efficient queries later)
- ClickHouse TTL to S3 is possible but adds complexity

### Parquet Schema Design:
- Partition by `event_date` and `event_type` (e.g., `s3://bucket/events/date=2026-01-14/type=ai_stream_status/`)
- This enables fast "replay events for Jan 14" or "analyze only ai_stream_status events"

### Pros/Cons Summary:

| Approach | Pros | Cons |
|----------|------|------|
| **ClickHouse only** | Simplest to start | Data loss risk, expensive long-term storage |
| **ClickHouse + S3 (dual-write)** | Best of both worlds, replayable | More Flink sinks to manage |
| **ClickHouse TTL → S3** | Automatic archival | Harder to replay, schema changes tricky |

**Verdict**: Dual-write to ClickHouse + MinIO from day 1. You get fast queries (ClickHouse) and durable audit trail (S3/Parquet) with minimal added complexity.

---

## 3. Event Correlation in Flink

This is where Flink shines. Let's use a concrete example from your data:

### Example: Calculate Prompt-to-First-Frame Latency

**Requirement**: Join two events by `request_id`:
```json
// Event 1: gateway_receive_stream_request
{"type": "stream_trace", "data": {"type": "gateway_receive_stream_request", "request_id": "ce6ee475", "timestamp": 1767966622124}}

// Event 2: gateway_receive_first_processed_segment
{"type": "stream_trace", "data": {"type": "gateway_receive_first_processed_segment", "request_id": "ce6ee475", "timestamp": 1767966625424}}

// Desired output: Latency = 3300ms
```

### Flink Approach: Interval Join

```java
// Pseudocode
DataStream<Event> streamTraceEvents = ...;

DataStream<Event> receiveRequest = streamTraceEvents
  .filter(e -> e.data.type.equals("gateway_receive_stream_request"))
  .keyBy(e -> e.data.request_id);

DataStream<Event> receiveFirstSegment = streamTraceEvents
  .filter(e -> e.data.type.equals("gateway_receive_first_processed_segment"))
  .keyBy(e -> e.data.request_id);

DataStream<LatencyMetric> latencies = receiveRequest
  .intervalJoin(receiveFirstSegment)
  .between(Time.seconds(-10), Time.seconds(60)) // Request can arrive up to 60s before first segment
  .process(new CalculateLatency());
```

### Best Practices:

1. **Event-time semantics**: Assign timestamps from `event.timestamp` field, not processing time
2. **Watermarks**: Generate watermarks to handle late/out-of-order events (e.g., "watermark = max_timestamp - 10 seconds")
3. **State TTL**: Clean up unmatched events after some time (e.g., if no match in 5 minutes, discard)
4. **Keyed state**: Key by `request_id`, `stream_id`, `orchestrator_address` as needed

### Main Concerns:

| Concern | Mitigation |
|---------|-----------|
| **Missing events** (one side of join never arrives) | Set TTL on state; emit "unmatched" metrics to detect gaps |
| **Out-of-order events** | Use watermarks with appropriate delay (e.g., 10-30s) |
| **State growth** (unbounded keys like `stream_id`) | State TTL, compaction, monitor state size |
| **Complex multi-way joins** (3+ events) | Use `CoProcessFunction` with multiple states or chained jobs |

### Pros/Cons of Flink Correlation:

**Pros:**
- Handles event-time and watermarks natively
- Interval joins are built for exactly this use case
- State backend (RocksDB) can handle millions of in-flight keys
- Savepoints let you replay and debug correlation logic

**Cons:**
- State management requires tuning (heap vs RocksDB, checkpointing frequency)
- Complex joins can be hard to debug ("why didn't these events match?")
- Need monitoring for watermark progress and state size

**Verdict**: Flink's event-time processing and interval joins are **purpose-built** for your use case. The state management complexity is manageable with good monitoring.

---

## 4. Orchestration: Docker → Kubernetes

### Docker Compose for MVP

**Best Practices:**

1. **Service Topology:**
```yaml
# docker-compose.yml
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:latest
  
  kafka:
    image: confluentinc/cp-kafka:latest
    depends_on: [zookeeper]
  
  flink-jobmanager:
    image: flink:1.18
    environment:
      - JOB_MANAGER_RPC_ADDRESS=flink-jobmanager
  
  flink-taskmanager:
    image: flink:1.18
    depends_on: [flink-jobmanager]
    environment:
      - JOB_MANAGER_RPC_ADDRESS=flink-jobmanager
  
  clickhouse:
    image: clickhouse/clickhouse-server:latest
    volumes:
      - ./clickhouse-data:/var/lib/clickhouse
  
  minio:
    image: minio/minio:latest
    command: server /data --console-address ":9001"
```

2. **Volume Management:**
    - Persistent volumes for ClickHouse data, Flink checkpoints, MinIO storage
    - Use named volumes (not bind mounts) for easier management

3. **Networking:**
    - Single Docker network for all services
    - Expose only necessary ports (Kafka 9092, ClickHouse 8123, MinIO 9000, Flink UI 8081)

### When to Move to Kubernetes

**Triggers:**
- Need multi-node Flink cluster (TaskManager scaling)
- High availability requirements (multiple JobManagers)
- Advanced networking (service mesh, ingress)
- Production workloads with SLAs

**Not Needed for MVP**: Docker Compose can easily handle 100s events/sec on a single beefy machine.

### Data Lineage

**What is it?**: Tracking data flow from source → transformations → destination

**Why it matters for you:**
- **Auditability**: "Where did this metric come from?"
- **Debugging**: "Why is jitter coefficient wrong for stream X?"
- **Compliance**: Foundation requirement for "verifiable test load results"

**Options:**

| Tool | Pros | Cons |
|------|------|------|
| **OpenLineage** | Standard, integrates with Flink/dbt | Requires separate backend (Marquez) |
| **Flink's built-in metrics** | Free, already there | Limited to job-level, not record-level |
| **dbt docs** (if using dbt) | Great for SQL transformations | Doesn't cover Flink jobs |
| **Custom logs** | Simple, full control | Manual effort, not standardized |

**MVP Recommendation**: **Skip for now**. Focus on good logging and Flink job descriptions. Add OpenLineage in Phase 2 if auditability becomes critical.

**If you must have it now**: Add OpenLineage with Marquez (another Docker service). Flink has OpenLineage integration.

---

## Recommended MVP Architecture

Here's the full stack:

```
┌─────────────────────────────────────────────────────┐
│  Data Sources                                       │
│  ├─ go-livepeer gateways → Kafka                   │
│  └─ Batch scraper → Kafka                          │
└─────────────────────────────────────────────────────┘
                     │
                     ▼
              ┌─────────────┐
              │   Kafka     │ (event log, 7-day retention)
              └─────────────┘
                     │
                     ▼
         ┌──────────────────────────┐
         │   Flink Cluster          │
         │  ┌──────────────────┐    │
         │  │  Job 1: Cleanse  │    │ (Parse, validate, enrich)
         │  │  & Enrich        │    │
         │  └──────────────────┘    │
         │           │               │
         │           ▼               │
         │  ┌──────────────────┐    │
         │  │  Job 2: Correlate│    │ (Join events for latency, etc.)
         │  │  & Calculate     │    │
         │  └──────────────────┘    │
         │           │               │
         └───────────┼───────────────┘
                     │
          ┌──────────┴──────────┐
          ▼                     ▼
    ┌──────────┐          ┌─────────┐
    │ClickHouse│          │ MinIO/S3│
    │ (hot)    │          │ (Parquet│
    │ 60-90d   │          │  cold)  │
    └──────────┘          └─────────┘
          │                     │
          ▼                     ▼
    ┌──────────────────────────────┐
    │  Query Layer                 │
    │  ├─ REST APIs (Go/Python)    │
    │  └─ Dashboards (Grafana)     │
    └──────────────────────────────┘
```

### Flink Job Design

**Job 1: Cleanse & Enrich**
- Input: Raw Kafka events
- Tasks:
    - Parse JSON
    - Validate schema
    - Enrich with lookups (e.g., orchestrator metadata)
    - Add event_date partition field
- Output: Cleansed events → Kafka topic + MinIO (Parquet)

**Job 2: Correlate & Calculate Metrics**
- Input: Cleansed events from Job 1
- Tasks:
    - Interval joins for latency calculations
    - Windowed aggregations for jitter coefficient
    - Calculate SLA scores
- Output: Metrics → ClickHouse + MinIO (Parquet)

### Why Split into 2 Jobs?

- **Replayability**: Can replay Job 2 without re-ingesting raw data
- **Performance**: Job 1 is stateless (fast), Job 2 is stateful (needs tuning)
- **Debugging**: Easier to isolate issues in correlation logic

---

## Key Decisions Summary

| Component | Choice | Reasoning |
|-----------|--------|-----------|
| **Stream Processing** | Flink | Complex event correlation, event-time semantics, production-grade |
| **Storage (Hot)** | ClickHouse | Fast analytics queries, time-series optimized |
| **Storage (Cold)** | MinIO/S3 + Parquet | Replayability, auditability, cost-effective archival |
| **Orchestration** | Docker Compose (MVP) | Simple, sufficient for 100s events/sec |
| **Data Lineage** | Skip for MVP | Add OpenLineage later if needed |

---

## Next Steps

1. **Prototype the Flink job** for a single metric (e.g., prompt-to-first-frame latency)
2. **Set up Docker Compose** with Kafka, Flink, ClickHouse, MinIO
3. **Define ClickHouse schema** for metrics tables (we can design this together)
4. **Test dual-write** to ClickHouse + MinIO from Flink
5. **Build a simple dashboard** (Grafana or custom) to visualize one metric

**Should we start with step 3 (ClickHouse schema design)? Or do you want to debate any of these choices further?**