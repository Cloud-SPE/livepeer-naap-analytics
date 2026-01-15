# ClickHouse Queries for NaaP Derived Metrics

## Overview
This document provides ClickHouse queries to calculate key NaaP metrics from the raw streaming events data.

---

## 1. Jitter Coefficient (σ/μ of FPS)

### What it measures
The Jitter Coefficient measures the stability of the output FPS over time. Lower values indicate more stable performance.

**Formula:** `Jitter Coefficient = σ(fps) / μ(fps)`
- σ = Standard deviation of FPS
- μ = Mean FPS

**Target:** ≤ 0.1 (10% variation)

### Query 1.1: Real-time Jitter Coefficient (Last 5 Minutes)

```sql
-- Per orchestrator, per pipeline, last 5 minutes
SELECT
    orchestrator_address,
    pipeline,
    COUNT(*) as sample_count,
    AVG(output_fps) as mean_fps,
    stddevPop(output_fps) as stddev_fps,
    stddevPop(output_fps) / AVG(output_fps) as jitter_coefficient,
    MIN(output_fps) as min_fps,
    MAX(output_fps) as max_fps,
    quantile(0.5)(output_fps) as median_fps,
    quantile(0.95)(output_fps) as p95_fps
FROM ai_stream_status
WHERE 
    event_timestamp >= now() - INTERVAL 5 MINUTE
    AND output_fps > 0  -- Filter out zero FPS samples
GROUP BY 
    orchestrator_address,
    pipeline
HAVING 
    sample_count >= 5  -- Need at least 5 samples for meaningful stats
ORDER BY 
    jitter_coefficient DESC
LIMIT 50;
```

### Query 1.2: Jitter Coefficient Over Time (Hourly Rollup)

```sql
-- Hourly jitter coefficient for trend analysis
SELECT
    toStartOfHour(event_timestamp) as hour,
    orchestrator_address,
    pipeline,
    COUNT(*) as sample_count,
    AVG(output_fps) as mean_fps,
    stddevPop(output_fps) / AVG(output_fps) as jitter_coefficient,
    quantile(0.95)(output_fps) as p95_fps
FROM ai_stream_status
WHERE 
    event_timestamp >= now() - INTERVAL 24 HOUR
    AND output_fps > 0
GROUP BY 
    hour,
    orchestrator_address,
    pipeline
HAVING 
    sample_count >= 10
ORDER BY 
    hour DESC,
    jitter_coefficient DESC;
```

### Query 1.3: Per-Stream Jitter Coefficient

```sql
-- Jitter coefficient for individual streams
SELECT
    stream_id,
    orchestrator_address,
    pipeline,
    MIN(event_timestamp) as stream_start,
    MAX(event_timestamp) as stream_end,
    COUNT(*) as sample_count,
    AVG(output_fps) as mean_fps,
    stddevPop(output_fps) as stddev_fps,
    stddevPop(output_fps) / AVG(output_fps) as jitter_coefficient,
    -- Additional context
    MIN(output_fps) as min_fps,
    MAX(output_fps) as max_fps,
    SUM(restart_count) as total_restarts,
    arrayStringConcat(groupUniqArray(state), ', ') as states_seen
FROM ai_stream_status
WHERE 
    event_timestamp >= now() - INTERVAL 1 HOUR
    AND output_fps > 0
GROUP BY 
    stream_id,
    orchestrator_address,
    pipeline
HAVING 
    sample_count >= 5
    AND jitter_coefficient > 0.1  -- Only show problematic streams
ORDER BY 
    jitter_coefficient DESC
LIMIT 100;
```

### Query 1.4: GPU-Level Jitter (When GPU_ID Available)

```sql
-- Per GPU jitter coefficient (requires gpu_id field to be populated)
SELECT
    orchestrator_address,
    gpu_id,
    pipeline,
    COUNT(*) as sample_count,
    COUNT(DISTINCT stream_id) as unique_streams,
    AVG(output_fps) as mean_fps,
    stddevPop(output_fps) / AVG(output_fps) as jitter_coefficient,
    quantile(0.95)(output_fps) as p95_fps,
    quantile(0.05)(output_fps) as p05_fps
FROM ai_stream_status
WHERE 
    event_timestamp >= now() - INTERVAL 1 HOUR
    AND output_fps > 0
    AND gpu_id IS NOT NULL  -- Only when GPU attribution is available
GROUP BY 
    orchestrator_address,
    gpu_id,
    pipeline
HAVING 
    sample_count >= 10
ORDER BY 
    jitter_coefficient DESC;
```

### Query 1.5: Network-Wide Jitter Benchmark

```sql
-- Network-wide jitter statistics per pipeline
SELECT
    pipeline,
    COUNT(DISTINCT orchestrator_address) as num_orchestrators,
    COUNT(DISTINCT stream_id) as num_streams,
    COUNT(*) as total_samples,
    -- Overall network stats
    AVG(output_fps) as network_mean_fps,
    stddevPop(output_fps) / AVG(output_fps) as network_jitter_coefficient,
    -- Percentile distribution
    quantile(0.05)(output_fps) as p05_fps,
    quantile(0.25)(output_fps) as p25_fps,
    quantile(0.5)(output_fps) as p50_fps,
    quantile(0.75)(output_fps) as p75_fps,
    quantile(0.95)(output_fps) as p95_fps,
    quantile(0.99)(output_fps) as p99_fps
FROM ai_stream_status
WHERE 
    event_timestamp >= now() - INTERVAL 1 HOUR
    AND output_fps > 0
GROUP BY 
    pipeline
ORDER BY 
    pipeline;
```

---

## 2. E2E Stream Latency (Prompt-to-First-Frame)

### What it measures
The time from when a stream request arrives at the gateway until the first processed frame is received back.

**Calculation:** `gateway_receive_first_processed_segment.timestamp - gateway_receive_stream_request.timestamp`

**Target:** ≤ 3000ms (3 seconds)

### Query 2.1: Calculate E2E Latency per Stream

```sql
-- Calculate prompt-to-first-frame latency for each stream
WITH request_times AS (
    SELECT
        request_id,
        stream_id,
        orchestrator_address,
        MIN(data_timestamp) as request_received_at
    FROM stream_trace_events
    WHERE 
        trace_type = 'gateway_receive_stream_request'
        AND event_timestamp >= now() - INTERVAL 1 HOUR
    GROUP BY request_id, stream_id, orchestrator_address
),
first_frame_times AS (
    SELECT
        request_id,
        stream_id,
        MIN(data_timestamp) as first_frame_received_at
    FROM stream_trace_events
    WHERE 
        trace_type = 'gateway_receive_first_processed_segment'
        AND event_timestamp >= now() - INTERVAL 1 HOUR
    GROUP BY request_id, stream_id
)
SELECT
    r.request_id,
    r.stream_id,
    r.orchestrator_address,
    r.request_received_at,
    f.first_frame_received_at,
    f.first_frame_received_at - r.request_received_at as latency_ms,
    -- Classify latency
    CASE
        WHEN f.first_frame_received_at - r.request_received_at <= 1000 THEN 'excellent'
        WHEN f.first_frame_received_at - r.request_received_at <= 2000 THEN 'good'
        WHEN f.first_frame_received_at - r.request_received_at <= 3000 THEN 'acceptable'
        ELSE 'poor'
    END as latency_class
FROM request_times r
INNER JOIN first_frame_times f ON r.request_id = f.request_id
WHERE f.first_frame_received_at - r.request_received_at > 0  -- Sanity check
ORDER BY latency_ms DESC
LIMIT 100;
```

### Query 2.2: E2E Latency Statistics per Orchestrator

```sql
-- Aggregate E2E latency statistics per orchestrator
WITH latencies AS (
    SELECT
        r.request_id,
        r.stream_id,
        r.orchestrator_address,
        f.data_timestamp - r.data_timestamp as latency_ms
    FROM (
        SELECT request_id, stream_id, orchestrator_address, MIN(data_timestamp) as data_timestamp
        FROM stream_trace_events
        WHERE trace_type = 'gateway_receive_stream_request'
            AND event_timestamp >= now() - INTERVAL 1 HOUR
        GROUP BY request_id, stream_id, orchestrator_address
    ) r
    INNER JOIN (
        SELECT request_id, stream_id, MIN(data_timestamp) as data_timestamp
        FROM stream_trace_events
        WHERE trace_type = 'gateway_receive_first_processed_segment'
            AND event_timestamp >= now() - INTERVAL 1 HOUR
        GROUP BY request_id, stream_id
    ) f ON r.request_id = f.request_id
    WHERE f.data_timestamp - r.data_timestamp > 0
        AND f.data_timestamp - r.data_timestamp < 30000  -- Ignore outliers > 30s
)
SELECT
    orchestrator_address,
    COUNT(*) as num_streams,
    AVG(latency_ms) as avg_latency_ms,
    quantile(0.5)(latency_ms) as p50_latency_ms,
    quantile(0.95)(latency_ms) as p95_latency_ms,
    quantile(0.99)(latency_ms) as p99_latency_ms,
    MIN(latency_ms) as min_latency_ms,
    MAX(latency_ms) as max_latency_ms,
    -- SLA compliance
    countIf(latency_ms <= 3000) as streams_meeting_sla,
    countIf(latency_ms <= 3000) / COUNT(*) as sla_compliance_rate
FROM latencies
GROUP BY orchestrator_address
ORDER BY avg_latency_ms ASC;
```

### Query 2.3: E2E Latency Over Time (Hourly)

```sql
-- Track E2E latency trends over time
WITH latencies AS (
    SELECT
        toStartOfHour(r.event_timestamp) as hour,
        r.orchestrator_address,
        f.data_timestamp - r.data_timestamp as latency_ms
    FROM (
        SELECT 
            request_id, 
            orchestrator_address, 
            MIN(event_timestamp) as event_timestamp,
            MIN(data_timestamp) as data_timestamp
        FROM stream_trace_events
        WHERE trace_type = 'gateway_receive_stream_request'
            AND event_timestamp >= now() - INTERVAL 24 HOUR
        GROUP BY request_id, orchestrator_address
    ) r
    INNER JOIN (
        SELECT request_id, MIN(data_timestamp) as data_timestamp
        FROM stream_trace_events
        WHERE trace_type = 'gateway_receive_first_processed_segment'
            AND event_timestamp >= now() - INTERVAL 24 HOUR
        GROUP BY request_id
    ) f ON r.request_id = f.request_id
    WHERE f.data_timestamp - r.data_timestamp > 0
        AND f.data_timestamp - r.data_timestamp < 30000
)
SELECT
    hour,
    orchestrator_address,
    COUNT(*) as num_streams,
    AVG(latency_ms) as avg_latency_ms,
    quantile(0.5)(latency_ms) as p50_latency_ms,
    quantile(0.95)(latency_ms) as p95_latency_ms,
    countIf(latency_ms <= 3000) / COUNT(*) as sla_compliance_rate
FROM latencies
GROUP BY hour, orchestrator_address
ORDER BY hour DESC, avg_latency_ms ASC;
```

### Query 2.4: Detailed Latency Breakdown (All Trace Points)

```sql
-- Detailed timing breakdown across all trace points
WITH trace_pivot AS (
    SELECT
        request_id,
        stream_id,
        orchestrator_address,
        anyIf(data_timestamp, trace_type = 'gateway_receive_stream_request') as t1_request,
        anyIf(data_timestamp, trace_type = 'runner_send_first_processed_segment') as t2_runner_send,
        anyIf(data_timestamp, trace_type = 'gateway_send_first_ingest_segment') as t3_gateway_send,
        anyIf(data_timestamp, trace_type = 'runner_receive_first_ingest_segment') as t4_runner_receive,
        anyIf(data_timestamp, trace_type = 'gateway_receive_first_processed_segment') as t5_gateway_receive
    FROM stream_trace_events
    WHERE event_timestamp >= now() - INTERVAL 1 HOUR
    GROUP BY request_id, stream_id, orchestrator_address
    HAVING t1_request > 0 AND t5_gateway_receive > 0
)
SELECT
    request_id,
    stream_id,
    orchestrator_address,
    -- Total E2E latency
    t5_gateway_receive - t1_request as total_latency_ms,
    -- Breakdown
    t3_gateway_send - t1_request as gateway_processing_ms,
    t4_runner_receive - t3_gateway_send as network_to_runner_ms,
    t2_runner_send - t4_runner_receive as runner_processing_ms,
    t5_gateway_receive - t2_runner_send as network_from_runner_ms,
    -- Timestamps for reference
    t1_request,
    t5_gateway_receive
FROM trace_pivot
WHERE 
    t5_gateway_receive - t1_request > 0
    AND t5_gateway_receive - t1_request < 30000
ORDER BY total_latency_ms DESC
LIMIT 100;
```

### Query 2.5: Network-Wide Latency Benchmark

```sql
-- Overall network latency statistics
WITH latencies AS (
    SELECT
        f.data_timestamp - r.data_timestamp as latency_ms
    FROM (
        SELECT request_id, MIN(data_timestamp) as data_timestamp
        FROM stream_trace_events
        WHERE trace_type = 'gateway_receive_stream_request'
            AND event_timestamp >= now() - INTERVAL 1 HOUR
        GROUP BY request_id
    ) r
    INNER JOIN (
        SELECT request_id, MIN(data_timestamp) as data_timestamp
        FROM stream_trace_events
        WHERE trace_type = 'gateway_receive_first_processed_segment'
            AND event_timestamp >= now() - INTERVAL 1 HOUR
        GROUP BY request_id
    ) f ON r.request_id = f.request_id
    WHERE f.data_timestamp - r.data_timestamp > 0
        AND f.data_timestamp - r.data_timestamp < 30000
)
SELECT
    COUNT(*) as total_streams,
    AVG(latency_ms) as avg_latency_ms,
    stddevPop(latency_ms) as stddev_latency_ms,
    -- Percentiles
    quantile(0.05)(latency_ms) as p05_latency_ms,
    quantile(0.25)(latency_ms) as p25_latency_ms,
    quantile(0.5)(latency_ms) as p50_latency_ms,
    quantile(0.75)(latency_ms) as p75_latency_ms,
    quantile(0.95)(latency_ms) as p95_latency_ms,
    quantile(0.99)(latency_ms) as p99_latency_ms,
    -- SLA compliance
    countIf(latency_ms <= 1000) as excellent_count,
    countIf(latency_ms <= 2000) as good_count,
    countIf(latency_ms <= 3000) as acceptable_count,
    countIf(latency_ms > 3000) as poor_count,
    countIf(latency_ms <= 3000) / COUNT(*) as sla_compliance_rate
FROM latencies;
```

---

## 3. Combined SLA Dashboard Query

### Query 3.1: Comprehensive SLA Scorecard per Orchestrator

```sql
-- Complete SLA metrics for each orchestrator
WITH 
-- Jitter calculation
jitter_stats AS (
    SELECT
        orchestrator_address,
        pipeline,
        COUNT(*) as fps_sample_count,
        AVG(output_fps) as mean_fps,
        stddevPop(output_fps) / AVG(output_fps) as jitter_coefficient,
        quantile(0.95)(output_fps) as p95_fps,
        SUM(restart_count) as total_restarts
    FROM ai_stream_status
    WHERE 
        event_timestamp >= now() - INTERVAL 1 HOUR
        AND output_fps > 0
    GROUP BY orchestrator_address, pipeline
    HAVING fps_sample_count >= 10
),
-- E2E latency calculation
latency_stats AS (
    SELECT
        r.orchestrator_address,
        COUNT(*) as stream_count,
        AVG(f.data_timestamp - r.data_timestamp) as avg_latency_ms,
        quantile(0.95)(f.data_timestamp - r.data_timestamp) as p95_latency_ms,
        countIf(f.data_timestamp - r.data_timestamp <= 3000) / COUNT(*) as latency_sla_rate
    FROM (
        SELECT request_id, orchestrator_address, MIN(data_timestamp) as data_timestamp
        FROM stream_trace_events
        WHERE trace_type = 'gateway_receive_stream_request'
            AND event_timestamp >= now() - INTERVAL 1 HOUR
        GROUP BY request_id, orchestrator_address
    ) r
    INNER JOIN (
        SELECT request_id, MIN(data_timestamp) as data_timestamp
        FROM stream_trace_events
        WHERE trace_type = 'gateway_receive_first_processed_segment'
            AND event_timestamp >= now() - INTERVAL 1 HOUR
        GROUP BY request_id
    ) f ON r.request_id = f.request_id
    WHERE f.data_timestamp - r.data_timestamp > 0
        AND f.data_timestamp - r.data_timestamp < 30000
    GROUP BY r.orchestrator_address
)
SELECT
    j.orchestrator_address,
    j.pipeline,
    -- Performance metrics
    j.mean_fps,
    j.p95_fps,
    j.jitter_coefficient,
    CASE 
        WHEN j.jitter_coefficient <= 0.1 THEN '✅ Excellent'
        WHEN j.jitter_coefficient <= 0.2 THEN '⚠️  Acceptable'
        ELSE '❌ Poor'
    END as jitter_status,
    -- Latency metrics
    l.avg_latency_ms,
    l.p95_latency_ms,
    l.latency_sla_rate,
    CASE 
        WHEN l.avg_latency_ms <= 2000 THEN '✅ Excellent'
        WHEN l.avg_latency_ms <= 3000 THEN '⚠️  Acceptable'
        ELSE '❌ Poor'
    END as latency_status,
    -- Reliability
    j.total_restarts,
    -- Sample sizes
    j.fps_sample_count,
    l.stream_count,
    -- Overall SLA score (0-100)
    ROUND(
        (
            -- Jitter component (40%)
            (CASE WHEN j.jitter_coefficient <= 0.1 THEN 40 
                  WHEN j.jitter_coefficient <= 0.2 THEN 20 
                  ELSE 0 END) +
            -- Latency component (40%)
            (CASE WHEN l.avg_latency_ms <= 2000 THEN 40 
                  WHEN l.avg_latency_ms <= 3000 THEN 20 
                  ELSE 0 END) +
            -- Reliability component (20%)
            (CASE WHEN j.total_restarts = 0 THEN 20 
                  WHEN j.total_restarts <= 2 THEN 10 
                  ELSE 0 END)
        ), 0
    ) as sla_score
FROM jitter_stats j
LEFT JOIN latency_stats l ON j.orchestrator_address = l.orchestrator_address
ORDER BY sla_score DESC, j.jitter_coefficient ASC;
```

---

## 4. Alerting Queries

### Query 4.1: High Jitter Alert

```sql
-- Orchestrators with jitter coefficient above threshold
SELECT
    orchestrator_address,
    pipeline,
    COUNT(*) as sample_count,
    AVG(output_fps) as mean_fps,
    stddevPop(output_fps) / AVG(output_fps) as jitter_coefficient,
    COUNT(DISTINCT stream_id) as affected_streams
FROM ai_stream_status
WHERE 
    event_timestamp >= now() - INTERVAL 15 MINUTE
    AND output_fps > 0
GROUP BY 
    orchestrator_address,
    pipeline
HAVING 
    sample_count >= 10
    AND jitter_coefficient > 0.2  -- Alert threshold
ORDER BY 
    jitter_coefficient DESC;
```

### Query 4.2: High Latency Alert

```sql
-- Orchestrators with latency above threshold
WITH latencies AS (
    SELECT
        r.orchestrator_address,
        f.data_timestamp - r.data_timestamp as latency_ms
    FROM (
        SELECT request_id, orchestrator_address, MIN(data_timestamp) as data_timestamp
        FROM stream_trace_events
        WHERE trace_type = 'gateway_receive_stream_request'
            AND event_timestamp >= now() - INTERVAL 15 MINUTE
        GROUP BY request_id, orchestrator_address
    ) r
    INNER JOIN (
        SELECT request_id, MIN(data_timestamp) as data_timestamp
        FROM stream_trace_events
        WHERE trace_type = 'gateway_receive_first_processed_segment'
            AND event_timestamp >= now() - INTERVAL 15 MINUTE
        GROUP BY request_id
    ) f ON r.request_id = f.request_id
    WHERE f.data_timestamp - r.data_timestamp > 0
)
SELECT
    orchestrator_address,
    COUNT(*) as stream_count,
    AVG(latency_ms) as avg_latency_ms,
    quantile(0.95)(latency_ms) as p95_latency_ms,
    MAX(latency_ms) as max_latency_ms
FROM latencies
GROUP BY orchestrator_address
HAVING 
    avg_latency_ms > 3000  -- Alert threshold
    OR p95_latency_ms > 5000
ORDER BY 
    avg_latency_ms DESC;
```

---

## 5. Materialized Views (Optional - for Performance)

### Create Materialized View for Jitter (5-minute aggregates)

```sql
-- Create materialized view for pre-aggregated jitter stats
CREATE MATERIALIZED VIEW IF NOT EXISTS ai_stream_jitter_5min_mv
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(time_bucket)
ORDER BY (time_bucket, orchestrator_address, pipeline)
POPULATE
AS
SELECT
    toStartOfInterval(event_timestamp, INTERVAL 5 MINUTE) as time_bucket,
    orchestrator_address,
    pipeline,
    gpu_id,
    region,
    COUNT(*) as sample_count,
    AVG(output_fps) as avg_fps,
    stddevPop(output_fps) as stddev_fps,
    stddevPop(output_fps) / AVG(output_fps) as jitter_coefficient,
    quantile(0.5)(output_fps) as median_fps,
    quantile(0.95)(output_fps) as p95_fps,
    MIN(output_fps) as min_fps,
    MAX(output_fps) as max_fps,
    SUM(restart_count) as total_restarts,
    COUNT(DISTINCT stream_id) as unique_streams
FROM ai_stream_status
WHERE output_fps > 0
GROUP BY 
    time_bucket,
    orchestrator_address,
    pipeline,
    gpu_id,
    region;

-- Query the materialized view
SELECT 
    time_bucket,
    orchestrator_address,
    pipeline,
    jitter_coefficient,
    avg_fps,
    p95_fps,
    sample_count
FROM ai_stream_jitter_5min_mv
WHERE time_bucket >= now() - INTERVAL 24 HOUR
ORDER BY time_bucket DESC, jitter_coefficient DESC
LIMIT 100;
```

---

## 6. Usage Examples

### Example 1: Check Current Network Health

```sql
-- Quick network health check
SELECT 
    'Jitter Coefficient' as metric,
    AVG(stddevPop(output_fps) / AVG(output_fps)) as value,
    'lower is better, target ≤ 0.1' as note
FROM ai_stream_status
WHERE event_timestamp >= now() - INTERVAL 5 MINUTE
    AND output_fps > 0

UNION ALL

SELECT 
    'Avg E2E Latency (ms)' as metric,
    AVG(f.data_timestamp - r.data_timestamp) as value,
    'target ≤ 3000ms' as note
FROM (
    SELECT request_id, MIN(data_timestamp) as data_timestamp
    FROM stream_trace_events
    WHERE trace_type = 'gateway_receive_stream_request'
        AND event_timestamp >= now() - INTERVAL 5 MINUTE
    GROUP BY request_id
) r
INNER JOIN (
    SELECT request_id, MIN(data_timestamp) as data_timestamp
    FROM stream_trace_events
    WHERE trace_type = 'gateway_receive_first_processed_segment'
        AND event_timestamp >= now() - INTERVAL 5 MINUTE
    GROUP BY request_id
) f ON r.request_id = f.request_id
WHERE f.data_timestamp > r.data_timestamp;
```

### Example 2: Compare Orchestrators

```sql
-- Head-to-head orchestrator comparison
SELECT
    orchestrator_address,
    COUNT(DISTINCT stream_id) as streams_served,
    AVG(output_fps) as avg_fps,
    stddevPop(output_fps) / AVG(output_fps) as jitter_coefficient,
    SUM(restart_count) as restarts,
    -- Rank
    ROW_NUMBER() OVER (ORDER BY stddevPop(output_fps) / AVG(output_fps)) as jitter_rank,
    ROW_NUMBER() OVER (ORDER BY AVG(output_fps) DESC) as fps_rank
FROM ai_stream_status
WHERE event_timestamp >= now() - INTERVAL 1 HOUR
    AND output_fps > 0
GROUP BY orchestrator_address
HAVING streams_served >= 5
ORDER BY jitter_coefficient ASC;
```

---

## Notes

### Data Availability
- **Jitter Coefficient**: Available now ✅ (depends on `ai_stream_status` table)
- **E2E Latency**: Available now ✅ (depends on `stream_trace_events` table)

### Performance Tips
1. Use time-based filters (`event_timestamp >= now() - INTERVAL X`) for all queries
2. Consider creating materialized views for frequently-accessed aggregates
3. Use `LIMIT` clauses to avoid large result sets
4. Add indexes on frequently filtered columns if needed

### Caveats
- E2E latency calculation requires both trace events to exist for a request_id
- Jitter coefficient requires at least 5-10 samples to be meaningful
- Very short-lived streams may not have enough samples for reliable jitter calculation
- Outliers (>30s latency) are filtered to avoid skewing statistics