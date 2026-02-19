# Flink Deployment Guide

This guide covers uploading and running Flink jobs, plus state-preserving updates via savepoints.

## Deployment Configuration

| Parameter | Example / Default | Purpose |
|---|---|---|
| Flink base URL | `http://YOUR_FLINK_SERVER:8081` | REST endpoint for deployment operations |
| Main entry class | `com.livepeer.analytics.pipeline.StreamingEventsToClickHouse` | Primary Flink pipeline job class |
| Parallelism | `1` | Initial deployment parallelism (tune by environment) |
| Savepoint drain mode | `false` | Savepoint stop behavior for updates |
| Savepoint path | `/opt/flink/storage/savepoints/<savepoint-id>` | Restore state for clean updates |
| JAR artifact | `livepeer-analytics-flink-0.1.0.jar` | Upload artifact for run/restore |

## Required Environment Variables

| Variable | Default | Purpose |
|---|---|---|
| `QUALITY_KAFKA_BOOTSTRAP` | `kafka:9092` | Kafka bootstrap for main pipeline |
| `QUALITY_INPUT_TOPIC` | `streaming_events` | Source topic |
| `QUALITY_DLQ_TOPIC` | `events.dlq.streaming_events.v1` | DLQ output topic |
| `QUALITY_QUARANTINE_TOPIC` | `events.quarantine.streaming_events.v1` | Quarantine output topic |
| `CLICKHOUSE_URL` | `http://clickhouse:8123` | ClickHouse sink endpoint |
| `CLICKHOUSE_DATABASE` | `livepeer_analytics` | Sink database |
| `CLICKHOUSE_USER` | `analytics_user` | Sink user |
| `CLICKHOUSE_PASSWORD` | none | Sink password |
| `CLICKHOUSE_SINK_MAX_BATCH_SIZE` | `1000` | Sink batching control |
| `CLICKHOUSE_SINK_MAX_IN_FLIGHT` | `2` | Concurrent sink batches |
| `CLICKHOUSE_SINK_MAX_BUFFERED` | `10000` | Max buffered rows |
| `CLICKHOUSE_SINK_MAX_BATCH_BYTES` | `5000000` | Batch payload limit |
| `CLICKHOUSE_SINK_MAX_TIME_MS` | `1000` | Flush interval |
| `CLICKHOUSE_SINK_MAX_RECORD_BYTES` | `1000000` | Per-row size guard |

## Monitor Cluster Health

- Check cluster health and slots:
  - `curl http://YOUR_FLINK_SERVER:8081/overview`
- List running jobs:
  - `curl http://YOUR_FLINK_SERVER:8081/jobs/overview`

## Standard Deployment (First Time)

1. Upload the JAR:

```bash
curl -X POST -H "Expect:" -F "jarfile=@livepeer-analytics-flink-0.1.0.jar" http://YOUR_FLINK_SERVER:8081/jars/upload
```

2. Run the job using the returned `jarid`:

```bash
curl -X POST http://YOUR_FLINK_SERVER:8081/jars/<jarid>/run \
     -H "Content-Type: application/json" \
     -d '{"entryClass":"com.livepeer.analytics.pipeline.StreamingEventsToClickHouse","parallelism":1}'
```

## Clean Update (Savepoint-Based)

Use this flow to update a JAR or restart while preserving state.

1. Stop with savepoint trigger:

```bash
curl -X POST http://YOUR_FLINK_SERVER:8081/jobs/<jid>/stop \
     -H "Content-Type: application/json" \
     -d '{"drain":false}'
```

2. Get savepoint location using `jobid` and returned `request-id`:

```bash
curl http://YOUR_FLINK_SERVER:8081/jobs/<jid>/savepoints/<request-id>
```

3. Upload new JAR:

```bash
curl -X POST -H "Expect:" -F "jarfile=@livepeer-analytics-flink-0.1.0.jar" http://YOUR_FLINK_SERVER:8081/jars/upload
```

4. Run with state recovery:

```bash
curl -X POST http://YOUR_FLINK_SERVER:8081/jars/<new_jarid>/run \
     -H "Content-Type: application/json" \
     -d '{"entryClass":"com.livepeer.analytics.pipeline.StreamingEventsToClickHouse","parallelism":1,"savepointPath":"/opt/flink/storage/savepoints/<savepoint-id>"}'
```
