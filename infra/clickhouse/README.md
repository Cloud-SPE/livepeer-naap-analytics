# ClickHouse Infrastructure

Single-node ClickHouse instance that ingests Kafka topics directly via the Kafka
engine and materialises aggregate views for the Go API.

## Directory structure

```
infra/clickhouse/
  config/            ClickHouse server config overrides (mounted read-only)
  init/              Docker entrypoint scripts (run on first container start)
  migrations/        SQL migrations applied in sort order by init script
  README.md          This file
```

## Migrations

| File | Description |
|------|-------------|
| `001_database.sql` | Database `naap`, application users (`naap_writer`, `naap_reader`) |
| `002_events.sql` | Raw events table (ReplacingMergeTree, partitioned by org+month) |
| `003_kafka_tables.sql` | Kafka engine tables for `network_events` and `streaming_events` |
| `004_ingest_mvs.sql` | Materialized views: Kafka tables → events table |
| `005_network_state.sql` | `agg_orch_state` — current orch capabilities (R1) |
| `006_stream_activity.sql` | `agg_stream_state` + `agg_stream_hourly` (R2) |
| `007_payments.sql` | `agg_payment_hourly` (R4) |
| `008_reliability.sql` | `agg_orch_reliability_hourly` (R5) |
| `009_performance.sql` | `agg_fps_hourly`, `agg_discovery_latency_hourly`, `agg_webrtc_hourly` (R3) |

Migrations are idempotent (`CREATE TABLE IF NOT EXISTS`, `CREATE USER IF NOT EXISTS`).

## Running locally

```bash
# Start everything (ClickHouse + Kafka broker):
make up

# Verify ClickHouse is healthy:
curl -s http://localhost:8123/ping          # → Ok.

# Check events flowing in (wait ~30s after make up):
clickhouse-client --query "SELECT count(), event_type FROM naap.events GROUP BY event_type ORDER BY count() DESC"

# Check aggregate tables populated:
clickhouse-client --query "SELECT count() FROM naap.agg_orch_state FINAL"
clickhouse-client --query "SELECT sum(started) FROM naap.agg_stream_hourly"
```

## Changing the Kafka broker

The broker is configured at migration time via the `KAFKA_BROKER_LIST` environment
variable (default: `infra1.livepeer.cloud:9092`).

To point an already-running ClickHouse at a different broker:
```sql
ALTER TABLE naap.kafka_network_events MODIFY SETTING kafka_broker_list = 'new-broker:9092';
ALTER TABLE naap.kafka_streaming_events MODIFY SETTING kafka_broker_list = 'new-broker:9092';
```

## Changing the TTL

Default TTL is 365 days. To change:
```sql
ALTER TABLE naap.events MODIFY TTL toDateTime(event_ts) + INTERVAL 180 DAY;
```

Allow ClickHouse time to process the TTL change before data is actually deleted.
TTL mutations run in the background and can be monitored via `system.mutations`.

## Consumer groups

| Group name | Topic | Owner |
|-----------|-------|-------|
| `clickhouse-naap-network` | `network_events` | ClickHouse Kafka engine |
| `clickhouse-naap-streaming` | `streaming_events` | ClickHouse Kafka engine |
| `naap-analytics-pipeline` | both | Python pipeline (Phase 2) |

These groups must not conflict. Verified distinct at time of creation.

## Ports

| Port | Protocol | Use |
|------|---------|-----|
| 8123 | HTTP | Browser / curl / healthcheck |
| 9000 | Native | `clickhouse-client`, Go driver |

## Replication

Single-node in Phase 1. No ClickHouse Keeper / replication configured.
Replication is deferred to Phase 8+ once data volume and availability
requirements are better understood. See ADR-001.
