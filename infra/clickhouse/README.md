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
| `010_fix_orch_uri.sql` | Fix `uri` extraction in `mv_orch_state` (`orch_uri` not `uri`) |
| `011_fix_orch_address.sql` | Fix `orch_address` in stream state and reliability tables (use ETH addr not local key) |

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

The broker address is substituted into the Kafka Engine DDL at migration time via
`KAFKA_BROKER_LIST` (default: `infra2.cloudspe.com:9092`).

> **ClickHouse Kafka Engine does not support `ALTER TABLE MODIFY SETTING`.**
> The broker cannot be changed on a live table without recreating it.

To point ClickHouse at a different broker:

1. Set the new broker in `.env`:
   ```
   KAFKA_BROKER_LIST=new-broker:9092
   ```
2. Restart the stack so migrations re-apply on a fresh ClickHouse container:
   ```bash
   make down && make up
   ```

This drops and recreates the Kafka Engine tables and their dependent MVs, then
repopulates aggregate tables from new events. Set `KAFKA_AUTO_OFFSET_RESET=earliest`
beforehand if a full historical backfill is needed.

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
