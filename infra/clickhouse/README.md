# ClickHouse Infrastructure

Single-node ClickHouse instance that ingests Kafka topics directly via the Kafka
engine and stores the physical `raw_*`, `normalized_*`, `canonical_*`,
`operational_*`, and resolver-runtime state used by the analytics stack.

## Directory structure

```
infra/clickhouse/
  config/            ClickHouse server config overrides (mounted read-only)
  init/              Migration runner and Docker entrypoint scripts
  migrations/        SQL migrations applied in sorted order by ch-migrate
  README.md          This file
```

## Migrations

| File | Description |
|------|-------------|
| `001_database.sql` | Database `naap`, application users (`naap_writer`, `naap_reader`) |
| `002_events.sql` | Legacy raw events table used before routed accepted/ignored raw cutover |
| `003_kafka_tables.sql` | Kafka engine tables for `network_events` and `streaming_events` |
| `004_ingest_mvs.sql` | Legacy materialized views: Kafka tables → `naap.events` |
| `005_network_state.sql` | `agg_orch_state` — current orch capabilities (R1) |
| `006_stream_activity.sql` | `agg_stream_state` + `agg_stream_hourly` (R2) |
| `007_payments.sql` | `agg_payment_hourly` (R4) |
| `008_reliability.sql` | `agg_orch_reliability_hourly` (R5) |
| `009_performance.sql` | `agg_fps_hourly`, `agg_discovery_latency_hourly`, `agg_webrtc_hourly` (R3) |
| `010_fix_orch_uri.sql` | Fix `uri` extraction in `mv_orch_state` (`orch_uri` not `uri`) |
| `011_fix_orch_address.sql` | Fix `orch_address` in stream state and reliability tables (use ETH addr not local key) |
| `012_enrichment.sql` | `naap.orch_metadata` + `naap.gateway_metadata` — Livepeer API enrichment tables (ENS names, stake, deposits) |
| `013_stream_status_samples.sql` | `naap.agg_stream_status_samples` — per-sample stream status (MV-populated from `ai_stream_status` events, 30-day TTL). Resolves canonical ETH orch address via `agg_orch_state.uri` JOIN (same pattern as migration 011). Powers Grafana live-operations and overview dashboards. |
| `014_gpu_inventory.sql` | `naap.agg_gpu_inventory` — structured GPU inventory (worker-populated, not MV). The enrichment worker reads `agg_orch_state.raw_capabilities`, parses the `gpu_info` map in Go, and batch-inserts into this table every 5m. ReplacingMergeTree(last_seen) deduplicates on (orch_address, gpu_id); 7-day TTL. Powers the Supply & Inventory dashboard. |
| `018_refresh_runtime_and_namespaces.sql` | Legacy namespace aliases from the refresh-era design |
| `019_canonical_store_tables.sql` | Legacy refresh-era store tables kept only for migration history |
| `020_api_rollup_store_tables.sql` | Legacy rollup store schema later superseded by lifecycle cutover migrations |
| `021_api_stream_state_store_tables.sql` | Physical serving store tables |
| `025_session_evidence_rollups.sql` | Session-grain evidence rollups and capability inventory tables used by the resolver |
| `047_ingest_routed_raw_tables.sql` | Routed ingest cutover: Kafka → `accepted_raw_events` / `ignored_raw_events` plus normalized MVs sourced from accepted raw only |
| `048_lifecycle_semantics_cutover.sql` | Hard-cutover lifecycle fields (`requested_seen`, `selection_outcome`, `startup_outcome`, `excusal_reason`) |
| `049_rollup_store_lifecycle_cutover.sql` | Rollup-store lifecycle metric cutover |
| `050_restore_selection_spine_tables.sql` | Restores resolver-owned selection spine physical tables |
| `051_repoint_raw_alias_and_payments.sql` | Repoints raw aliases and payment typing to `accepted_raw_events` |

Migrations are managed by the repo-native `ch-migrate` runner, which records
checksums in `naap.schema_migrations` and supports `up`, `status`, and
`validate`.

### Table population strategies

| Strategy | Tables | Trigger |
|----------|--------|---------|
| **MV-populated** | `accepted_raw_events`, `ignored_raw_events`, `normalized_*`, event-driven aggregates | Fires synchronously as Kafka rows are routed by the ingest materialized views |
| **Worker-populated** | `orch_metadata`, `gateway_metadata`, `agg_gpu_inventory` | API enrichment worker polls every 5m and batch-inserts |

Worker-populated tables are applied manually on existing volumes — migrations only create the table schema. The enrichment worker will populate them on next startup.

### Tier contract

- `raw_*` — accepted raw envelopes
- `normalized_*` — normalized event-family records
- `canonical_*` — authoritative corrected derivation source
- `operational_*` — low-latency live ops tables
- `api_*` — serving/read models only

`canonical_*` is the only valid source for downstream derivations. `api_*` is a
serving layer and must not be used as truth for new warehouse logic.

Resolver/store rule:

- `canonical_selection_events`, `canonical_selection_attribution_current`,
  `canonical_session_current_store`, `canonical_status_hours_store`, and
  `canonical_session_demand_input_current` are resolver-owned physical tables.
- `api_*_store` tables hold slice/current serving outputs keyed by their
  authoritative ownership timestamp and materialization lineage.
- dbt owns semantic read models over these physical tables, but it does not
  shadow resolver-owned physical tables with recursive views.

Bounded rebuild path:

1. Apply migrations and bring up the stack.
2. Run a bounded resolver backfill on a representative `(org, time window)`.
3. Run `make parity-verify` on the same window.
4. Only after bounded replay/backfill looks clean should you perform a wider
   cold replay from earliest offsets.

Prometheus alert rules scrape the resolver and serving stack; legacy
`canonical-refresh` alerts are no longer part of the steady-state design.

## Running locally

```bash
# Start everything (ClickHouse + Kafka broker):
make up

# Verify ClickHouse is healthy:
curl -s http://localhost:8123/ping          # → Ok.

# Check accepted vs ignored events flowing in (wait ~30s after make up):
clickhouse-client --query "SELECT count(), event_type FROM naap.accepted_raw_events GROUP BY event_type ORDER BY count() DESC"
clickhouse-client --query "SELECT count(), ignore_reason FROM naap.ignored_raw_events GROUP BY ignore_reason ORDER BY count() DESC"

# Check canonical/serving tables populated:
clickhouse-client --query "SELECT count() FROM naap.canonical_session_current_store FINAL"
clickhouse-client --query "SELECT sum(requested_sessions) FROM naap.api_network_demand_by_org_store FINAL"
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
repopulates `accepted_raw_events`, `ignored_raw_events`, normalized tables, and
downstream canonical/serving state from replayed events. Set
`KAFKA_AUTO_OFFSET_RESET=earliest` beforehand if a full historical backfill is needed.

## Changing the TTL

Default accepted-raw TTL is 365 days. To change:
```sql
ALTER TABLE naap.accepted_raw_events MODIFY TTL toDateTime(event_ts) + INTERVAL 180 DAY;
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
| 9363 | HTTP | Prometheus metrics endpoint (internal only; scraped by Prometheus container) |

## Replication

Single-node in Phase 1. No ClickHouse Keeper / replication configured.
Replication is deferred to Phase 8+ once data volume and availability
requirements are better understood. See ADR-001.
