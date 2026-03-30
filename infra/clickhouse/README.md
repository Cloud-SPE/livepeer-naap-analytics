# ClickHouse Infrastructure

Single-node ClickHouse instance that ingests Kafka topics directly via the Kafka
engine and stores the physical schema used by the analytics stack. Semantic
tiers still center on `raw_*`, `normalized_*`, `canonical_*`, `operational_*`,
and `api_*`, but the supported bootstrap also includes infrastructure/runtime
objects such as `accepted_raw_events`, `ignored_raw_events`, `kafka_*`,
`resolver_*`, `agg_*`, metadata tables, and change/audit tables.

The current fresh-volume bootstrap artifact is [`bootstrap/v1.sql`](bootstrap/v1.sql).
The generated inventory for that bootstrap lives at [`../../docs/generated/schema.md`](../../docs/generated/schema.md).

## Directory structure

```
infra/clickhouse/
  config/            ClickHouse server config overrides (mounted read-only)
  bootstrap/         Generated fresh-volume bootstrap schema
  init/              Schema runner and Docker entrypoint scripts
  migrations/        Forward migrations only after the v1 bootstrap baseline
  README.md          This file
```

## Schema Modes

The ClickHouse entrypoint supports two schema modes via `CLICKHOUSE_SCHEMA_MODE`:

- `bootstrap` — apply the extracted fresh-volume baseline from [`bootstrap/v1.sql`](bootstrap/v1.sql), then apply any forward migrations present in [`migrations/`](migrations/)
- `migrations` — apply forward migrations from [`migrations/`](migrations/) to an existing database

## Migrations

The historical migration chain has been retired from the active repo path.
Fresh volumes bootstrap from [`bootstrap/v1.sql`](bootstrap/v1.sql). Future schema
changes should be added as forward-only migrations in [`migrations/`](migrations/).

Forward migrations are managed by the repo-native `ch-migrate` runner, which
records checksums in `naap.schema_migrations` only when forward migrations are
actually present and supports `up`, `bootstrap`, `status`, and `validate`.

Regenerate the bootstrap artifact from a clean bootstrap-backed validation stack with:

```bash
make bootstrap-extract
```

### Table population strategies

| Strategy | Tables | Trigger |
|----------|--------|---------|
| **MV-populated** | `accepted_raw_events`, `ignored_raw_events`, `normalized_*`, event-driven aggregates | Fires synchronously as Kafka rows are routed by the ingest materialized views |
| **Worker-populated** | `orch_metadata`, `gateway_metadata`, `agg_gpu_inventory` | API enrichment worker polls every 5m and batch-inserts |

Worker-populated tables are applied manually on existing volumes — the bootstrap and any future forward migrations only create the table schema. The enrichment worker will populate them on next startup.

### Tier contract

- `raw_*` — accepted raw envelopes
- `normalized_*` — normalized event-family records
- `canonical_*` — authoritative corrected derivation source
- `operational_*` — low-latency live ops tables
- `api_*` — serving/read models only

This contract governs semantic data flow, not physical naming uniformity. The
generated bootstrap in [`../../docs/generated/schema.md`](../../docs/generated/schema.md)
is the source of truth for the supported physical object inventory.

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

1. Apply the bootstrap baseline and bring up the stack.
2. Run a bounded resolver backfill on a representative `(org, time window)`.
3. Run `make parity-verify` on the same window.
4. Only after bounded replay/backfill looks clean should you perform a wider
   cold replay from earliest offsets.

Prometheus alert rules scrape the resolver and serving stack; legacy
`canonical-refresh` alerts are no longer part of the steady-state design.

## Running locally

```bash
# Start the runtime stack:
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
2. Restart the stack so the bootstrap re-applies on a fresh ClickHouse container:
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
