# Data Retention Policy

| Field | Value |
|---|---|
| **Status** | Active |
| **Effective date** | 2026-04-02 |
| **Ticket** | TASK-06 / [#285](https://github.com/livepeer/livepeer-naap-analytics-deployment/issues/285) |
| **Last reviewed** | 2026-04-09 |

---

## Overview

This document is the single source of truth for all data retention decisions in the NaaP analytics stack. It covers Kafka topic retention windows, ClickHouse table TTLs, and the relationship between the two tiers.

For the architectural rationale behind the Kafka + ClickHouse engine choice, see [`../design-docs/adr-001-storage-architecture.md`](../design-docs/adr-001-storage-architecture.md). For the canonical SQL TTL statements, see [`../../infra/clickhouse/retention.sql`](../../infra/clickhouse/retention.sql).

---

## Principles

- **Kafka is a transport replay buffer, not an archive.** ClickHouse is the authoritative data store. Kafka retention windows are sized for operational recovery, not long-term storage.
- **Retention is deliberately asymmetric across tiers.** Kafka provides a short-term replay window; ClickHouse provides long-term audit, backfill, and API serving.
- **No Kafka-level DLQ or quarantine topics exist.** Events that fail validation are not routed to a separate Kafka topic. Instead, they are written to the `naap.ignored_raw_events` ClickHouse table, which carries the same 90-day TTL as accepted events and is fully queryable for audit and replay analysis.
- **Retention windows are the minimum needed for operational recovery and compliance.** They are not sized for indefinite growth.
- **Per-topic Kafka config overrides the broker default.** The broker default (`KAFKA_LOG_RETENTION_HOURS=168`, 7 days) applies only to auto-created topics. All named topics in `infra/kafka/topics.yaml` carry explicit `retention.ms` values that take precedence.

---

## Kafka Topic Retention

| Topic | Partitions | Retention | Rationale |
|---|---|---|---|
| `network_events` | 6 | **7 days** (604,800,000 ms) | Primary raw network-event stream for the daydream org. |
| `streaming_events` | 6 | **7 days** (604,800,000 ms) | Primary raw streaming-event stream for the cloudspe org. |
| Broker default | — | **7 days** (`KAFKA_LOG_RETENTION_HOURS=168`) | Applies to auto-created topics that are not explicitly overridden. |

Current broker default source: `deploy/infra2/kafka/stack.yml`.

### Replay within the Kafka window

With `KAFKA_AUTO_OFFSET_RESET=earliest`, ClickHouse consumer groups can re-consume from up to 7 days ago by resetting their offsets. Consumer groups:

| Consumer group | Topic |
|---|---|
| `clickhouse-naap-network` | `network_events` |
| `clickhouse-naap-streaming` | `streaming_events` |

Offset reset procedure: stop the ClickHouse Kafka engine table, reset the consumer group offset using the Kafka CLI, then restart. See `infra/clickhouse/README.md` for operational steps.

---

## ClickHouse Table TTL Inventory

TTL expressions are applied at the table level in ClickHouse and enforced asynchronously in the background. The canonical SQL for all TTL statements is in `infra/clickhouse/retention.sql`.

### Tier 1 — Raw ingest (90 days)

| Table | TTL column | TTL |
|---|---|---|
| `naap.accepted_raw_events` | `event_ts` | 90 days |
| `naap.ignored_raw_events` | `event_ts` | 90 days |

`ignored_raw_events` is the functional equivalent of a DLQ/quarantine store. All events rejected during ingest (invalid schema, unsupported type, parse failure) are written here with a `ignore_reason` label. The 90-day TTL matches accepted events to ensure full audit coverage.

Queryable diagnostic view: `naap.ignored_raw_event_diagnostics`.

### Tier 1b — Normalized event tables — AI Batch / BYOC (90 days)

| Table | TTL column | TTL |
|---|---|---|
| `naap.normalized_ai_batch_job` | `event_ts` | 90 days |
| `naap.normalized_ai_llm_request` | `event_ts` | 90 days |
| `naap.normalized_byoc_job` | `event_ts` | 90 days |
| `naap.normalized_byoc_auth` | `event_ts` | 90 days |
| `naap.normalized_worker_lifecycle` | `event_ts` | 90 days |
| `naap.normalized_byoc_payment` | `event_ts` | 90 days |

These tables store all accepted AI batch, BYOC, and payment events after normalization. The 90-day TTL matches `accepted_raw_events` so that canonical dbt models can always be recomputed from normalized tables within the same audit window. TTL statements are in `infra/clickhouse/retention.sql`.

### Tier 2 — Aggregate samples (30 days)

| Table | TTL column | TTL |
|---|---|---|
| `naap.agg_stream_status_samples` | `sample_ts` | 30 days |
| `naap.resolver_dirty_partitions` | `event_date` | 30 days |
| `naap.resolver_dirty_windows` | `window_start` | 30 days |
| `naap.resolver_repair_requests` | `created_at` | 30 days |

This tier is intentionally longer-lived than Kafka replay. These tables support
validation, debugging, and bounded forensic review after the 7-day Kafka window
has expired but before the 90-day raw-event TTL boundary.

### Tier 3 — Entity metadata / cache (7 days)

| Table | TTL column | TTL |
|---|---|---|
| `naap.agg_gpu_inventory` | `last_seen` | 7 days |
| `naap.gateway_metadata` | `updated_at` | 7 days |
| `naap.orch_metadata` | `updated_at` | 7 days |
| `naap.resolver_window_claims` | `created_at` | 7 days |
| `naap.selection_attribution_changes` | `created_at` | 7 days |
| `naap.session_current_changes` | `created_at` | 7 days |
| `naap.status_hour_changes` | `created_at` | 7 days |

These tables are refreshed continuously by the resolver and ingest pipeline. Entries older than 7 days have no operational value. Long-term history for change events is available via `accepted_raw_events`.

### Tier 4 — Resolver working tables (1–2 days)

| Table | TTL column | TTL |
|---|---|---|
| `naap.resolver_query_event_ids` | `created_at` | 1 day |
| `naap.resolver_query_identities` | `created_at` | 1 day |
| `naap.resolver_query_selection_event_ids` | `created_at` | 1 day |
| `naap.resolver_query_session_keys` | `created_at` | 1 day |
| `naap.resolver_query_window_slices` | `created_at` | 2 days |

Ephemeral per-query scratch tables. Short TTLs prevent accumulation of stale working state.

### No TTL — Unbounded growth (known gap)

The following tables currently have no TTL and will grow indefinitely:

| Table | Notes |
|---|---|
| `naap.agg_orch_state_hourly` | Hourly orchestrator aggregates |
| `naap.agg_stream_state_hourly` | Hourly stream aggregates |
| `naap.orch_current_store` | Resolver-published orchestrator current state |
| `naap.session_current_store` | Resolver-published session current state |
| `naap.canonical_orch_state` | dbt-published semantic view |
| `naap.canonical_stream_state` | dbt-published semantic view |
| `naap.api_orch_state_store` | API serving table |
| `naap.api_stream_state_store` | API serving table |

A TTL of 90 days (aligning with raw event retention) is the candidate for hourly aggregates and current-store tables. Applying a TTL to `api_*_store` and `canonical_*_store` tables requires a separate decision based on API consumer requirements and the downstream query window. See [Known Gaps](#known-gaps-and-future-work).

---

## Kafka-to-ClickHouse Alignment

The following table defines the replay path based on event age:

| Event age | Replay source | Mechanism |
|---|---|---|
| 0 – 7 days | Kafka `network_events` / `streaming_events` | Reset ClickHouse consumer group offset to `earliest`; re-consume directly from Kafka |
| 7 days – 90 days | ClickHouse `naap.accepted_raw_events` | Use resolver `backfill` mode: `make resolver-backfill FROM=<start> TO=<end> ORG=<org>` |
| > 90 days | Not available (TTL expired) | Raw events have been purged from ClickHouse. Derived aggregates may still be queryable depending on their TTL. |

For events rejected during original ingest, the equivalent path is `naap.ignored_raw_events` (same 90-day TTL), queryable via `naap.ignored_raw_event_diagnostics`.

---

## Changing Retention Values

### Kafka topic retention

To change a topic's retention window:

1. Update `infra/kafka/topics.yaml` with the new `retention.ms` value.
2. Apply to the live broker:
   ```bash
   kafka-configs.sh --bootstrap-server <broker>:9092 \
     --alter \
     --entity-type topics \
     --entity-name network_events \
     --add-config retention.ms=<new_value_ms>
   ```
3. Verify:
   ```bash
   kafka-configs.sh --bootstrap-server <broker>:9092 \
     --describe \
     --entity-type topics \
     --entity-name network_events
   ```

Note: `topics.yaml` is the source of truth for documentation and provisioning scripts (`scripts/setup.sh`). Updating the file does not automatically alter the live topic — the CLI step is required.

### ClickHouse TTL

To change a table's TTL on a running cluster:

```sql
ALTER TABLE naap.<table> MODIFY TTL <new_expression>;
```

TTL mutations run asynchronously. Monitor progress:

```sql
SELECT * FROM system.mutations WHERE is_done = 0;
```

No container restart is required. See `infra/clickhouse/README.md` for the full procedure and examples. The canonical set of TTL statements is in `infra/clickhouse/retention.sql`.

---

## Known Gaps and Future Work

1. **Hourly aggregate tables have no TTL** (`agg_*_hourly`). These tables grow unbounded. Candidate fix: apply a 90-day TTL matching raw event retention. Requires a forward migration in `infra/clickhouse/migrations/`.

2. **Current-store and API-serving tables have no TTL** (`*_current_store`, `api_*_store`, `canonical_*_store`). TTL for serving tables depends on the downstream API query window and consumer SLA. This requires a separate decision.

3. **Prometheus retention is out of scope for this document.** It is governed by the Prometheus stack configs:
   - infra1 Prometheus: 180-day retention (`deploy/infra1/prometheus/stack.yml`)
   - infra2 Prometheus: 90-day retention (`deploy/infra2/prometheus/stack.yml`)
