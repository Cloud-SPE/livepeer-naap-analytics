# network_events Topic Onboarding

Operational guide and risk assessment for adding the `network_events` Kafka topic to the live
Flink pipeline alongside the existing `streaming_events` topic.

---

## Background

The pipeline now supports multiple input topics via `QUALITY_INPUT_TOPICS`. Each topic is mapped
to a logical org label via `QUALITY_TOPIC_ORG_MAP`. The planned mapping is:

| Kafka Topic        | Org Label   |
|--------------------|-------------|
| `streaming_events` | `cloud_spe` |
| `network_events`   | `daydream`  |

Events are structurally identical across topics. Session and event IDs are guaranteed unique per
topic — there is no cross-topic duplication.

---

## Key Risk: Historical Data Flood on First Deployment

`network_events` has accumulated historical data. If the Flink consumer starts from the earliest
offset, this creates two serious problems.

### 1. Capability Broadcast State Corruption (Live Data Quality)

Both topics emit `network_capabilities` events. These flow into a **shared broadcast state** keyed
by orchestrator hot wallet address, used to attribute GPU/model identity to live sessions.

When a historical capability event (e.g., timestamped 2024) is processed, the pruning logic in
`CapabilityAttributionSelector.upsertCandidate` uses the **historical event timestamp** as its
reference point:

```
pruneStale(bucket, historicalTimestamp, ttlMs)
// A current live snapshot timestamped 2026 is pruned because:
// Math.abs(2024 - 2026) = 2 years > maxDelta
```

Live capability snapshots from `streaming_events` are evicted. In-flight `streaming_events`
sessions lose their orchestrator/GPU attribution until the next live capability event repopulates
the bucket.

**This is the only risk that degrades live data quality for the already-running org.**

### 2. Memory Pressure During Catch-Up

All historical `network_events` sessions get instantiated in Flink keyed state simultaneously with
live `streaming_events` state. Peak memory during the catch-up period can be significantly higher
than steady state.

---

## What Is NOT a Risk

- **Duplicate data**: `network_events` data has never been processed. No pre-existing rows to
  collide with in ClickHouse.
- **Session interference**: Session IDs are unique per topic. Lifecycle state is fully isolated
  between orgs.
- **Fact table conflicts**: `ReplacingMergeTree(version)` fact tables have no existing rows for
  `network_events` sessions to collide with.

---

## Deployment Approach: Start from Recent Offset

The safest deployment starts `network_events` from a recent point in time (e.g., last 24-48 hours)
and backfills historical raw data separately, offline.

### Step 1 — Configure Per-Topic Offset Reset Strategy

The pipeline now supports `QUALITY_TOPIC_RESET_STRATEGY_MAP`. Set `network_events` to `latest`
so that if Flink finds no committed offset for a partition, it starts from the tip rather than
the beginning:

```
QUALITY_INPUT_TOPICS=streaming_events,network_events
QUALITY_TOPIC_ORG_MAP=streaming_events=cloud_spe,network_events=daydream
QUALITY_TOPIC_RESET_STRATEGY_MAP=streaming_events=earliest,network_events=latest
```

`streaming_events` continues to use `earliest` fallback (existing behavior, unaffected).
`network_events` uses `latest` fallback — if no committed offset exists for a partition, it starts
from the tip.

### Step 2 (Optional) — Pre-Set a Specific Start Position via Kafka CLI

If you want `network_events` to start from a specific point in time (e.g., 48 hours ago) rather
than the very tip, pre-commit the consumer group offset before deploying:

```bash
kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 \
  --group flink-quality-gate-v1 \
  --reset-offsets \
  --topic network_events \
  --to-datetime 2026-03-03T00:00:00.000Z \
  --execute
```

**Order matters**: set the offsets _before_ restarting Flink. Once committed offsets exist for
`network_events` partitions, the `QUALITY_TOPIC_RESET_STRATEGY_MAP` setting is irrelevant — Kafka
resumes from the committed position regardless.

### Step 3 — Deploy and Restart

Deploy with the updated env vars and restart the Flink job. `network_events` starts from the
recent position. No historical flood, no broadcast state corruption.

---

## Historical Backfill (Raw Tables Only)

For historical `network_events` data that was skipped, only raw tables need backfilling. Fact
tables cannot be meaningfully reconstructed for expired sessions (Flink lifecycle state is gone).

**Safe tables to backfill:**

| Table                                        | Engine                                | Idempotent? |
|----------------------------------------------|---------------------------------------|-------------|
| `raw_network_capabilities`                   | `ReplacingMergeTree(event_timestamp)` | Yes — same ORDER BY key keeps latest row |
| `raw_network_capabilities_advertised`        | `ReplacingMergeTree(event_timestamp)` | Yes |
| `raw_network_capabilities_model_constraints` | `ReplacingMergeTree(event_timestamp)` | Yes |
| `raw_network_capabilities_prices`            | `ReplacingMergeTree(event_timestamp)` | Yes |
| `raw_ai_stream_status`                       | `MergeTree`                           | No — run backfill exactly once |
| `raw_ai_stream_events`                       | `MergeTree`                           | No — run backfill exactly once |
| `raw_discovery_results`                      | `MergeTree`                           | No — run backfill exactly once |
| `raw_payment_events`                         | `MergeTree`                           | No — run backfill exactly once |
| `raw_stream_ingest_metrics`                  | `MergeTree`                           | No — run backfill exactly once |

**Approach**: Run a standalone Flink batch job (or direct consumer script) that reads
`network_events` from earliest to the cutoff offset, applies the same `EventParsers`, and writes
only to raw ClickHouse tables. Do **not** include lifecycle operators — no broadcast state is
touched and no fact tables are written.

```
network_events (earliest → cutoff offset)
  → QualityGate (parse + validate)
  → EventParsers (same as live pipeline)
  → ClickHouse raw_* tables only
  (NO lifecycle aggregators, NO broadcast state, NO fact tables)
```

---

## Pending Code Fix: Broadcast State Pruning Direction

The root cause of the historical data risk is in `CapabilityAttributionSelector.pruneStale`. The
current symmetric `Math.abs()` check can evict entries that are far in the **future** relative to
a historical reference timestamp. This should be changed to only prune entries strictly in the past:

```java
// Current (symmetric — dangerous during historical processing):
Math.abs(referenceTs - entry.getValue().snapshotTs) > maxDelta

// Safer (one-directional — only prune genuinely stale past entries):
referenceTs - entry.getValue().snapshotTs > maxDelta
```

This fix would make broadcast state resilient to historical data regardless of deployment approach
and should be applied before any scenario involving large-scale historical ingestion from either
topic.

---

## Deployment Checklist

- [ ] Set `QUALITY_TOPIC_RESET_STRATEGY_MAP=streaming_events=earliest,network_events=latest` in
      deployment config
- [ ] Optionally pre-set `network_events` offset via Kafka CLI to a specific datetime before
      restarting Flink
- [ ] Deploy and verify `streaming_events` pipeline continues uninterrupted — monitor attribution
      quality via `fact_workflow_sessions.attribution_method` and `attribution_confidence`
- [ ] Run historical backfill job for `network_events` raw tables offline, writing directly to
      ClickHouse without going through the live Flink pipeline
- [ ] Apply `CapabilityAttributionSelector.pruneStale` directional fix before any large-scale
      historical ingestion
