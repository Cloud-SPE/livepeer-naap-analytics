# Plan: Phase 1 — ClickHouse Schema + Kafka Ingestion

**Status:** complete
**Started:** 2026-03-24
**Depends on:** nothing (first phase)
**Blocks:** Phase 2 (pipeline), Phase 3 (Go API repo layer)

---

## Goal

Stand up a ClickHouse instance that continuously ingests both Kafka topics into a
raw events table and materialises a set of aggregate views that the Go API will query.
No Go API changes yet — this phase ends when ClickHouse is running, ingesting live
data, and aggregate views are populated.

## Context

- Broker: `infra1.livepeer.cloud:9092` (plaintext)
- Topics: `network_events` (daydream), `streaming_events` (cloudspe)
- ClickHouse Kafka engine consumes directly — no intermediary pipeline
- See ADR-001 for the full storage architecture decision

## Architecture

```
Kafka                     ClickHouse
─────────────────────     ─────────────────────────────────────────
network_events    ────►  kafka_network_events (Kafka engine, ephemeral)
                              │
                              ▼ (mv_ingest_network_events)
                         events (MergeTree, partitioned by org+month)
                              │
                              ▼ (mv_*)
                         aggregate tables (AggregatingMergeTree)

streaming_events  ────►  kafka_streaming_events (Kafka engine, ephemeral)
                              │
                              ▼ (mv_ingest_streaming_events)
                         events (same table, org='cloudspe')
```

## Steps

- [x] Create exec-plan
- [x] Add ClickHouse to docker-compose.yml
- [x] Create infra/clickhouse/ directory structure
- [x] Migration 001: database and user setup
- [x] Migration 002: raw events table (MergeTree)
- [x] Migration 003: Kafka engine tables (one per topic)
- [x] Migration 004: ingestion materialized views (Kafka → events)
- [x] Migration 005: network state aggregate views (R1)
- [x] Migration 006: stream activity aggregate views (R2)
- [x] Migration 007: payment aggregate views (R4)
- [x] Migration 008: reliability aggregate views (R5)
- [x] Migration 009: performance aggregate views (R3)
- [x] Update .env.example with ClickHouse credentials
- [x] Smoke test: `make up` → verify events flowing into ClickHouse
- [x] Document any schema deviations in progress log

## Decisions made in this plan

1. **Single `events` table for both topics** — org field distinguishes source.
   Tradeoff: partition pruning on org is less granular than separate tables, but
   simpler query surface and single TTL policy. Acceptable at this event volume.

2. **Raw `data` stored as JSON String** — envelope fields are typed; `data` payload
   stored raw. Tradeoff: `JSONExtract*` functions needed in queries; mitigated by
   materialized views pre-extracting the key fields needed for aggregates.

3. **ReplacingMergeTree for raw events** — deduplication on `event_id` during
   background merges. Queries use `FINAL` modifier or aggregate views (which are
   idempotent) for accurate counts. Tradeoff: `FINAL` adds query overhead; acceptable
   because aggregate views (not the raw table) serve the API.

4. **TTL default = 365 days, configurable via ALTER TABLE** — migration sets 365 days.
   To change: `ALTER TABLE events MODIFY TTL event_ts + INTERVAL N DAY`. Documented
   in infra/clickhouse/README.md.

5. **No ClickHouse Keeper / replication in Phase 1** — single-node ClickHouse for
   development and initial launch. Replication is an operational concern for Phase 8+.

## Progress log

2026-03-24: Plan created. Beginning implementation.
2026-03-24: All migrations written (001–009). docker-compose.yml updated with
  ClickHouse 24.3 service. infra/clickhouse/ structure created. .env.example
  updated.
2026-03-24: Smoke test passed. Fixed pipeline.Dockerfile (editable install
  without source) and added [tool.hatch.build.targets.wheel] to pipeline
  pyproject.toml. Generated api/go.sum via go mod tidy.
  Live data confirmed: 525K+ events ingested across both topics. All 8
  aggregate tables populated: 85 orchs, 32K stream starts, 21K payment events,
  58 active orchestrators. Phase complete.

## Known risks

- ClickHouse Kafka engine creates a consumer group `clickhouse-naap-network` and
  `clickhouse-naap-streaming`. These must not conflict with the Python pipeline
  consumer group `naap-analytics-pipeline`. Verified: different group IDs.
- `network_capabilities` events contain a JSON array as `data` (not an object).
  The ingestion MV must handle both array and object `data` payloads gracefully.
