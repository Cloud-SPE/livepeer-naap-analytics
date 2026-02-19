# Data Quality Guide

## Overview

This document consolidates the data quality controls implemented in the Flink pipeline. It covers deduplication, validation, DLQ/quarantine handling, troubleshooting, and monitoring.

## Deduplication Strategy

### Dedup Key

**Where it lives**
- `flink-jobs/src/main/java/com/livepeer/analytics/quality/QualityGateProcessFunction.java`
- `flink-jobs/src/main/java/com/livepeer/analytics/quality/DeduplicationProcessFunction.java`

**Strategy**

- The gateway already generates a UUID per event and Kafka publish retries can result in duplicate writes.
- Kafka does not deduplicate on key; therefore `event_id` is the correct logical identifier.

1. If `event.id` (GUID) is present, use it as the dedup key.
2. If missing, build a deterministic hash from the normalized payload and selected dimensions.
3. The dedup key is stored on the event and used as the Flink key for dedup state.

**Rationale**
- GUIDs provide the strongest idempotency when upstream sends stable identifiers.
- Hash fallback keeps replayed or legacy events idempotent without losing data.
- Hashing removes replay metadata fields to avoid changing keys across replays.

**TTL / State**
- TTL is configurable via `QUALITY_DEDUP_TTL_MINUTES` (default: `1440` minutes).
- The TTL bounds state size while still preventing short‑term duplicates.

**Quarantine**
Duplicates are emitted to the quarantine side output and written to Kafka topic `events.quarantine.streaming_events.v1` and ClickHouse table `streaming_events_quarantine`.

### Why This Approach Was Chosen Over Alternatives

#### Avoid Deduping at Kafka Connect / ClickHouse Ingest

- Kafka Connect sinks are **at-least-once**; retries can insert duplicates.
- ClickHouse insert quorum and in-memory/block dedup do **not** provide reliable idempotency by key.
- Storage-engine dedup (`ReplacingMergeTree`) is **eventual** and can expose duplicates in near-real-time queries.

#### Avoid Deduping in ClickHouse Queries (`GROUP BY` / `DISTINCT` / `FINAL`)

- Pushes correctness into every query and degrades performance.

#### Flink as the Control Point
- Flink already performs semantic parsing and writes typed tables.
- Keyed state + TTL is cheap, bounded, and auditable.
- One implementation protects all downstream tables and avoids query-time workarounds.

## Validation Rules

**Where rules live**
`flink-jobs/src/main/java/com/livepeer/analytics/quality/SchemaValidator.java`

**What is validated**
- Root must be a JSON object.
- Required fields per event type (e.g., `data.stream_id`, `data.request_id`).
- Timestamp must be numeric or numeric string.
- Event type must be supported.
- Supported versions are enforced when provided.

**Version support**
- Controlled by `QUALITY_SUPPORTED_VERSIONS` (default: `1,v1`).

**DLQ process**
Schema failures emit a DLQ envelope with failure details. Output is written to Kafka topic `events.dlq.streaming_events.v1` and ClickHouse table `streaming_events_dlq`. Source and payload fields are preserved for replay and diagnostics.

**Replay flag**
- Replayed events carry `__replay=true` and are preserved in DLQ envelopes.

## Pre-Sink Row Guard

**Where it lives**
- `flink-jobs/src/main/java/com/livepeer/analytics/sink/ParsedEventRowGuardProcessFunction.java`
- `flink-jobs/src/main/java/com/livepeer/analytics/sink/EnvelopeRowGuardProcessFunction.java`

**Behavior**
- Enforces `CLICKHOUSE_SINK_MAX_RECORD_BYTES` (default: `1_000_000`).
- Oversized typed rows emit a DLQ envelope with `SINK_GUARD` failure class.
- Oversized DLQ/quarantine rows are dropped to avoid recursive failures.

## Troubleshooting

**High DLQ volume**
1. Check `SchemaValidator` rules for missing/changed fields.
2. Inspect a sample from `streaming_events_dlq` to see `failure_class` and `failure_reason`.
3. Update these files in order: `configs/clickhouse-init/01-schema.sql`, `flink-jobs/src/main/java/com/livepeer/analytics/parse/EventParsers.java`, `flink-jobs/src/main/java/com/livepeer/analytics/model/EventPayloads.java`, `flink-jobs/src/main/java/com/livepeer/analytics/sink/ClickHouseRowMappers.java`, tests `flink-jobs/src/test/java/com/livepeer/analytics/sink/ClickHouseSchemaSyncTest.java` and `flink-jobs/src/test/java/com/livepeer/analytics/parse/EventParsersTest.java`.

**High quarantine volume**
1. Confirm upstream is using stable GUIDs.
2. Validate dedup TTL is not too long for your volume.
3. Review dedup key logic in `QualityGateProcessFunction.DedupKey`.

**Sink guard drops**
1. Check `quality_gate.sink_guard.oversize_drops` metric.
2. If raw payloads grew, adjust `CLICKHOUSE_SINK_MAX_RECORD_BYTES`.
3. Consider pruning or truncating large raw fields if they are not required.

## Metrics and Monitoring

**Flink metrics**
Collected via `QualityGateProcessFunction` and `DeduplicationProcessFunction`:
- `quality_gate.input` (counter)
- `quality_gate.accepted` (counter)
- `quality_gate.dlq` (counter)
- `quality_gate.input_rate` (meter)
- `quality_gate.dlq_rate` (meter)
- `quality_gate.dedup.duplicates` (counter)
- `quality_gate.dedup.accepted` (counter)
- `quality_gate.dedup.duplicate_rate` (meter)
- `quality_gate.sink_guard.oversize_drops` (counter)

**Key tables for monitoring**
- `streaming_events_dlq`
- `streaming_events_quarantine`

**Recommended alerts**
1. DLQ rate > 1% of total events (schema drift likely).
2. Quarantine rate spikes (dedup or replay issues).
3. Sink guard drops > 0 (payload size regression).
