# Pipeline Execution Split (Stateful vs Non-Stateful)

## Purpose
Lock where transformation logic runs so we reduce Java boilerplate without creating metric drift.

## Decision
- Flink owns stateful, correctness-critical transformations.
- ClickHouse owns non-stateful, 1:1 event projections and serving rollups.

## Why this split
- Stateful logic in ClickHouse SQL is harder to test and keep deterministic across replays.
- Non-stateful projections in Flink create repetitive parser/mapper/sink code with little correctness benefit.
- This split keeps the hard logic in one place (Flink) and keeps high-volume reshaping close to storage (ClickHouse MVs).

## Stateful facts (computed in Flink)
- `fact_workflow_sessions`
- `fact_workflow_session_segments`

These require cross-event correlation, session windows, ordering, and classification:
- deterministic `workflow_session_id`
- success/excused/unexcused outcomes
- swap detection across edges
- attribution quality (`gpu_attribution_method`, `gpu_attribution_confidence`)

## Non-stateful facts (computed in ClickHouse MVs)
- `fact_stream_status_samples` from `ai_stream_status`
- `fact_stream_trace_edges` from `stream_trace_events`
- `fact_stream_ingest_samples` from `stream_ingest_metrics`

These are direct projections with normalization only (no cross-event state).

## Dimension handling
- Flink emits normalized capability snapshots and exploded capability rows:
- `network_capabilities`
- `network_capabilities_advertised`
- `network_capabilities_model_constraints`
- `network_capabilities_prices`
- ClickHouse derives latest-state dimension:
- `dim_orchestrator_capability_current` from snapshots

## MergeTree guidance
- `MergeTree`: append-only raw and non-stateful silver facts.
- `ReplacingMergeTree`: mutable/latest-state facts or dimensions with a version/timestamp.
- `AggregatingMergeTree`: KPI rollups storing aggregate states.
- `SummingMergeTree`: only for strict additive counters; avoid for mixed aggregates.

## Drift controls
- One metric contract document and one validation query pack:
- `documentation/METRICS_SCHEMA_V1_DESIGN.md`
- `documentation/reports/METRICS_VALIDATION_QUERIES.sql`
- Keep Flink classification logic unit-tested.
- Keep ClickHouse MV/view SQL deterministic and covered by schema sync/smoke tests.
