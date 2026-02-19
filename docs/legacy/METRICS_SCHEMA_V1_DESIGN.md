# Metrics Schema V1 Design (Supplemental)

This file is retained as supplemental design rationale.

Canonical schema and metric contracts:
- `docs/data/SCHEMA_AND_METRIC_CONTRACTS.md`

## Retained Rationale

- Keep a strict `dim_*` / `fact_*` / `agg_*` separation.
- Preserve additive API fields as canonical; recompute ratios/scores on re-rollup.
- Keep stateful lifecycle correctness in Flink and serving reshapes/rollups in ClickHouse.
- Preserve deterministic drill-through identity (`workflow_session_id`, `stream_id`, `request_id`, `source_event_uid`).

## Implementation Anchors

- `configs/clickhouse-init/01-schema.sql`
- `flink-jobs/src/main/java/com/livepeer/analytics/lifecycle/WorkflowSessionStateMachine.java`
