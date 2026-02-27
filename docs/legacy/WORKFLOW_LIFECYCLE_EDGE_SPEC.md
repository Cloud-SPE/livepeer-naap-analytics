# Workflow Lifecycle Edge Spec (Archived Supplemental)

This file is retained as historical context only.

Canonical lifecycle contract source:
- `docs/data/SCHEMA_AND_METRIC_CONTRACTS.md`

## Resolved V1 Facts

- Deterministic composite session identity.
- Locked lifecycle edge dictionary and startup/swap classification semantics.
- Excused error taxonomy with substring matching.
- Swap detection by explicit event and unique-orchestrator fallback.
- Snapshot-based GPU/model attribution with method + confidence and 24h default TTL.

## Implementation Traceability

- Flink lifecycle logic:
  - `flink-jobs/src/main/java/com/livepeer/analytics/lifecycle/WorkflowSessionStateMachine.java`
  - `flink-jobs/src/main/java/com/livepeer/analytics/lifecycle/WorkflowLatencyDerivation.java`
- ClickHouse serving/materialization:
  - `configs/clickhouse-init/01-schema.sql`
