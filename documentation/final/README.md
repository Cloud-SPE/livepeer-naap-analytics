# Livepeer Analytics Documentation (Consolidated)

This folder contains the consolidated, implementation-focused documentation set.

Legacy notes and working documents are intentionally preserved in their original locations.

## Document Set

1. `documentation/final/01_SYSTEM_ARCHITECTURE.md`
- Canonical architecture and ownership boundaries.

2. `documentation/final/02_DATA_FLOW_AND_PROCESSING.md`
- End-to-end flow: Kafka -> Flink -> ClickHouse -> API/dashboard views.

3. `documentation/final/03_SCHEMA_AND_SERVING_MODEL.md`
- Curated facts, dimensions, rollups, API views, and table engine rationale.

4. `documentation/final/04_METRIC_CONTRACTS_AND_VALIDATION.md`
- Metric formulas, proxy caveats, validation method, and readiness status.

5. `documentation/final/05_OPERATIONS_AND_RELEASE.md`
- Deployment order, health checks, replay/backfill approach, and cutover checklist.

## Source Preservation

Existing notes are retained and remain useful for historical context:
- `documentation/METRICS_SCHEMA_DESIGN_SCRATCHPATH.md`
- `documentation/WORKFLOW_LIFECYCLE_EDGE_SPEC.md`
- `documentation/METRICS_SCHEMA_V1_DESIGN.md`
- `documentation/reports/METRICS_VALIDATION_QUERIES.sql`
- `documentation/reports/METRICS_VALIDATION_REPORT.md`
