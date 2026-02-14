# Metric Contracts and Validation

## Primary Metrics
- Output FPS
- Jitter coefficient
- Startup proxy latency
- E2E proxy latency
- Prompt-to-playable proxy latency
- Failure rate proxy (unexcused startup outcomes)
- Swap rate

Bandwidth KPI remains blocked pending orchestrator-side telemetry ingestion.

## Validation Sources
- Query pack: `documentation/reports/METRICS_VALIDATION_QUERIES.sql`
- Lifecycle spec: `documentation/WORKFLOW_LIFECYCLE_EDGE_SPEC.md`

## Validation Principles
- Validate at session grain first, then aggregate.
- Always compare rollup/view output to raw/fact recomputation windows.
- Treat classification/edge mapping changes as versioned contract changes.

## Param Update Analysis
- Markers from `fact_workflow_param_updates`.
- Before/after windows on status samples can measure FPS/jitter impact.
- Planned metric: `param_update_failure_rate`.
