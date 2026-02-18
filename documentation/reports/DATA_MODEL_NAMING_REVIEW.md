# Data Model Naming Review

Date: 2026-02-18

Scope reviewed:
- `configs/clickhouse-init/01-schema.sql`
- Flink row mappers and lifecycle fact payload fields
- Integration/validation SQL references under `documentation/reports/`

## Summary

This review focused on potentially confusing or vague column names, especially where names can become ambiguous as the model grows.

Primary action taken:
- Renamed generic attribution columns to explicit GPU-scoped names:
  - `attribution_method` -> `gpu_attribution_method`
  - `attribution_confidence` -> `gpu_attribution_confidence`

Applied to:
- `fact_stream_status_samples`
- `fact_workflow_sessions`
- `fact_workflow_session_segments`
- `fact_workflow_param_updates`
- Flink ClickHouse row mapper output keys
- Validation SQL that referenced old names

## Findings

1. Generic attribution columns were ambiguous
- Previous names implied a single attribution domain.
- In practice, these fields currently represent model/GPU attribution from capability snapshots.
- Risk: future non-GPU attribution (region, orchestrator identity, etc.) would create semantic conflict.
- Resolution: scoped names above.

2. `state` is broad but acceptable at status-sample grain
- Table: `fact_stream_status_samples.state`.
- Context is clear enough when joined with stream/session keys.
- Recommendation: keep as-is for now; if additional state domains are introduced, consider `stream_state`.

3. `reason` on segments can be interpreted too broadly
- Table: `fact_workflow_session_segments.reason`.
- Today it is boundary/closure reason, not arbitrary error reason.
- Recommendation: future rename candidate to `segment_transition_reason` if consumers report confusion.

4. `workflow_id` is overloaded by fallback logic
- Table: `fact_workflow_sessions.workflow_id`.
- It can be derived from `pipeline_id` or fallback `pipeline`.
- Recommendation: keep for now (contract already in use), but document derivation rule in API docs.

## Migration Notes

If database objects already exist, apply column migrations before deploying updated Flink mappers:

```sql
ALTER TABLE livepeer_analytics.fact_stream_status_samples
    RENAME COLUMN attribution_method TO gpu_attribution_method;
ALTER TABLE livepeer_analytics.fact_stream_status_samples
    RENAME COLUMN attribution_confidence TO gpu_attribution_confidence;

ALTER TABLE livepeer_analytics.fact_workflow_sessions
    RENAME COLUMN attribution_method TO gpu_attribution_method;
ALTER TABLE livepeer_analytics.fact_workflow_sessions
    RENAME COLUMN attribution_confidence TO gpu_attribution_confidence;

ALTER TABLE livepeer_analytics.fact_workflow_session_segments
    RENAME COLUMN attribution_method TO gpu_attribution_method;
ALTER TABLE livepeer_analytics.fact_workflow_session_segments
    RENAME COLUMN attribution_confidence TO gpu_attribution_confidence;

ALTER TABLE livepeer_analytics.fact_workflow_param_updates
    RENAME COLUMN attribution_method TO gpu_attribution_method;
ALTER TABLE livepeer_analytics.fact_workflow_param_updates
    RENAME COLUMN attribution_confidence TO gpu_attribution_confidence;
```

Then update any dashboards/queries using old column names.
