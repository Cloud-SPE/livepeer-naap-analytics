# System Architecture

## Scope
Live Video AI stream analytics for Livepeer, with forward-compatible design for future inference/BYOC/batch workflows.

## Ownership Boundaries
- Flink owns correctness:
  - dedup
  - session identity
  - lifecycle classification
  - stateful correlations
- ClickHouse owns serving:
  - curated storage
  - rollups
  - stable API views
  - health-check query surfaces

## Execution Split
- Non-stateful projections in ClickHouse MVs:
  - `fact_stream_status_samples`
  - `fact_stream_trace_edges`
  - `fact_stream_ingest_samples`
- Stateful lifecycle facts in Flink:
  - `fact_workflow_sessions`
  - `fact_workflow_session_segments`
  - `fact_workflow_param_updates`

## Key Contracts
- Session identity: deterministic composite key (`stream_id|request_id` with fallbacks).
- Canonical orchestrator identity: derived from capability snapshots (`local_address` -> canonical).
- Attribution contract: method + confidence + TTL-based nearest snapshot logic.
