# Schema and Serving Model

## Core Table Classes
- `raw/typed`: parsed event tables from Flink.
- `fact_*`: curated analytical facts at explicit grain.
- `dim_*`: enrichment snapshots/current projections.
- `agg_*`: rollups for high-traffic reads.
- `v_api_*`: stable serving interfaces for UI/API consumers.

## Current Lifecycle Facts
- `fact_workflow_sessions`: one row per workflow session (versioned upsert).
- `fact_workflow_session_segments`: one row per session segment (versioned upsert).
- `fact_workflow_param_updates`: one row per params update marker.

## MergeTree Engine Guidance
- `MergeTree`: append-only raw/non-stateful sample facts.
- `ReplacingMergeTree`: mutable/latest session and segment snapshots.
- `AggregatingMergeTree`: rollup aggregate-state tables.

## Serving Views
- `v_api_gpu_metrics`
- `v_api_network_demand`
- `v_api_sla_compliance`

## Pending Serving Extension
- Param-update reliability:
  - `agg_param_update_reliability_1m` (planned)
  - `v_api_param_update_reliability` (planned)
