# Metrics Schema V1 Design

## Purpose
Define a ClickHouse serving schema for Live Video AI stream analytics where:
- Flink owns correctness: dedup, schema validation, correlation, sessionization.
- ClickHouse owns serving: curated facts, rollups, API/dashboards, drill-through.
- Design remains extensible for future BYOC and batch inference workflows.

Capability catalog source:
- Seed file: `configs/reference/capability_catalog.v1.json`
- Governance: PR updates + Flink redeploy
- Unknown IDs resolve to `unknown`

## Design Pattern: `dim` vs `fact` vs `agg`

### `fact_*` tables
- Store clean, queryable events at a stable analytical grain.
- Preserve drill-through keys (`workflow_session_id`, `stream_id`, `request_id`, `source_event_uid`).
- Used for investigation and ad hoc analysis beyond pre-defined KPIs.

### `dim_*` tables
- Store low-cardinality enrichment attributes used by facts/rollups.
- Example: orchestrator capability snapshots and latest orchestrator metadata.
- Keep as data-driven tables sourced from events/snapshots, not config files.

### `agg_*` tables
- Pre-aggregate high-traffic KPI query patterns by time bucket and dimensions.
- Feed dashboards/APIs with fast predictable latency.
- Always include join-back identifiers in group-by dimensions so users can drill down to facts.

## Table Inventory (V1)

## Dimensions

### `dim_orchestrator_capability_snapshots`
- Grain: one row per capability snapshot (`orchestrator_address + pipeline_id + model_id + snapshot_ts`).
- Purpose: time-aware enrichment and audits (what capability data was known at a given time).
- Key fields: `capability_id`, `capability_name`, `capability_group`, `gpu_id`, `model_id`, `runner_version`.
- Raw mapping: sourced from `network_capabilities`.

### `dim_orchestrator_capability_current`
- Grain: latest row per orchestrator/model/pipeline.
- Purpose: fast lookup for serving joins where historical snapshot precision is not needed.
- Key fields: `capability_id`, `capability_name`, `capability_group`, `gpu_id`, `model_id`.
- Raw mapping: ClickHouse-derived latest projection from `dim_orchestrator_capability_snapshots` (not Flink-written).

### `dim_orchestrator_capability_advertised_snapshots`
- Grain: one row per source capability event and capability id.
- Purpose: complete 1-to-many capability inventory from a single network capability event.
- Key fields: `capability_id`, `capacity`, `capability_catalog_version`.
- Raw mapping: sourced from `network_capabilities.capabilities.capacities`.

### `dim_orchestrator_capability_model_constraints`
- Grain: one row per source capability event, capability id, and model id.
- Purpose: preserve per-model runner/capacity/warm constraints for attribution and audits.
- Key fields: `capability_id`, `model_id`, `runner_version`, `capacity`, `capacity_in_use`, `warm`.
- Raw mapping: sourced from `network_capabilities.capabilities.constraints.PerCapability`.

### `dim_orchestrator_capability_prices`
- Grain: one row per source capability event and capability price entry.
- Purpose: preserve pricing metadata for capability constraints.
- Key fields: `capability_id`, `constraint_name`, `price_per_unit`, `pixels_per_unit`.
- Raw mapping: sourced from `network_capabilities.capabilities_prices`.

## Facts

### `fact_workflow_sessions`
- Grain: one row per `workflow_session_id`.
- Purpose: canonical lifecycle row for reliability and session-level drill-through.
- Metric use: denominator and classification source for failure/swap/startup outcomes.
- Attribution quality: `gpu_attribution_method`, `gpu_attribution_confidence` expose enrichment certainty.
- Raw mapping:
- `stream_trace_events` -> lifecycle edges (`known_stream`, edge timestamps, swap evidence)
- `ai_stream_events` -> error counts/taxonomy
- `ai_stream_status` -> session timeline context

### `fact_workflow_session_segments`
- Grain: one row per segment (`workflow_session_id`, `segment_index`).
- Purpose: actor attribution over time (gateway/orchestrator/worker/GPU transitions).
- Metric use: swap behavior, per-actor lifecycle analysis.
- Attribution quality: `gpu_attribution_method`, `gpu_attribution_confidence` expose enrichment certainty per segment.
- Raw mapping: primarily `stream_trace_events` + Flink sessionization logic.

### `fact_stream_status_samples`
- Grain: one row per status sample timestamp.
- Purpose: FPS and state timeseries.
- Metric use: output FPS, FPS jitter coefficient.
- Attribution quality: `gpu_attribution_method`, `gpu_attribution_confidence` expose enrichment certainty per sample.
- Raw mapping: `ai_stream_status` (+ capability enrichment fields).

### `fact_stream_trace_edges`
- Grain: one row per normalized trace edge.
- Purpose: latency edge pairing and lifecycle classification evidence.
- Metric use: startup, e2e proxy, prompt-to-playable proxy, swap evidence.
- Raw mapping: `stream_trace_events`.

### `fact_workflow_latency_samples`
- Grain: one row per workflow-session snapshot version.
- Purpose: deterministic latency KPI derivation from Flink-owned edge semantics.
- Metric use: prompt-to-first-frame, startup time, e2e latency (+ coverage counts).
- Key fields: `prompt_to_first_frame_ms`, `startup_time_ms`, `e2e_latency_ms`, `edge_semantics_version`.
- Raw mapping: derived from `fact_workflow_sessions` edge timestamps in Flink.

### `fact_stream_ingest_samples`
- Grain: one row per ingest snapshot.
- Purpose: ingest-leg (WHIP) quality metrics.
- Metric use: ingest jitter/latency observability, not orchestrator transport bandwidth KPI.
- Raw mapping: `stream_ingest_metrics`.

### `fact_orchestrator_transport_bandwidth` (future)
- Grain: one row per orchestrator transport sample.
- Purpose: true bandwidth KPI source for gateway<->orchestrator and orchestrator<->worker links.
- Raw mapping: planned scraper payloads.

## Rollups

### `agg_stream_performance_1m`
- Grain: 1 minute x orchestrator/workflow/model/GPU/region/gateway.
- Purpose: fast serving for FPS/jitter/active sessions.
- Join-back keys: dimensions + `workflow_session_id`/`stream_id` in facts.

### `agg_reliability_1h`
- Grain: 1 hour x gateway/orchestrator/workflow/model/GPU/region.
- Purpose: reliability KPIs (success/excused/unexcused/swap rates).
- Join-back keys: dimensions + `workflow_session_id` in `fact_workflow_sessions`.

### `agg_latency_kpis_1m`
- Grain: 1 minute x orchestrator/workflow/model/GPU/region.
- Purpose: serve latency KPIs with validity counts.
- Join-back keys: dimensions + `workflow_session_id` in `fact_workflow_latency_samples`.

## Raw -> Fact Mapping

- `ai_stream_status` -> `fact_stream_status_samples`, `fact_workflow_sessions` (session boundaries/context)
- `stream_trace_events` -> `fact_stream_trace_edges`, `fact_workflow_sessions`, `fact_workflow_session_segments`
- `ai_stream_events` -> `fact_workflow_sessions` (error taxonomy and counts)
- `stream_ingest_metrics` -> `fact_stream_ingest_samples`
- `network_capabilities` -> `dim_orchestrator_capability_snapshots` -> `dim_orchestrator_capability_current`

## Execution Split (Locked)

- ClickHouse MVs produce non-stateful silver facts:
- `fact_stream_status_samples`
- `fact_stream_trace_edges`
- `fact_stream_ingest_samples`
- Flink produces stateful lifecycle facts:
- `fact_workflow_sessions`
- `fact_workflow_session_segments`

Reasoning:
- non-stateful projections are 1:1 reshapes and are cheaper to maintain in SQL.
- sessionization/classification logic must stay deterministic and testable in Flink.

## Metrics Coverage Matrix

- Output FPS: covered now via `fact_stream_status_samples` + `agg_stream_performance_1m`.
- Jitter Coefficient (FPS): covered now via `fact_stream_status_samples` + `agg_stream_performance_1m`.
- Startup Time (proxy): covered with edge pairs from `fact_stream_trace_edges` and session timestamps in `fact_workflow_sessions`.
- E2E Stream Latency (proxy): covered with agreed edge pairs in `fact_stream_trace_edges`.
- Prompt-to-First-Frame (proxy/playable): derived from edge timestamps in `fact_workflow_sessions`; serving rollup is planned as Flink-owned derived fact/aggregate (not ClickHouse edge-pair SQL).
- Failure Rate (unexcused proxy): covered via `fact_workflow_sessions` classification fields + `agg_reliability_1h`.
- Swap Rate: covered via `swap_count` in `fact_workflow_sessions` and segment transitions in `fact_workflow_session_segments`.
- Up/Down Bandwidth (target GPU/orchestrator): not covered by current events; planned via `fact_orchestrator_transport_bandwidth`.

## API Mapping

- `/gpu/metrics` -> `v_api_gpu_metrics` (FPS/jitter by GPU/model/orchestrator/time).
- `/gpu/metrics` latency extension -> `v_api_gpu_metrics` (prompt-to-first-frame/startup/e2e from `agg_latency_kpis_1m`).
- `/network/demand` -> `v_api_network_demand` (active streams/sessions and throughput proxies by gateway/region/workflow).
- `/network/demand` (supply slice) -> `v_api_network_demand_by_gpu` (GPU-type/capacity proxy by gateway/orchestrator/model/GPU/time).
- `/sla/compliance` -> `v_api_sla_compliance` (success/no-swap components; weighting applied in API layer).

## API Contract Summary (Canonical)

| API Endpoint | Backing View | Canonical Grain | Supported Fields (Current/Planned) | Explicitly Out of Scope (Now) |
|---|---|---|---|---|
| `/gpu/metrics` | `v_api_gpu_metrics` | 1 minute x orchestrator/pipeline/model/gpu/region | keys: `window_start`, `orchestrator_address`, `pipeline`, `pipeline_id`, `model_id`, `gpu_id`, `region`; metadata: `gpu_name`, `gpu_memory_total`, `cuda_version`, `runner_version`; KPIs: `avg_output_fps`, `p95_output_fps`, `jitter_coeff_fps`, `status_samples`, `known_sessions`, `success_sessions`, `excused_sessions`, `unexcused_sessions`, `swapped_sessions`, `failure_rate`, `swap_rate`; latency (planned via Flink-derived fact): `prompt_to_first_frame_ms`, `startup_time_ms`, `e2e_latency_ms` | `network_up_mbps`, `network_down_mbps`, `cue` |
| `/network/demand` | `v_api_network_demand` | 1 hour x gateway/region/pipeline | `total_streams`, `total_sessions`, `total_inference_minutes`, `known_sessions`, `unexcused_sessions`, `swapped_sessions`, `missing_capacity_count`, `fee_payment_eth`, `avg_output_fps`, `success_ratio` | `staking_lpt` |
| `/network/demand` (supply slice) | `v_api_network_demand_by_gpu` (planned) | 1 hour x gateway/orchestrator/gpu_type/pipeline/region | `inference_minutes_by_gpu_type`, `used_inference_minutes`, `available_capacity_minutes` (proxy), `capacity_rate`, `missing_capacity_count`, `fee_payment_eth` | none beyond source availability |
| `/sla/compliance` | `v_api_sla_compliance` | 1 hour x orchestrator/pipeline/model/gpu/region | `known_sessions`, `success_sessions`, `excused_sessions`, `unexcused_sessions`, `swapped_sessions`, `success_ratio`, `no_swap_ratio`, `sla_score` | none |

Contract rule:
- Additive fields are authoritative for client-side re-rollups.
- Derived ratios/scores must be recomputed from additive fields when aggregating to larger windows.

## Dimension Join Policy for API Views

Use capability dimensions based on the question the view is answering:

- `*_current` answers "what is true now" (latest metadata labels/inventory).
- `*_snapshots` answers "what was true at event time" (historically accurate metadata).

### View-specific policy

- `v_api_gpu_metrics`:
  - Primary use: realtime operations and dashboard health panels.
  - Join policy: use `dim_orchestrator_capability_current` for descriptive metadata.
  - Note: historical rows may display current metadata labels when capabilities change.

- `v_api_network_demand`:
  - Primary use: historical demand, usage, and cost reporting.
  - Join policy: prefer `dim_orchestrator_capability_snapshots` with an as-of time join
    (latest snapshot at or before the bucket timestamp).
  - Goal: historical buckets reflect metadata known at that time.

- `v_api_sla_compliance`:
  - Core SLA math should not depend on dimension joins.
  - If labels are added, use `*_current` for display-only dashboards, or `*_snapshots`
    when historical label correctness is required.

### Join safety rules

- Do not mix `*_current` and `*_snapshots` semantics in one API view.
- Keep one declared grain per view and ensure dimension joins stay 1:1 at that grain.
- If a dimension join can become 1:N, pre-deduplicate to one row per join key before joining.

## Flink Responsibilities Required for V1

- Compute `workflow_session_id` deterministically and consistently across all event streams.
- Emit `event_uid`, `source`, `version` for idempotent writes.
- Normalize trace taxonomy: classify trace edges and flag swap events.
- Sessionize and classify startup outcomes (`success`, `excused`, `unexcused`) from trace + error taxonomy.
- Enrich facts with nearest valid orchestrator capability snapshot (`model_id`, `gpu_id`, `capability_id`, `capability_name`, `capability_group`) when available.
- Emit attribution quality fields (`gpu_attribution_method`, `gpu_attribution_confidence`) for auditability.
- Produce stateful lifecycle facts directly: `fact_workflow_sessions`, `fact_workflow_session_segments`.

## What Stays in ClickHouse

- Non-stateful silver fact materialization from typed raw tables.
- Rollups and API views.
- Read-time joins to dimensions for non-critical enrichment.
- Health/freshness checks and backfill SQL.

## Standardization Boundary (Recommendation)

- Put deterministic, business-critical logic in Flink:
- identity, dedup, sessionization, edge pairing rules, failure taxonomy classification.
- Keep flexible serving logic in ClickHouse:
- rollup windows, API-specific view shapes, optional exploratory derivations.

This split avoids metric drift between consumers while preserving fast iteration in serving queries.
