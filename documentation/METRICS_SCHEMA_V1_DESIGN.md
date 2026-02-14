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
- Attribution quality: `attribution_method`, `attribution_confidence` expose enrichment certainty.
- Raw mapping:
- `stream_trace_events` -> lifecycle edges (`known_stream`, edge timestamps, swap evidence)
- `ai_stream_events` -> error counts/taxonomy
- `ai_stream_status` -> session timeline context

### `fact_workflow_session_segments`
- Grain: one row per segment (`workflow_session_id`, `segment_index`).
- Purpose: actor attribution over time (gateway/orchestrator/worker/GPU transitions).
- Metric use: swap behavior, per-actor lifecycle analysis.
- Attribution quality: `attribution_method`, `attribution_confidence` expose enrichment certainty per segment.
- Raw mapping: primarily `stream_trace_events` + Flink sessionization logic.

### `fact_stream_status_samples`
- Grain: one row per status sample timestamp.
- Purpose: FPS and state timeseries.
- Metric use: output FPS, FPS jitter coefficient.
- Attribution quality: `attribution_method`, `attribution_confidence` expose enrichment certainty per sample.
- Raw mapping: `ai_stream_status` (+ capability enrichment fields).

### `fact_stream_trace_edges`
- Grain: one row per normalized trace edge.
- Purpose: latency edge pairing and lifecycle classification evidence.
- Metric use: startup, e2e proxy, prompt-to-playable proxy, swap evidence.
- Raw mapping: `stream_trace_events`.

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

### `agg_latency_edges_1m`
- Grain: 1 minute x session/request.
- Purpose: latency KPI serving with explicit pair coverage counts.
- Join-back keys: `workflow_session_id`, `stream_id`, `request_id`.

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
- Prompt-to-First-Frame (proxy/playable): covered with `start_time` + trace edges in `fact_workflow_sessions` / `agg_latency_edges_1m`.
- Failure Rate (unexcused proxy): covered via `fact_workflow_sessions` classification fields + `agg_reliability_1h`.
- Swap Rate: covered via `swap_count` in `fact_workflow_sessions` and segment transitions in `fact_workflow_session_segments`.
- Up/Down Bandwidth (target GPU/orchestrator): not covered by current events; planned via `fact_orchestrator_transport_bandwidth`.

## API Mapping

- `/gpu/metrics` -> `v_api_gpu_metrics` (FPS/jitter by GPU/model/orchestrator/time).
- `/network/demand` -> `v_api_network_demand` (active streams/sessions and throughput proxies by gateway/region/workflow).
- `/sla/compliance` -> `v_api_sla_compliance` (success/no-swap components; weighting applied in API layer).

## Flink Responsibilities Required for V1

- Compute `workflow_session_id` deterministically and consistently across all event streams.
- Emit `event_uid`, `source`, `version` for idempotent writes.
- Normalize trace taxonomy: classify trace edges and flag swap events.
- Sessionize and classify startup outcomes (`success`, `excused`, `unexcused`) from trace + error taxonomy.
- Enrich facts with nearest valid orchestrator capability snapshot (`model_id`, `gpu_id`, `capability_id`, `capability_name`, `capability_group`) when available.
- Emit attribution quality fields (`attribution_method`, `attribution_confidence`) for auditability.
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
