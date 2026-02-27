# Schema and Metric Contracts

## Table Taxonomy

- `dim_*`: enrichment snapshots/current projections.
- `fact_*`: curated analytical facts at explicit grain.
- `agg_*`: rollups for high-traffic reads.
- `v_api_*`: stable serving interfaces for API/dashboard consumers.

Primary schema source: `configs/clickhouse-init/01-schema.sql`.

## Core Facts

- Lifecycle/session facts:
  - `fact_workflow_sessions`
  - `fact_workflow_session_segments`
  - `fact_workflow_param_updates`
- Sample/edge facts:
  - `fact_stream_status_samples`
  - `fact_stream_trace_edges`
  - `fact_workflow_latency_samples`
  - `fact_stream_ingest_samples`

## Dimension Model

- Snapshot source of truth:
  - `dim_orchestrator_capability_snapshots`
- Latest-state projection:
  - `dim_orchestrator_capability_current`
- Capability expansion dimensions:
  - `dim_orchestrator_capability_advertised_snapshots`
  - `dim_orchestrator_capability_model_constraints`
  - `dim_orchestrator_capability_prices`

## Metric Contract Status

| Metric | Status | Canonical Source |
|---|---|---|
| Output FPS | Live | `fact_stream_status_samples` + rollups |
| Jitter coefficient (FPS) | Live | `fact_stream_status_samples` + rollups |
| Startup time proxy | Live/derived | lifecycle edge contract + latency facts |
| E2E proxy latency | Live/derived | lifecycle edge contract + latency facts |
| Prompt-to-playable/first-frame proxy | Live/derived | status + edge pairing |
| Failure rate (unexcused) | Live (contracted) | `fact_workflow_sessions` |
| Swap rate | Live (contracted) | `fact_workflow_sessions` + segments |
| Up/Down orchestrator transport bandwidth | Blocked | requires new telemetry/fact source |

## API Serving Grains

- `v_api_gpu_metrics`: minute-level orchestrator/pipeline/model/GPU grain.
- `v_api_network_demand`: hour-level gateway/region/pipeline grain.
- `v_api_sla_compliance`: hour-level orchestrator/pipeline/model/GPU grain.

Contract rule: additive fields are canonical; clients must recompute ratios/scores when re-rolling windows.

## Key Metrics and API Contracts

- Key metrics enabled by contract:
  - output FPS
  - FPS jitter coefficient
  - startup time proxy
  - E2E proxy latency
  - prompt-to-playable/first-frame proxy
  - failure rate (unexcused)
  - swap rate
- Blocked metric:
  - up/down orchestrator transport bandwidth (requires new telemetry source)
- API contracts:
  - `/gpu/metrics` -> `v_api_gpu_metrics`
  - `/network/demand` -> `v_api_network_demand` (+ GPU supply slice via `v_api_network_demand_by_gpu`)
  - `/sla/compliance` -> `v_api_sla_compliance`

## Raw-to-Fact and Execution Contract

- Raw -> fact mapping:
  - `ai_stream_status` -> `fact_stream_status_samples`
  - `stream_trace_events` -> `fact_stream_trace_edges`
  - `stream_ingest_metrics` -> `fact_stream_ingest_samples`
  - `stream_trace_events` + `ai_stream_events` + `fact_stream_status_samples` -> Flink lifecycle facts
  - `network_capabilities*` -> capability dimensions
- Execution split:
  - ClickHouse MVs own non-stateful per-event reshaping.
  - Flink owns stateful lifecycle correctness (sessionization, classification, latency derivation, versioning).

## Lifecycle Rules Contract

- Session identity contract:
  - canonical key: `workflow_session_id`.
  - deterministic composition:
    - `stream_id + '|' + request_id` when both are present.
    - missing-side fallbacks, and hashed fallback when both are missing.
  - reason: `request_id` alone is reusable; `stream_id` alone can mask multiple attempts.
- Edge dictionary (lifecycle signals):
  - `gateway_receive_stream_request`: known-stream denominator membership.
  - `gateway_receive_first_processed_segment`: first processed output at gateway.
  - `gateway_receive_few_processed_segments`: startup success/playable signal.
  - `gateway_send_first_ingest_segment`: ingest-to-processing start edge.
  - `runner_send_first_processed_segment`: runner first processed output edge.
  - `gateway_no_orchestrators_available`: explicit excused startup failure signal.
  - `orchestrator_swap`: explicit swap signal.
- Trace category normalization:
  - `gateway_*` -> `gateway`
  - `orchestrator_*` -> `orchestrator`
  - `runner_*` -> `runner`
  - `app_*` -> `app`
  - otherwise -> `other`
- Metric edge definitions:
  - startup proxy: `gateway_receive_stream_request` -> `gateway_receive_first_processed_segment`
  - E2E proxy: `gateway_send_first_ingest_segment` -> `runner_send_first_processed_segment`
  - prompt-to-playable proxy: `ai_stream_status.start_time` -> `gateway_receive_few_processed_segments`
- Startup outcome classification:
  - classes: `success`, `excused`, `unexcused`, `not_in_denominator` (debug only).
  - rule order:
    1. unknown stream -> `not_in_denominator`
    2. playable signal present -> `success`
    3. no-orchestrator signal present -> `excused`
    4. all session errors excusable -> `excused`
    5. otherwise -> `unexcused`
  - excusable error taxonomy (substring match):
    - `no orchestrators available`
    - `mediamtx ingest disconnected`
    - `whip disconnected`
    - `missing video`
    - `ice connection state failed`
    - `user disconnected`
- Swap detection:
  - `confirmed_swap_count`: explicit `orchestrator_swap` trace evidence.
  - `inferred_orchestrator_change_count`: unique canonical orchestrator count > 1 in the session.
  - `swap_count` (legacy compatibility): mirrors confirmed swaps (`confirmed_swap_count`).
  - gold rollups expose both split metrics and also keep `swapped_sessions` as the union of confirmed/inferred.
  - segment boundary opens on orchestrator identity change.
- GPU/model attribution:
  - enrich by nearest capability snapshot using canonicalized orchestrator identity and model-matched join rules.
  - attribution fields: `model_id`, `gpu_id`, `gpu_attribution_method`, `gpu_attribution_confidence`.
  - default attribution TTL: `24h`.
  - method enum:
    - `exact_orchestrator_time_match`
    - `nearest_prior_snapshot`
    - `nearest_snapshot_within_ttl`
    - `proxy_address_join`
    - `none`

### Attribution Selection Contract

This is a correctness-critical contract for lifecycle facts and downstream API views.

1. `pipeline` source of truth:
`pipeline` is event-derived lifecycle state. `pipeline` is not sourced from capability snapshots.

2. Capability candidate cache model:
Candidates are keyed by hot wallet and stored as bounded multi-candidate snapshot sets. Candidate sets are TTL-pruned and hard-capped per wallet.

3. Selection precedence:
Compatibility first: incompatible model candidates must be ignored when pipeline context is present. Freshness second: choose exact match, then nearest prior within TTL, then nearest within TTL. Deterministic tie-breakers are required.

4. Empty-pipeline handling:
Selector input pipeline must use signal pipeline with fallback to persisted lifecycle pipeline/model context. Passing empty pipeline broadens compatibility and can reintroduce mixed-model drift.

5. No-compatible-candidate behavior:
Do not overwrite `model_id`/`gpu_id` for that signal. Continue lifecycle signal processing and row emission.

6. Regression guardrails:
Prod-derived regression tests must cover mixed-model hot-wallet candidate sets, empty signal pipeline drift path, and incompatible-but-fresh candidate rejection.

## Rules-to-Implementation Traceability

- Flink rule owners:
  - sessionization and startup/swap classification:
    - `flink-jobs/src/main/java/com/livepeer/analytics/lifecycle/WorkflowSessionStateMachine.java`
  - latency KPI derivation and semantics versioning:
    - `flink-jobs/src/main/java/com/livepeer/analytics/lifecycle/WorkflowLatencyDerivation.java`
  - lifecycle coverage diagnostics:
    - `flink-jobs/src/main/java/com/livepeer/analytics/lifecycle/WorkflowLifecycleCoverageAggregatorFunction.java`
  - sink mappings for persisted contract fields:
    - `flink-jobs/src/main/java/com/livepeer/analytics/sink/ClickHouseRowMappers.java`
- ClickHouse rule owners:
  - canonical MV/fact/view contract:
    - `configs/clickhouse-init/01-schema.sql`
  - key objects impacted by lifecycle rules:
    - `mv_ai_stream_status_to_fact_stream_status_samples`
    - `mv_stream_trace_events_to_fact_stream_trace_edges`
    - `fact_workflow_sessions`
    - `fact_workflow_session_segments`
    - `fact_workflow_latency_samples`
    - `fact_lifecycle_edge_coverage`
    - `agg_reliability_1h`
    - `agg_latency_kpis_1m`
    - `v_api_gpu_metrics`
    - `v_api_network_demand`
    - `v_api_network_demand_by_gpu`
    - `v_api_sla_compliance`

## Join Policy

- Use `*_current` dimensions for "what is true now" labels/inventory.
- Use `*_snapshots` dimensions for historical correctness at event time.
- Do not mix current and snapshot semantics in the same view contract.

## Wallet Identity Contract

- Canonical wallet identity for silver/gold/API layers is `orchestrator_address` (official wallet).
- Raw event tables can contain proxy/local wallet values in `orchestrator_address`:
  - `ai_stream_status`
  - `stream_trace_events`
  - `discovery_results`
- `network_capabilities` is the identity bridge and intentionally keeps both:
  - `orchestrator_address`: official/canonical wallet
  - `local_address`: proxy/local wallet reference
- Join rule:
  - Downstream-to-downstream joins: use canonical `orchestrator_address` directly.
  - Raw-to-downstream joins: map through `network_capabilities` (`local_address` -> `orchestrator_address`) before joining.
- Canonicalization expectation:
  - `fact_*`, `agg_*`, and `v_api_*` relations persist canonical `orchestrator_address`.
  - `network_capabilities*` relations preserve both canonical and local identity fields for traceability.
  - Flink attribution lookup should tolerate both identity representations at ingest boundaries:
    capability context is indexed by both `network_capabilities.local_address` (hot wallet) and
    `network_capabilities.orchestrator_address` (canonical), while persisted gold outputs remain canonical.

## Naming and Semantics Invariants

- Keep attribution fields explicitly GPU-scoped:
  - `gpu_attribution_method`
  - `gpu_attribution_confidence`
- Use versioned semantics fields for edge/metric changes (`edge_semantics_version`).
- Treat lifecycle contract changes as versioned contract migrations, not silent query edits.

## Outstanding Improvement Backlog

- Canonical backlog now lives in `docs/references/ISSUES_BACKLOG.md`.
- Keep this document focused on active contracts and invariants; add new backlog items in the canonical backlog file.

## Change Procedure (Schema or Metric Semantics)

1. Update schema and/or lifecycle contract docs:
   - `configs/clickhouse-init/01-schema.sql`
   - `docs/data/SCHEMA_AND_METRIC_CONTRACTS.md`
2. Update Flink parser/model/mappers and tests.
3. Re-run:
   - `cd flink-jobs && mvn test`
   - `tests/integration/run_all.sh`
4. Update validation SQL and canonical docs in the same PR.

## Detailed References

- Validation query packs: `docs/reports/METRICS_VALIDATION_QUERIES.sql`
- Ops validation query packs: `docs/reports/OPS_ACTIVITY_VALIDATION_QUERIES.sql`
- End-to-end trace notebook: `tests/python/notebooks/FLINK_DATA_TRACE_AND_INTEGRATION_TESTS.ipynb`
- Canonical testing guide: `docs/quality/TESTING_AND_VALIDATION.md`
