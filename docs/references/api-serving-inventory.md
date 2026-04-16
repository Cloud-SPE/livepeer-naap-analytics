# API Serving Inventory

| Field | Value |
|---|---|
| **Status** | Active |
| **Effective date** | 2026-04-15 |
| **Audience** | API designers, warehouse developers, agents |

This document maps common API questions to the current published table set.
Use it when deciding which relations a new endpoint should query.

The rule is:

- API handlers query `api_*`.
- New serving rollups publish from `canonical_*`.
- `normalized_*` tables are lineage only, not direct API query inputs.

If a required API shape is missing additive support fields, publish a new
`api_*` contract instead of rebuilding calculations in Go.

## Query Rules

- Treat `api_current_orchestrator`, `api_current_capability_offer`, `api_current_capability_hardware`, and `api_current_active_stream_state` as snapshot facts. Apply freshness filters in the request query.
- Recompute ratios from additive numerators and denominators. Do not average `avg_*` fields or sum overlapping warm inventory counts.
- For capacity, dedupe on `(orch_address, gpu_id)` before counting GPUs or summing GPU memory. The same GPU can appear under multiple models or pipelines.
- For active orchestrators, the repo currently uses different freshness windows by surface: `10m` for the dashboard KPI online count and `30m` for capacity, pricing, pipeline catalog, and discovery.

## Inventory By API Question

| API question | Job type | Query first | Grain | Safe rollup inputs | Canonical source | Normalized lineage / notes |
|---|---|---|---|---|---|---|
| Capacity by model, pipeline, and GPU | all | `api_current_capability_offer` + `api_current_capability_hardware` | offer rows at `(org, orch_address, canonical_pipeline/offered_name, model_id, gpu_id)` plus deduped hardware rows at `(org, orch_address, gpu_id)` | recompute `warm_orch_count` with `count(distinct orch_address)`; recompute GPU slots and memory after deduping `(orch_address, gpu_id)` | `canonical_capability_offer_current`, `canonical_capability_hardware_current` | capability offers come from `PerCapability` and `hardware[]`; hardware rows are the source of GPU identity |
| Demand by model and pipeline, including no-capacity | stream | `api_hourly_streaming_demand` | one row per `(window_start, org, gateway, pipeline_id, model_id)` | `requested_sessions`, `startup_success_sessions`, `no_orch_sessions`, `startup_failed_sessions`, `effective_failed_sessions`, `output_fps_sum`, `status_samples`, `total_minutes`, `ticket_face_value_eth` | resolver-published `canonical_streaming_demand_hourly_store`, derived from canonical session state | lineage is `normalized_ai_stream_status` plus `normalized_stream_trace`; the public API grain intentionally drops region and keeps additive support fields for recomputation |
| Demand by orchestrator, pipeline, model, and GPU quality surface | stream | `api_hourly_streaming_sla`, `api_hourly_streaming_gpu_metrics` | one row per `(window_start, org, orchestrator_address, pipeline_id, model_id, gpu_id)` | `requested_sessions`, `startup_success_sessions`, `effective_failed_sessions`, `output_fps_sum`, `status_samples`, latency support sums and counts | resolver-published SLA and GPU metric stores derived from canonical session state | use `api_hourly_streaming_sla` for unresolved or null-GPU rows and `api_hourly_streaming_gpu_metrics` for hardware-resolved GPU latency or FPS analysis; both publish the API grain directly |
| Demand by model, pipeline, GPU, and no-orch breakdown | ai-batch | `api_hourly_request_demand` | one row per `(window_start, org, gateway, execution_mode, capability_family, capability_name, canonical_pipeline, canonical_model, orchestrator identity)` | `job_count`, `selected_count`, `no_orch_count`, `success_count`, `duration_ms_sum`, `price_sum`, `llm_*` additive fields | `canonical_ai_batch_jobs` | builtin request summaries and LLM rollups should use this additive hourly surface before touching request facts |
| Demand by capability or model, GPU, and no-orch breakdown | byoc | `api_fact_byoc_job` | one row per `(org, request_id)` where `request_id` is the canonical BYOC event key | `count(*)`, `countIf(selection_outcome = 'selected')`, `countIf(selection_outcome = 'no_orch')`, `countIf(success = 1)`, `sum(duration_ms)` | `canonical_byoc_jobs` | lineage is `normalized_byoc_job`; BYOC capability names remain verbatim and are not normalized into builtin pipelines |
| Hourly request demand when no-orch detail is not needed | ai-batch, byoc | `api_hourly_request_demand` | one row per `(window_start, org, gateway, execution_mode, capability_family, capability_name, canonical_pipeline, canonical_model, orchestrator identity)` | `job_count`, `selected_count`, `no_orch_count`, `success_count`, `duration_ms_sum`, `price_sum`, `llm_request_count`, `llm_success_count`, `llm_total_tokens_sum`, `llm_tokens_per_second_sum`, `llm_ttft_ms_sum`, and their sample counts | `canonical_ai_batch_jobs`, `canonical_byoc_jobs` | this is the cross-family additive request surface; derive presentation-level `job_type` from capability family and execution mode only when needed |
| Current published pricing | capability-announced pipelines | `api_current_capability_pricing` | latest quote rows per `(org, orch_address, capability_id, constraint/model)` | row-level `price_per_unit`, `pixels_per_unit`; recompute aggregates from rows | `canonical_capability_price_current` | lineage is `normalized_network_capabilities`; pricing is now published as a serving contract instead of being parsed from `raw_capabilities` in handlers |
| Current BYOC worker price | byoc | `api_current_byoc_worker` plus `api_current_capability_pricing` | one row per worker plus one row per current BYOC quote | event facts are additive; latest-state reads come from `api_current_byoc_worker` | `canonical_byoc_workers`, `canonical_capability_price_current` | use the worker lifecycle for worker identity/model and capability pricing for current quoted rates |
| Realized payment and price history | stream, byoc | no minimal public `api_*` contract; use canonical economics facts directly for internal analysis | payment event or canonical payment fact | additive: `face_value_wei`, `num_tickets`, `total_wei`, `event_count`; do not re-roll up derived averages without publishing a denominator | `canonical_payment_links`, `canonical_byoc_payments`, canonical job facts | realized payments are intentionally outside the minimal OpenAPI-serving surface after the refactor |
| Orchestrator by model, pipeline, and URI | all | `api_current_orchestrator` plus `api_current_capability_offer` | latest orchestrator snapshot joined to current pipeline/model/GPU offers | use `orch_address` as the stable join key; use `orchestrator_uri` as presentation identity | `canonical_latest_orchestrator_state`, `canonical_capability_offer_current` | use `api_hourly_request_demand` and `api_hourly_streaming_sla` only when you also need recent-work or performance fields |
| Current active streams | stream | `api_current_active_stream_state` | one row per current live session | count rows or distinct `canonical_session_key`; apply `completed = 0` and optional `last_seen` freshness in the query | `canonical_active_stream_state_latest` | lineage is `normalized_ai_stream_status` plus `normalized_stream_trace`; this is a current-state fact table, not an additive hourly rollup |
| Current active orchestrators | all | `api_current_orchestrator` | one row per latest orchestrator snapshot | count distinct orchestrators after the chosen freshness filter; join capability offers when slicing by model, pipeline, or GPU | `canonical_latest_orchestrator_state` | lineage is `normalized_network_capabilities`; there is no separate `api_active_orchestrator_state` view |

## Inventory By Job Type

| Job type | Current-state and inventory tables | Demand tables | Quality / SLA tables | Pricing / payment tables | Notes |
|---|---|---|---|---|---|
| Streaming | `api_current_orchestrator`, `api_current_capability_offer`, `api_current_capability_hardware`, `api_current_active_stream_state` | `api_hourly_streaming_demand`, `api_hourly_streaming_gpu_metrics` | `api_hourly_streaming_sla` | no public payment `api_*` contract; use canonical economics facts for internal analysis | strongest current additive contract coverage; use these before touching canonical session facts |
| AI batch | `api_current_orchestrator`, `api_current_capability_offer`, `api_fact_ai_batch_job` | `api_fact_ai_batch_job`, `api_hourly_request_demand` | `api_hourly_request_demand` | `api_fact_ai_batch_job`, `api_fact_ai_batch_llm_request` | use `api_fact_ai_batch_job` when `selection_outcome` or no-orch visibility matters |
| BYOC | `api_current_orchestrator`, `api_current_capability_offer`, `api_fact_byoc_job`, `api_current_byoc_worker` | `api_fact_byoc_job`, `api_hourly_request_demand` | `api_hourly_request_demand` | `api_current_byoc_worker`, `api_current_capability_pricing`, `api_fact_byoc_job` | current-price reads come from the capability pricing surface plus worker lifecycle identity |
| LLM enrichment | `api_hourly_request_demand`, `api_fact_ai_batch_llm_request` | additive hourly summaries from `api_hourly_request_demand`; request-level drilldown from `api_fact_ai_batch_llm_request` | none beyond job-level joins today | `api_fact_ai_batch_llm_request.price_per_unit` | use the hourly additive `llm_*` columns for summaries and keep the one-row-per-request fact only for drilldown or audit trails |

## Recommended Additive Fields

When a new endpoint needs a wider window or a higher grain than the stored
serving table, prefer tables that already expose these support fields:

- streaming demand: `requested_sessions`, `startup_success_sessions`, `no_orch_sessions`, `effective_failed_sessions`, `output_fps_sum`, `status_samples`, `total_minutes`
- streaming GPU demand: the same fields, plus `orchestrator_address`, `gpu_id`, and `gpu_identity_status`
- request-job facts: `selection_outcome`, `success`, `duration_ms`, `price_per_unit`
- realized payments: `face_value_wei`, `num_tickets`, `total_wei`, `event_count`

Fields that are not safe to aggregate directly:

- `avg_output_fps`, `avg_duration_ms`, `success_rate`, `effective_success_rate`, `sla_score`
- `warm_orch_count`, `gpu_slots`, or `total_capacity` after rows from overlapping model or pipeline cohorts have been merged
- `avg_price_wei_per_pixel` without publishing the denominator needed to recompute it

## Current Gaps

- A few warehouse validation tests and alert/ops surfaces still need occasional alignment as the minimal `api_*` surface settles, but the serving namespace itself now reflects the refactor.
- If capacity APIs need frequent cross-model or cross-capability rollups, a dedicated deduped inventory summary may still be worth publishing later.
- Realized payment history remains intentionally outside the minimal OpenAPI-serving surface; use canonical economics facts for internal analysis.

## Final Spine Summary

The current serving contract after the refactor is:

- `api_current_*` for latest-state serving rows
- `api_hourly_*` for additive hourly serving rows
- `api_fact_*` for request-level facts that callers may page or aggregate

Those public models sit on:

- `canonical_*` latest-state facts
- `canonical_*_store` bounded hourly rollups
- canonical job facts such as `canonical_ai_batch_jobs` and `canonical_byoc_jobs`

`job_type` is no longer a storage-level organizing dimension for the serving spine. Where a response still needs it, derive it from capability family plus execution mode at the API boundary.

The active API inventory above reflects the current implementation. Earlier proposal material that used the old `job_type`-centric spine shape or legacy `api_*` names has been superseded by the minimal capability-aware surface described here.
