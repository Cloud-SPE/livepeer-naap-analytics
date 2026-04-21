-- Phase 6.1: read pre-aggregated rows from api_hourly_request_demand_store.
-- The resolver writes one row per (org, window_start, gateway, capability,
-- pipeline_id, model, orchestrator) grain via insertHourlyRequestDemand on
-- every refresh run, so an API-layer query is an O(window_start) primary-key
-- lookup plus a latest-slice pick — no per-request rescan of the two canonical
-- job feeds.
--
-- The previous on-demand view UNION-ed canonical_ai_batch_jobs and
-- canonical_byoc_jobs with duplicated column lists; the resolver writer now
-- owns that shape and the view becomes a thin alias.

{{ config(materialized='view') }}

with latest_slices as (
    select
        org,
        window_start,
        argMax(refresh_run_id, refreshed_at) as refresh_run_id
    from naap.api_hourly_request_demand_store
    group by org, window_start
)
select
    s.window_start,
    s.org,
    s.gateway,
    s.execution_mode,
    s.capability_family,
    s.capability_name,
    s.capability_id,
    s.canonical_pipeline,
    s.pipeline_id,
    s.canonical_model,
    s.orchestrator_address,
    s.orchestrator_uri,
    s.job_count,
    s.selected_count,
    s.no_orch_count,
    s.success_count,
    s.duration_ms_sum,
    s.price_sum,
    s.llm_request_count,
    s.llm_success_count,
    s.llm_total_tokens_sum,
    s.llm_total_tokens_sample_count,
    s.llm_tokens_per_second_sum,
    s.llm_tokens_per_second_sample_count,
    s.llm_ttft_ms_sum,
    s.llm_ttft_ms_sample_count
from naap.api_hourly_request_demand_store as s
inner join latest_slices as l
    on  s.org = l.org
    and s.window_start = l.window_start
    and s.refresh_run_id = l.refresh_run_id
