-- Phase 6.3: read one row per orch_address from the resolver-written
-- api_current_orchestrator_store, picking the most recent refresh slice
-- via argMax. Serves the three orchestrator-listing endpoints
-- (/v1/streaming/orchestrators, /v1/requests/orchestrators, and
-- dashboard-orchestrators) with a single MergeTree scan each.

{{ config(materialized='view') }}

with latest_slices as (
    select
        orch_address,
        argMax(refresh_run_id, refreshed_at) as refresh_run_id
    from naap.api_current_orchestrator_store
    group by orch_address
)
select
    s.orch_address,
    s.orchestrator_uri,
    s.orch_name,
    s.orch_label,
    s.last_seen,
    s.gpu_count,
    s.streaming_models,
    s.request_capability_pairs,
    s.pipelines,
    s.pipeline_model_pairs,
    s.known_sessions_count,
    s.success_sessions,
    s.requested_sessions,
    s.effective_success_rate,
    s.no_swap_rate,
    s.latest_sla_score,
    s.latest_sla_window_start
from naap.api_current_orchestrator_store as s
inner join latest_slices as l
    on  s.orch_address = l.orch_address
    and s.refresh_run_id = l.refresh_run_id
