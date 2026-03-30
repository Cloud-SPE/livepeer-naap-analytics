with latest_slices as (
    select
        org,
        window_start,
        argMax(refresh_run_id, refreshed_at) as refresh_run_id
    from naap.api_sla_compliance_by_org_store
    group by org, window_start
)
select
    s.window_start,
    s.org,
    s.orchestrator_address,
    s.pipeline_id,
    s.model_id,
    s.gpu_id,
    s.region,
    s.known_sessions_count,
    s.startup_success_sessions,
    s.no_orch_sessions,
    s.startup_excused_sessions,
    s.startup_failed_sessions,
    s.confirmed_swapped_sessions,
    s.inferred_swap_sessions,
    s.total_swapped_sessions,
    s.sessions_ending_in_error,
    s.error_status_samples,
    s.health_signal_coverage_ratio,
    s.startup_success_rate,
    s.excused_failure_rate,
    s.no_swap_rate,
    s.sla_score
from naap.api_sla_compliance_by_org_store s
inner join latest_slices l
    on s.org = l.org
   and s.window_start = l.window_start
   and s.refresh_run_id = l.refresh_run_id
