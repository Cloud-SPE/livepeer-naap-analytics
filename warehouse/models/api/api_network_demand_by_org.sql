with latest_slices as (
    select
        org,
        window_start,
        argMax(refresh_run_id, refreshed_at) as refresh_run_id
    from naap.api_network_demand_by_org_store
    group by org, window_start
)
select
    s.window_start,
    s.org,
    s.gateway,
    s.region,
    s.pipeline_id,
    s.model_id,
    s.sessions_count,
    s.avg_output_fps,
    s.total_minutes,
    s.known_sessions_count,
    s.requested_sessions,
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
    s.ticket_face_value_eth
from naap.api_network_demand_by_org_store s
inner join latest_slices l
    on s.org = l.org
   and s.window_start = l.window_start
   and s.refresh_run_id = l.refresh_run_id
