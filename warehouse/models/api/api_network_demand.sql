with latest_slices as (
    select
        window_start,
        argMax(refresh_run_id, refreshed_at) as refresh_run_id
    from naap.api_network_demand_by_org_store
    group by window_start
)
select
    s.window_start,
    cast(null as Nullable(String)) as org,
    s.gateway,
    s.region,
    s.pipeline_id,
    s.model_id,
    sum(s.sessions_count) as sessions_count,
    avg(s.avg_output_fps) as avg_output_fps,
    sum(s.total_minutes) as total_minutes,
    sum(s.known_sessions_count) as known_sessions_count,
    sum(s.requested_sessions) as requested_sessions,
    sum(s.startup_success_sessions) as startup_success_sessions,
    sum(s.no_orch_sessions) as no_orch_sessions,
    sum(s.startup_excused_sessions) as startup_excused_sessions,
    sum(s.startup_failed_sessions) as startup_failed_sessions,
    sum(s.confirmed_swapped_sessions) as confirmed_swapped_sessions,
    sum(s.inferred_swap_sessions) as inferred_swap_sessions,
    sum(s.total_swapped_sessions) as total_swapped_sessions,
    sum(s.sessions_ending_in_error) as sessions_ending_in_error,
    sum(s.error_status_samples) as error_status_samples,
    avg(s.health_signal_coverage_ratio) as health_signal_coverage_ratio,
    sum(s.startup_success_sessions) / nullIf(toFloat64(sum(s.known_sessions_count)), 0.0) as startup_success_rate,
    sum(s.startup_excused_sessions) / nullIf(toFloat64(sum(s.known_sessions_count)), 0.0) as excused_failure_rate,
    sum(s.ticket_face_value_eth) as ticket_face_value_eth
from naap.api_network_demand_by_org_store s
inner join latest_slices l
    on s.window_start = l.window_start
   and s.refresh_run_id = l.refresh_run_id
group by
    s.window_start,
    s.gateway,
    s.region,
    s.pipeline_id,
    s.model_id
