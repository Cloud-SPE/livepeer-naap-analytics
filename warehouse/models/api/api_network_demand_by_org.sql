with {{ latest_value_cte('latest_slices', 'naap.api_network_demand_by_org_store', ['org', 'window_start'], 'refresh_run_id') }}
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
    s.loading_only_sessions,
    s.zero_output_fps_sessions,
    s.effective_failed_sessions,
    s.confirmed_swapped_sessions,
    s.inferred_swap_sessions,
    s.total_swapped_sessions,
    s.sessions_ending_in_error,
    s.error_status_samples,
    s.health_signal_count,
    s.health_expected_signal_count,
    s.health_signal_coverage_ratio,
    s.startup_success_rate,
    s.excused_failure_rate,
    s.effective_success_rate,
    s.ticket_face_value_eth
from naap.api_network_demand_by_org_store s
inner join latest_slices l
    on {{ join_on_columns('s', 'l', ['org', 'window_start']) }}
   and s.refresh_run_id = l.refresh_run_id
