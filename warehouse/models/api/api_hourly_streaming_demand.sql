with {{ latest_value_cte('latest_slices', 'naap.canonical_streaming_demand_hourly_store', ['org', 'window_start'], 'refresh_run_id') }}
select
    s.window_start,
    s.org,
    s.gateway,
    s.region,
    s.pipeline_id,
    s.model_id,
    sum(s.sessions_count) as sessions_count,
    if(sum(s.status_samples) > 0, sum(s.output_fps_sum) / toFloat64(sum(s.status_samples)), 0.0) as avg_output_fps,
    sum(s.output_fps_sum) as output_fps_sum,
    sum(s.status_samples) as status_samples,
    sum(s.total_minutes) as total_minutes,
    sum(s.known_sessions_count) as known_sessions_count,
    sum(s.requested_sessions) as requested_sessions,
    sum(s.startup_success_sessions) as startup_success_sessions,
    sum(s.no_orch_sessions) as no_orch_sessions,
    sum(s.startup_excused_sessions) as startup_excused_sessions,
    sum(s.startup_failed_sessions) as startup_failed_sessions,
    sum(s.loading_only_sessions) as loading_only_sessions,
    sum(s.zero_output_fps_sessions) as zero_output_fps_sessions,
    sum(s.effective_failed_sessions) as effective_failed_sessions,
    sum(s.confirmed_swapped_sessions) as confirmed_swapped_sessions,
    sum(s.inferred_swap_sessions) as inferred_swap_sessions,
    sum(s.total_swapped_sessions) as total_swapped_sessions,
    sum(s.sessions_ending_in_error) as sessions_ending_in_error,
    sum(s.error_status_samples) as error_status_samples,
    sum(s.health_signal_count) as health_signal_count,
    sum(s.health_expected_signal_count) as health_expected_signal_count,
    least(
        if(sum(s.health_expected_signal_count) > 0, sum(s.health_signal_count) / toFloat64(sum(s.health_expected_signal_count)), 1.0),
        1.0
    ) as health_signal_coverage_ratio,
    sum(s.startup_success_sessions) / nullIf(toFloat64(sum(s.requested_sessions)), 0.0) as startup_success_rate,
    sum(s.startup_excused_sessions) / nullIf(toFloat64(sum(s.requested_sessions)), 0.0) as excused_failure_rate,
    1.0 - (sum(s.effective_failed_sessions) / nullIf(toFloat64(sum(s.requested_sessions)), 0.0)) as effective_success_rate,
    sum(s.ticket_face_value_eth) as ticket_face_value_eth
from naap.canonical_streaming_demand_hourly_store s
inner join latest_slices l
    on {{ join_on_columns('s', 'l', ['org', 'window_start']) }}
   and s.refresh_run_id = l.refresh_run_id
group by
    s.window_start,
    s.org,
    s.gateway,
    s.region,
    s.pipeline_id,
    s.model_id
