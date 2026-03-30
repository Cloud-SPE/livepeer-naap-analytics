with {{ latest_value_cte('latest_slices', 'naap.api_sla_compliance_by_org_store', ['window_start'], 'refresh_run_id') }}
select
    s.window_start,
    cast(null as Nullable(String)) as org,
    s.orchestrator_address,
    s.pipeline_id,
    s.model_id,
    s.gpu_id,
    s.region,
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
    if(sum(s.health_expected_signal_count) > 0, sum(s.health_signal_count) / toFloat64(sum(s.health_expected_signal_count)), 1.0) as health_signal_coverage_ratio,
    sum(s.startup_success_sessions) / nullIf(toFloat64(sum(s.requested_sessions)), 0.0) as startup_success_rate,
    sum(s.startup_excused_sessions) / nullIf(toFloat64(sum(s.requested_sessions)), 0.0) as excused_failure_rate,
    1.0 - (sum(s.effective_failed_sessions) / nullIf(toFloat64(sum(s.requested_sessions)), 0.0)) as effective_success_rate,
    1.0 - (sum(s.total_swapped_sessions) / nullIf(toFloat64(sum(s.requested_sessions)), 0.0)) as no_swap_rate,
    1.0 - ((sum(s.loading_only_sessions) + sum(s.zero_output_fps_sessions)) / nullIf(toFloat64(sum(s.requested_sessions)), 0.0)) as output_viability_rate,
    if(
        sum(s.requested_sessions) > 0,
        100.0
        * if(sum(s.health_expected_signal_count) > 0, sum(s.health_signal_count) / toFloat64(sum(s.health_expected_signal_count)), 1.0)
        * (
            (0.4 * (sum(s.startup_success_sessions) / toFloat64(sum(s.requested_sessions))))
            + (0.2 * (1.0 - sum(s.total_swapped_sessions) / toFloat64(sum(s.requested_sessions))))
            + (0.4 * (1.0 - ((sum(s.loading_only_sessions) + sum(s.zero_output_fps_sessions)) / toFloat64(sum(s.requested_sessions)))))
        ),
        cast(null as Nullable(Float64))
    ) as sla_score
from naap.api_sla_compliance_by_org_store s
inner join latest_slices l
    on {{ join_on_columns('s', 'l', ['window_start']) }}
   and s.refresh_run_id = l.refresh_run_id
group by
    s.window_start,
    s.orchestrator_address,
    s.pipeline_id,
    s.model_id,
    s.gpu_id,
    s.region
