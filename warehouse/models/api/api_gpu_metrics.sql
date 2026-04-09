with {{ latest_value_cte('latest_slices', 'naap.api_gpu_metrics_by_org_store', ['org', 'window_start'], 'refresh_run_id') }}
select
    s.window_start,
    cast(null as Nullable(String)) as org,
    s.orchestrator_address,
    s.pipeline_id,
    s.model_id,
    s.gpu_id,
    s.region,
    if(sum(s.status_samples) > 0, sum(s.output_fps_sum) / toFloat64(sum(s.status_samples)), 0.0) as avg_output_fps,
    sum(s.output_fps_sum) as output_fps_sum,
    quantileTDigestMerge(s.output_fps_p95_state) as p95_output_fps,
    cast(null as Nullable(Float64)) as fps_jitter_coefficient,
    sum(s.status_samples) as status_samples,
    sum(s.error_status_samples) as error_status_samples,
    sum(s.health_signal_count) as health_signal_count,
    sum(s.health_expected_signal_count) as health_expected_signal_count,
    least(
        if(sum(s.health_expected_signal_count) > 0, sum(s.health_signal_count) / toFloat64(sum(s.health_expected_signal_count)), 1.0),
        1.0
    ) as health_signal_coverage_ratio,
    any(s.gpu_model_name) as gpu_model_name,
    any(s.gpu_memory_bytes_total) as gpu_memory_bytes_total,
    any(s.runner_version) as runner_version,
    any(s.cuda_version) as cuda_version,
    sum(s.prompt_to_first_frame_sum_ms) / nullIf(toFloat64(sum(s.prompt_to_first_frame_sample_count)), 0.0) as avg_prompt_to_first_frame_ms,
    sum(s.prompt_to_first_frame_sum_ms) as prompt_to_first_frame_sum_ms,
    sum(s.startup_latency_sum_ms) / nullIf(toFloat64(sum(s.startup_latency_sample_count)), 0.0) as avg_startup_latency_ms,
    sum(s.startup_latency_sum_ms) as startup_latency_sum_ms,
    sum(s.e2e_latency_sum_ms) / nullIf(toFloat64(sum(s.e2e_latency_sample_count)), 0.0) as avg_e2e_latency_ms,
    sum(s.e2e_latency_sum_ms) as e2e_latency_sum_ms,
    quantileTDigestIfMerge(s.prompt_to_first_frame_p95_state) as p95_prompt_to_first_frame_latency_ms,
    quantileTDigestIfMerge(s.startup_latency_p95_state) as p95_startup_latency_ms,
    quantileTDigestIfMerge(s.e2e_latency_p95_state) as p95_e2e_latency_ms,
    sum(s.prompt_to_first_frame_sample_count) as prompt_to_first_frame_sample_count,
    sum(s.startup_latency_sample_count) as startup_latency_sample_count,
    sum(s.e2e_latency_sample_count) as e2e_latency_sample_count,
    sum(s.known_sessions_count) as known_sessions_count,
    sum(s.startup_success_sessions) as startup_success_sessions,
    sum(s.no_orch_sessions) as no_orch_sessions,
    sum(s.startup_excused_sessions) as startup_excused_sessions,
    sum(s.startup_failed_sessions) as startup_failed_sessions,
    sum(s.confirmed_swapped_sessions) as confirmed_swapped_sessions,
    sum(s.inferred_swap_sessions) as inferred_swap_sessions,
    sum(s.total_swapped_sessions) as total_swapped_sessions,
    sum(s.sessions_ending_in_error) as sessions_ending_in_error,
    sum(s.startup_failed_sessions) / nullIf(toFloat64(sum(s.known_sessions_count)), 0.0) as startup_failed_rate,
    sum(s.total_swapped_sessions) / nullIf(toFloat64(sum(s.known_sessions_count)), 0.0) as swap_rate
from naap.api_gpu_metrics_by_org_store s
inner join latest_slices l
    on {{ join_on_columns('s', 'l', ['org', 'window_start']) }}
   and s.refresh_run_id = l.refresh_run_id
group by
    s.window_start,
    s.orchestrator_address,
    s.pipeline_id,
    s.model_id,
    s.gpu_id,
    s.region
