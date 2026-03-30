with latest_slices as (
    select
        org,
        window_start,
        argMax(refresh_run_id, refreshed_at) as refresh_run_id
    from naap.api_gpu_metrics_by_org_store
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
    s.avg_output_fps,
    s.p95_output_fps,
    s.fps_jitter_coefficient,
    s.status_samples,
    s.error_status_samples,
    s.health_signal_coverage_ratio,
    s.gpu_model_name,
    s.gpu_memory_bytes_total,
    s.runner_version,
    s.cuda_version,
    s.avg_prompt_to_first_frame_ms,
    s.avg_startup_latency_ms,
    s.avg_e2e_latency_ms,
    s.p95_prompt_to_first_frame_latency_ms,
    s.p95_startup_latency_ms,
    s.p95_e2e_latency_ms,
    s.prompt_to_first_frame_sample_count,
    s.startup_latency_sample_count,
    s.e2e_latency_sample_count,
    s.known_sessions_count,
    s.startup_success_sessions,
    s.no_orch_sessions,
    s.startup_excused_sessions,
    s.startup_failed_sessions,
    s.confirmed_swapped_sessions,
    s.inferred_swap_sessions,
    s.total_swapped_sessions,
    s.sessions_ending_in_error,
    s.startup_failed_rate,
    s.swap_rate
from naap.api_gpu_metrics_by_org_store s
inner join latest_slices l
    on s.org = l.org
   and s.window_start = l.window_start
   and s.refresh_run_id = l.refresh_run_id
