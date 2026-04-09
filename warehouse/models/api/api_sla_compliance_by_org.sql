with latest_rows as (
    select
        org,
        window_start,
        orchestrator_address,
        pipeline_id,
        ifNull(model_id, '') as model_id_key,
        ifNull(gpu_id, '') as gpu_id_key,
        ifNull(region, '') as region_key,
        argMax(refresh_run_id, refreshed_at) as refresh_run_id
    from naap.api_sla_compliance_by_org_store
    group by
        org,
        window_start,
        orchestrator_address,
        pipeline_id,
        model_id_key,
        gpu_id_key,
        region_key
)
select
    s.window_start,
    s.org,
    s.orchestrator_address,
    s.pipeline_id,
    s.model_id,
    s.gpu_id,
    s.gpu_model_name,
    s.region,
    s.known_sessions_count,
    s.requested_sessions,
    s.startup_success_sessions,
    s.no_orch_sessions,
    s.startup_excused_sessions,
    s.startup_failed_sessions,
    s.loading_only_sessions,
    s.zero_output_fps_sessions,
    s.output_failed_sessions,
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
    s.no_swap_rate,
    s.output_viability_rate,
    s.output_fps_sum,
    s.status_samples,
    s.avg_output_fps,
    s.prompt_to_first_frame_sum_ms,
    s.prompt_to_first_frame_sample_count,
    s.avg_prompt_to_first_frame_ms,
    s.e2e_latency_sum_ms,
    s.e2e_latency_sample_count,
    s.avg_e2e_latency_ms,
    s.reliability_score,
    s.ptff_score,
    s.e2e_score,
    s.latency_score,
    s.fps_score,
    s.quality_score,
    s.sla_semantics_version,
    s.sla_score
from naap.api_sla_compliance_by_org_store s
inner join latest_rows l
    on s.org = l.org
   and s.window_start = l.window_start
   and s.orchestrator_address = l.orchestrator_address
   and s.pipeline_id = l.pipeline_id
   and ifNull(s.model_id, '') = l.model_id_key
   and ifNull(s.gpu_id, '') = l.gpu_id_key
   and ifNull(s.region, '') = l.region_key
   and s.refresh_run_id = l.refresh_run_id
