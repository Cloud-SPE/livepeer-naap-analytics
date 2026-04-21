-- Reads pre-scored rows from api_hourly_streaming_sla_store directly. The
-- resolver's insertFinalSLAComplianceRollups writes a scored row per
-- (window_start, orchestrator_address, pipeline_id, model_id, gpu_id) on
-- every refresh run, so an API-layer query is an O(window_start) primary-key
-- lookup plus a latest-slice pick — no per-request scoring, no benchmark
-- cohort recomputation. api_base_* was retired in Phase 5; scoring math now
-- lives inline in the resolver (see docs/exec-plans/completed/serving-layer-v2.md).

{{ config(materialized='view') }}

with latest_slices as (
    select window_start, argMax(refresh_run_id, refreshed_at) as refresh_run_id
    from naap.api_hourly_streaming_sla_store
    group by window_start
)
select
    s.window_start,
    s.org,
    s.orchestrator_address,
    s.orchestrator_uri,
    s.pipeline_id,
    s.model_id,
    s.gpu_id,
    s.gpu_model_name,
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
from naap.api_hourly_streaming_sla_store as s
inner join latest_slices as l
    on  s.window_start = l.window_start
    and s.refresh_run_id = l.refresh_run_id
