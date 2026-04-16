{{ config(materialized='view') }}

with scored_inputs as (
    select
        i.*,
        b.ptff_score_raw,
        b.e2e_score_raw,
        b.fps_score_raw
    from {{ ref('api_base_sla_quality_inputs') }} i
    inner join {{ ref('api_base_sla_quality_benchmarks') }} b
        on i.window_start = b.window_start
       and i.orchestrator_address = b.orchestrator_address
       and i.pipeline_id = b.pipeline_id
       and ifNull(i.model_id, '') = ifNull(b.model_id, '')
       and ifNull(i.gpu_id, '') = ifNull(b.gpu_id, '')
),
component_scores as (
    select
        s.*,
        if(
            s.requested_sessions > 0,
            least(
                greatest(
                    (0.4 * s.startup_success_rate)
                    + (0.2 * s.no_swap_rate)
                    + (0.4 * s.output_viability_rate),
                    0.0
                ),
                1.0
            ),
            cast(null as Nullable(Float64))
        ) as reliability_score,
        least(
            greatest(
                0.5 + least(s.prompt_to_first_frame_sample_count / 10.0, 1.0) * (s.ptff_score_raw - 0.5),
                0.0
            ),
            1.0
        ) as ptff_score,
        least(
            greatest(
                0.5 + least(s.e2e_latency_sample_count / 10.0, 1.0) * (s.e2e_score_raw - 0.5),
                0.0
            ),
            1.0
        ) as e2e_score,
        least(
            greatest(
                0.5 + least(s.status_samples / 30.0, 1.0) * (s.fps_score_raw - 0.5),
                0.0
            ),
            1.0
        ) as fps_score
    from scored_inputs s
)
select
    s.window_start,
    s.org,
    s.orchestrator_address,
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
    least(greatest(0.6 * s.ptff_score + 0.4 * s.e2e_score, 0.0), 1.0) as latency_score,
    s.fps_score,
    least(greatest((0.6 * (0.6 * s.ptff_score + 0.4 * s.e2e_score)) + (0.4 * s.fps_score), 0.0), 1.0) as quality_score,
    'quality-benchmark-v1' as sla_semantics_version,
    if(
        s.requested_sessions > 0,
        least(
            greatest(
                100.0 * s.health_signal_coverage_ratio * (
                    (0.7 * s.reliability_score) + (0.3 * least(
                        greatest(
                            (0.6 * (0.6 * s.ptff_score + 0.4 * s.e2e_score)) + (0.4 * s.fps_score),
                            0.0
                        ),
                        1.0
                    ))
                ),
                0.0
            ),
            100.0
        ),
        cast(null as Nullable(Float64))
    ) as sla_score
from component_scores s
