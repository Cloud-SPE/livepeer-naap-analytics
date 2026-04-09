{{ config(materialized='view') }}

select
    toDate(s.window_start) as cohort_date,
    s.pipeline_id,
    s.model_id,
    -- Benchmark state is built from additive org-hour SLA inputs, never from
    -- previously scored SLA rows. The serving models merge the prior 7 full
    -- UTC days of these states for each current row.
    quantileTDigestIfState(
        0.5
    )(ifNull(s.avg_prompt_to_first_frame_ms, 0.0), toUInt8(s.avg_prompt_to_first_frame_ms is not null)) as ptff_p50_state,
    quantileTDigestIfState(
        0.9
    )(ifNull(s.avg_prompt_to_first_frame_ms, 0.0), toUInt8(s.avg_prompt_to_first_frame_ms is not null)) as ptff_p90_state,
    sumState(toUInt64(s.avg_prompt_to_first_frame_ms is not null)) as ptff_row_count_state,
    quantileTDigestIfState(
        0.5
    )(ifNull(s.avg_e2e_latency_ms, 0.0), toUInt8(s.avg_e2e_latency_ms is not null)) as e2e_p50_state,
    quantileTDigestIfState(
        0.9
    )(ifNull(s.avg_e2e_latency_ms, 0.0), toUInt8(s.avg_e2e_latency_ms is not null)) as e2e_p90_state,
    sumState(toUInt64(s.avg_e2e_latency_ms is not null)) as e2e_row_count_state,
    quantileTDigestIfState(
        0.1
    )(ifNull(s.avg_output_fps, 0.0), toUInt8(s.avg_output_fps is not null)) as fps_p10_state,
    quantileTDigestIfState(
        0.5
    )(ifNull(s.avg_output_fps, 0.0), toUInt8(s.avg_output_fps is not null)) as fps_p50_state,
    sumState(toUInt64(s.avg_output_fps is not null)) as fps_row_count_state
from {{ ref('api_base_sla_quality_inputs_by_org') }} s
where s.pipeline_id != ''
group by
    cohort_date,
    s.pipeline_id,
    s.model_id
