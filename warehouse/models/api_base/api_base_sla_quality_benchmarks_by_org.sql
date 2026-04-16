{{ config(materialized='view') }}

with current_rows_source as (
    select
        window_start,
        org,
        orchestrator_address,
        pipeline_id,
        model_id,
        gpu_id,
        avg_prompt_to_first_frame_ms,
        avg_e2e_latency_ms,
        avg_output_fps,
        toDate(window_start) as benchmark_anchor_date
    from {{ ref('api_base_sla_quality_inputs_by_org') }}
),
current_rows as (
    select
        window_start,
        org,
        orchestrator_address,
        pipeline_id,
        model_id,
        gpu_id,
        avg_prompt_to_first_frame_ms,
        avg_e2e_latency_ms,
        avg_output_fps,
        arrayMap(offset -> benchmark_anchor_date - offset, range(1, 8)) as benchmark_dates
    from current_rows_source
),
benchmark_days as (
    select
        c.window_start,
        c.org,
        c.orchestrator_address,
        c.pipeline_id,
        c.model_id,
        c.gpu_id,
        c.avg_prompt_to_first_frame_ms,
        c.avg_e2e_latency_ms,
        c.avg_output_fps,
        arrayJoin(c.benchmark_dates) as benchmark_cohort_date
    from current_rows c
),
benchmarks as (
    select
        c.window_start,
        c.org,
        c.orchestrator_address,
        c.pipeline_id,
        c.model_id,
        c.gpu_id,
        toUInt64(sumMerge(d.ptff_row_count_state)) as ptff_benchmark_row_count,
        quantileTDigestIfMerge(0.5)(d.ptff_p50_state) as ptff_p50,
        quantileTDigestIfMerge(0.9)(d.ptff_p90_state) as ptff_p90,
        toUInt64(sumMerge(d.e2e_row_count_state)) as e2e_benchmark_row_count,
        quantileTDigestIfMerge(0.5)(d.e2e_p50_state) as e2e_p50,
        quantileTDigestIfMerge(0.9)(d.e2e_p90_state) as e2e_p90,
        toUInt64(sumMerge(d.fps_row_count_state)) as fps_benchmark_row_count,
        quantileTDigestIfMerge(0.1)(d.fps_p10_state) as fps_p10,
        quantileTDigestIfMerge(0.5)(d.fps_p50_state) as fps_p50,
        c.avg_prompt_to_first_frame_ms,
        c.avg_e2e_latency_ms,
        c.avg_output_fps
    from benchmark_days c
    left join {{ ref('api_base_sla_quality_cohort_daily_state') }} d
        on d.pipeline_id = c.pipeline_id
       and ifNull(d.model_id, '') = ifNull(c.model_id, '')
       and d.cohort_date = c.benchmark_cohort_date
    group by
        c.window_start,
        c.org,
        c.orchestrator_address,
        c.pipeline_id,
        c.model_id,
        c.gpu_id,
        c.avg_prompt_to_first_frame_ms,
        c.avg_e2e_latency_ms,
        c.avg_output_fps
)
select
    b.window_start,
    b.org,
    b.orchestrator_address,
    b.pipeline_id,
    b.model_id,
    b.gpu_id,
    if(
        b.avg_prompt_to_first_frame_ms is null or b.ptff_benchmark_row_count < 48,
        0.5,
        if(
            b.ptff_p90 <= b.ptff_p50,
            if(b.avg_prompt_to_first_frame_ms <= b.ptff_p50, 1.0, 0.0),
            multiIf(
                b.avg_prompt_to_first_frame_ms <= b.ptff_p50, 1.0,
                b.avg_prompt_to_first_frame_ms >= b.ptff_p90, 0.0,
                1.0 - ((b.avg_prompt_to_first_frame_ms - b.ptff_p50) / (b.ptff_p90 - b.ptff_p50))
            )
        )
    ) as ptff_score_raw,
    if(
        b.avg_e2e_latency_ms is null or b.e2e_benchmark_row_count < 48,
        0.5,
        if(
            b.e2e_p90 <= b.e2e_p50,
            if(b.avg_e2e_latency_ms <= b.e2e_p50, 1.0, 0.0),
            multiIf(
                b.avg_e2e_latency_ms <= b.e2e_p50, 1.0,
                b.avg_e2e_latency_ms >= b.e2e_p90, 0.0,
                1.0 - ((b.avg_e2e_latency_ms - b.e2e_p50) / (b.e2e_p90 - b.e2e_p50))
            )
        )
    ) as e2e_score_raw,
    if(
        b.avg_output_fps is null or b.fps_benchmark_row_count < 48,
        0.5,
        if(
            b.fps_p50 <= b.fps_p10,
            if(b.avg_output_fps >= b.fps_p50, 1.0, 0.0),
            multiIf(
                b.avg_output_fps >= b.fps_p50, 1.0,
                b.avg_output_fps <= b.fps_p10, 0.0,
                (b.avg_output_fps - b.fps_p10) / (b.fps_p50 - b.fps_p10)
            )
        )
    ) as fps_score_raw
from benchmarks b
