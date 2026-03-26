with base as (
    select
        h.hour as window_start,
        h.org,
        ifNull(h.orch_address, '') as orchestrator_address,
        h.canonical_pipeline as pipeline_id,
        h.canonical_model as model_id,
        h.status_samples,
        h.error_samples as error_status_samples,
        h.avg_output_fps,
        h.avg_e2e_latency_ms,
        fs.health_signal_coverage_ratio,
        fs.startup_outcome,
        fs.swap_count,
        fs.error_seen,
        fs.startup_latency_ms
    from {{ ref('fact_workflow_status_hours') }} h
    left join {{ ref('fact_workflow_sessions') }} fs on h.canonical_session_key = fs.canonical_session_key
    where h.is_terminal_tail_artifact = 0
),
inventory as (
    select
        orch_address as orchestrator_address,
        pipeline_id,
        argMax(model_id, last_seen) as model_id,
        argMax(gpu_id, last_seen) as gpu_id,
        argMax(gpu_model_name, last_seen) as gpu_model_name,
        argMax(gpu_memory_bytes_total, last_seen) as gpu_memory_bytes_total,
        argMax(runner_version, last_seen) as runner_version,
        argMax(cuda_version, last_seen) as cuda_version
    from {{ ref('serving_latest_orchestrator_pipeline_models') }}
    group by orchestrator_address, pipeline_id
)
select
    b.window_start,
    b.org,
    b.orchestrator_address,
    b.pipeline_id,
    coalesce(b.model_id, i.model_id) as model_id,
    i.gpu_id,
    cast(null as Nullable(String)) as region,
    avg(b.avg_output_fps) as avg_output_fps,
    quantile(0.95)(b.avg_output_fps) as p95_output_fps,
    cast(null as Nullable(Float64)) as fps_jitter_coefficient,
    toUInt64(sum(b.status_samples)) as status_samples,
    toUInt64(sum(b.error_status_samples)) as error_status_samples,
    avg(b.health_signal_coverage_ratio) as health_signal_coverage_ratio,
    i.gpu_model_name,
    i.gpu_memory_bytes_total,
    i.runner_version,
    i.cuda_version,
    cast(null as Nullable(Float64)) as avg_prompt_to_first_frame_ms,
    avgIf(b.startup_latency_ms, b.startup_latency_ms > 0) as avg_startup_latency_ms,
    avgIf(b.avg_e2e_latency_ms, b.avg_e2e_latency_ms > 0) as avg_e2e_latency_ms,
    cast(null as Nullable(Float64)) as p95_prompt_to_first_frame_latency_ms,
    quantileTDigestIf(0.95)(b.startup_latency_ms, b.startup_latency_ms > 0) as p95_startup_latency_ms,
    quantileIf(0.95)(b.avg_e2e_latency_ms, b.avg_e2e_latency_ms > 0) as p95_e2e_latency_ms,
    toUInt64(0) as prompt_to_first_frame_sample_count,
    toUInt64(countIf(b.startup_latency_ms > 0)) as startup_latency_sample_count,
    toUInt64(countIf(b.avg_e2e_latency_ms > 0)) as e2e_latency_sample_count,
    toUInt64(count()) as known_sessions_count,
    toUInt64(countIf(b.startup_outcome = 'success')) as startup_success_sessions,
    toUInt64(countIf(b.startup_outcome = 'excused')) as startup_excused_sessions,
    toUInt64(countIf(b.startup_outcome = 'unexcused')) as startup_unexcused_sessions,
    toUInt64(0) as confirmed_swapped_sessions,
    toUInt64(0) as inferred_swap_sessions,
    toUInt64(countIf(b.swap_count > 0)) as total_swapped_sessions,
    toUInt64(countIf(b.error_seen = 1)) as sessions_ending_in_error,
    toFloat64(countIf(b.startup_outcome = 'unexcused')) / nullIf(toFloat64(count()), 0.0) as startup_unexcused_rate,
    toFloat64(countIf(b.swap_count > 0)) / nullIf(toFloat64(count()), 0.0) as swap_rate
from base b
left join inventory i
  on b.orchestrator_address = i.orchestrator_address
 and b.pipeline_id = i.pipeline_id
group by
    b.window_start,
    b.org,
    b.orchestrator_address,
    b.pipeline_id,
    coalesce(b.model_id, i.model_id),
    i.gpu_id,
    i.gpu_model_name,
    i.gpu_memory_bytes_total,
    i.runner_version,
    i.cuda_version
