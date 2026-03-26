with base as (
    select
        toStartOfHour(coalesce(started_at, last_seen)) as window_start,
        org,
        ifNull(attributed_orch_address, '') as orchestrator_address,
        canonical_pipeline as pipeline_id,
        canonical_model as model_id,
        startup_outcome,
        swap_count,
        error_seen,
        status_error_sample_count,
        health_signal_coverage_ratio
    from {{ ref('fact_workflow_sessions') }}
),
inventory as (
    select
        orch_address as orchestrator_address,
        pipeline_id,
        argMax(model_id, last_seen) as model_id,
        argMax(gpu_id, last_seen) as gpu_id
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
    toUInt64(count()) as known_sessions_count,
    toUInt64(countIf(b.startup_outcome = 'success')) as startup_success_sessions,
    toUInt64(countIf(b.startup_outcome = 'excused')) as startup_excused_sessions,
    toUInt64(countIf(b.startup_outcome = 'unexcused')) as startup_unexcused_sessions,
    toUInt64(0) as confirmed_swapped_sessions,
    toUInt64(0) as inferred_swap_sessions,
    toUInt64(countIf(b.swap_count > 0)) as total_swapped_sessions,
    toUInt64(countIf(b.error_seen = 1)) as sessions_ending_in_error,
    toUInt64(sum(b.status_error_sample_count)) as error_status_samples,
    avg(b.health_signal_coverage_ratio) as health_signal_coverage_ratio,
    if(count() >= 5, toFloat64(countIf(b.startup_outcome = 'success')) / toFloat64(count()), cast(null as Nullable(Float64))) as startup_success_rate,
    if(count() >= 5, toFloat64(countIf(b.startup_outcome in ('success', 'excused'))) / toFloat64(count()), cast(null as Nullable(Float64))) as effective_success_rate,
    if(count() >= 5, 1.0 - toFloat64(countIf(b.swap_count > 0)) / toFloat64(count()), cast(null as Nullable(Float64))) as no_swap_rate,
    if(
        count() >= 5,
        0.6 * (toFloat64(countIf(b.startup_outcome = 'success')) / toFloat64(count())) +
        0.4 * (1.0 - toFloat64(countIf(b.swap_count > 0)) / toFloat64(count())),
        cast(null as Nullable(Float64))
    ) as sla_score
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
    i.gpu_id
