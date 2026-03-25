with base as (
    select
        canonical_session_key,
        toStartOfHour(coalesce(started_at, last_seen)) as window_start,
        org,
        canonical_pipeline as pipeline_id,
        canonical_model as model_id,
        startup_outcome,
        swap_count,
        error_seen,
        status_error_sample_count,
        health_signal_coverage_ratio
    from {{ ref('fact_workflow_sessions') }}
),
gateway_lookup as (
    select
        canonical_session_key,
        argMax(gateway, sample_ts) as gateway
    from {{ ref('serving_status_samples') }}
    group by canonical_session_key
),
session_fps as (
    select
        canonical_session_key,
        avg(output_fps) as avg_output_fps
    from {{ ref('serving_status_samples') }}
    group by canonical_session_key
),
session_duration as (
    select
        canonical_session_key,
        greatest(dateDiff('millisecond', min(sample_ts), max(sample_ts)), 0) / 60000.0 as total_minutes
    from {{ ref('serving_status_samples') }}
    group by canonical_session_key
),
payments as (
    select
        canonical_session_key,
        sum(face_value_wei) / 1000000000000000000.0 as ticket_face_value_eth
    from {{ ref('fact_workflow_payment_links') }}
    where canonical_session_key is not null
    group by canonical_session_key
)
select
    b.window_start,
    b.org,
    ifNull(g.gateway, '') as gateway,
    cast(null as Nullable(String)) as region,
    b.pipeline_id,
    b.model_id,
    toUInt64(count()) as sessions_count,
    avg(ifNull(sf.avg_output_fps, 0.0)) as avg_output_fps,
    sum(ifNull(sd.total_minutes, 0.0)) as total_minutes,
    toUInt64(count()) as known_sessions_count,
    toUInt64(countIf(b.startup_outcome = 'success')) as served_sessions,
    toUInt64(countIf(b.startup_outcome = 'failed')) as unserved_sessions,
    toUInt64(count()) as total_demand_sessions,
    toUInt64(countIf(b.startup_outcome = 'failed')) as startup_unexcused_sessions,
    toUInt64(0) as confirmed_swapped_sessions,
    toUInt64(0) as inferred_swap_sessions,
    toUInt64(countIf(b.swap_count > 0)) as total_swapped_sessions,
    toUInt64(countIf(b.error_seen = 1)) as sessions_ending_in_error,
    toUInt64(sum(b.status_error_sample_count)) as error_status_samples,
    avg(b.health_signal_coverage_ratio) as health_signal_coverage_ratio,
    toFloat64(countIf(b.startup_outcome = 'success')) / nullIf(toFloat64(count()), 0.0) as startup_success_rate,
    toFloat64(countIf(b.startup_outcome in ('success', 'excused'))) / nullIf(toFloat64(count()), 0.0) as effective_success_rate,
    sum(ifNull(p.ticket_face_value_eth, 0.0)) as ticket_face_value_eth
from base b
left join gateway_lookup g on b.canonical_session_key = g.canonical_session_key
left join session_fps sf on b.canonical_session_key = sf.canonical_session_key
left join session_duration sd on b.canonical_session_key = sd.canonical_session_key
left join payments p on b.canonical_session_key = p.canonical_session_key
group by
    b.window_start,
    b.org,
    ifNull(g.gateway, ''),
    b.pipeline_id,
    b.model_id
