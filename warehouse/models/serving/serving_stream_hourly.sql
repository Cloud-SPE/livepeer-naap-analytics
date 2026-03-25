with session_gateway as (
    select canonical_session_key, anyIf(gateway, gateway != '') as gateway
    from {{ ref('stg_stream_trace') }}
    where canonical_session_key != ''
    group by canonical_session_key
)
select
    toStartOfHour(coalesce(f.started_at, f.last_seen)) as hour,
    f.org,
    f.canonical_pipeline as pipeline,
    ifNull(sg.gateway, '') as gateway,
    countIf(f.started = 1) as started,
    countIf(f.startup_outcome = 'success') as completed,
    countIf(f.no_orch = 1) as no_orch,
    countIf(f.swap_count > 0) as orch_swap
from {{ ref('fact_workflow_sessions') }} f
left join session_gateway sg on f.canonical_session_key = sg.canonical_session_key
where f.canonical_session_key != ''
group by hour, f.org, pipeline, gateway
