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
    countIf(f.requested_seen = 1) as requested_sessions,
    countIf(f.startup_outcome = 'success') as startup_success_sessions,
    countIf(f.selection_outcome = 'no_orch') as no_orch_sessions,
    countIf(f.startup_outcome = 'failed' and f.excusal_reason != 'none') as startup_excused_sessions,
    countIf(f.startup_outcome = 'failed' and f.excusal_reason = 'none') as startup_failed_sessions,
    countIf(f.swap_count > 0) as orch_swap_sessions
from {{ ref('canonical_session_current') }} f
left join session_gateway sg on f.canonical_session_key = sg.canonical_session_key
where f.canonical_session_key != ''
group by hour, f.org, pipeline, gateway
