with latest_sessions as (
    select
        canonical_session_key,
        argMax(refreshed_at, refreshed_at) as refreshed_at
    from naap.canonical_active_stream_state_latest_store
    group by canonical_session_key
)
select
    s.canonical_session_key,
    s.event_id,
    s.sample_ts,
    s.org,
    s.stream_id,
    s.request_id,
    s.gateway,
    s.pipeline,
    s.model_id,
    s.orch_address,
    s.attribution_status,
    s.attribution_reason,
    s.state,
    s.output_fps,
    s.input_fps,
    s.e2e_latency_ms,
    s.started_at,
    s.last_seen,
    s.completed
from naap.canonical_active_stream_state_latest_store s
inner join latest_sessions l
    on s.canonical_session_key = l.canonical_session_key
   and s.refreshed_at = l.refreshed_at
