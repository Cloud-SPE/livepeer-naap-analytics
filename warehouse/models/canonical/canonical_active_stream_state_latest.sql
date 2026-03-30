with {{ latest_value_cte('latest_sessions', 'naap.canonical_active_stream_state_latest_store', ['canonical_session_key'], 'refreshed_at') }}
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
    on {{ join_on_columns('s', 'l', ['canonical_session_key']) }}
   and s.refreshed_at = l.refreshed_at
