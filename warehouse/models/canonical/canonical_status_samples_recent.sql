with {{ latest_value_cte('latest_events', 'naap.canonical_status_samples_recent_store', ['event_id'], 'refreshed_at') }}
select
    s.canonical_session_key,
    s.event_id,
    s.sample_ts,
    s.org,
    s.stream_id,
    s.request_id,
    s.gateway,
    s.orch_address,
    s.pipeline,
    s.model_id,
    s.attribution_status,
    s.attribution_reason,
    s.state,
    s.output_fps,
    s.input_fps,
    s.e2e_latency_ms,
    s.is_attributed
from naap.canonical_status_samples_recent_store s
inner join latest_events l
    on {{ join_on_columns('s', 'l', ['event_id']) }}
   and s.refreshed_at = l.refreshed_at
