with latest_events as (
    select
        event_id,
        argMax(refreshed_at, refreshed_at) as refreshed_at
    from naap.canonical_status_samples_recent_store
    group by event_id
)
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
    on s.event_id = l.event_id
   and s.refreshed_at = l.refreshed_at
