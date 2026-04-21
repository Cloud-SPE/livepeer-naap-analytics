select
    a.canonical_session_key,
    a.event_id,
    a.sample_ts,
    a.org,
    a.stream_id,
    a.request_id,
    a.gateway,
    a.pipeline,
    a.model_id,
    a.orch_address,
    coalesce(nullIf(a.orchestrator_uri, ''), nullIf(s.attributed_orch_uri, ''), '') as orchestrator_uri,
    a.attribution_status,
    a.attribution_reason,
    a.state,
    a.output_fps,
    a.input_fps,
    a.e2e_latency_ms,
    a.started_at,
    a.last_seen,
    a.completed,
    s.gpu_id,
    s.attributed_orch_address
from {{ ref('canonical_active_stream_state_latest') }} a
left join {{ ref('canonical_session_current') }} s
    on a.org = s.org
   and a.canonical_session_key = s.canonical_session_key
where ifNull(a.stream_id, '') != ''
