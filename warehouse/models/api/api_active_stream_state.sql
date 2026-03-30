select
    event_id,
    sample_ts,
    org,
    stream_id,
    request_id,
    gateway,
    pipeline,
    model_id,
    orch_address,
    attribution_status,
    attribution_reason,
    state,
    output_fps,
    input_fps,
    e2e_latency_ms,
    started_at,
    last_seen,
    completed
from {{ ref('canonical_active_stream_state_latest') }}
where completed = 0
  and nullIf(stream_id, '') is not null
