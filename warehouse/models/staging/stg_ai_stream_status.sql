select
    event_id,
    event_ts,
    org,
    gateway,
    stream_id,
    request_id,
    canonical_session_key,
    raw_pipeline_hint,
    state,
    orch_raw_address,
    orch_url,
    output_fps,
    input_fps,
    e2e_latency_ms,
    restart_count,
    last_error,
    last_error_ts,
    data
from naap.normalized_ai_stream_status final
where event_id != ''
