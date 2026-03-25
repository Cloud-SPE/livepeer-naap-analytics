select
    event_id,
    event_ts,
    org,
    gateway,
    stream_id,
    request_id,
    {{ canonical_session_key("org", "stream_id", "request_id") }} as canonical_session_key,
    trace_type,
    raw_pipeline_hint,
    pipeline_id,
    orch_raw_address,
    orch_url,
    message,
    data
from naap.typed_stream_trace final
where event_id != ''
