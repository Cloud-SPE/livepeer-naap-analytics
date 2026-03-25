select
    event_id,
    event_ts,
    org,
    gateway,
    stream_id,
    request_id,
    {{ canonical_session_key("org", "stream_id", "request_id") }} as canonical_session_key,
    raw_pipeline_hint,
    event_name,
    orch_raw_address,
    orch_url,
    message,
    data
from naap.typed_ai_stream_events final
where event_id != ''
