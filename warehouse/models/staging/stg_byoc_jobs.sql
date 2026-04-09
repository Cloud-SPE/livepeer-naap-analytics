select
    event_id,
    event_ts,
    org,
    gateway,
    request_id,
    capability,
    subtype,
    source_event_type,
    success,
    duration_ms,
    http_status,
    orch_address,
    orch_url,
    orch_url_norm,
    worker_url,
    charged_compute,
    latency_ms,
    available_capacity,
    error
from naap.normalized_byoc_job final
where event_id != ''
