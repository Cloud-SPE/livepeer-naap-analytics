select
    event_id,
    event_ts,
    org,
    gateway,
    request_id,
    capability,
    subtype,
    orch_address,
    orch_url,
    orch_url_norm,
    success,
    error
from naap.normalized_byoc_auth final
where event_id != ''
