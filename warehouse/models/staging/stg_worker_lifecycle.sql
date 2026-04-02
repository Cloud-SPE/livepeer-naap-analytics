select
    event_id,
    event_ts,
    org,
    gateway,
    capability,
    orch_address,
    orch_url,
    orch_url_norm,
    worker_url,
    price_per_unit,
    model,
    worker_options_raw,
    data
from naap.normalized_worker_lifecycle final
where event_id != ''
