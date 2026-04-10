select
    event_id,
    event_ts,
    org,
    gateway,
    request_id,
    capability,
    orch_address,
    amount,
    currency,
    payment_type
from naap.normalized_byoc_payment final
where event_id != ''
