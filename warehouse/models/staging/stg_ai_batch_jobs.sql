select
    event_id,
    event_ts,
    org,
    gateway,
    request_id,
    pipeline,
    model_id,
    subtype,
    success,
    tries,
    duration_ms,
    orch_url,
    orch_url_norm,
    latency_score,
    price_per_unit,
    error_type,
    error,
    data
from naap.normalized_ai_batch_job final
where event_id != ''
