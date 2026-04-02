select
    event_id,
    event_ts,
    org,
    gateway,
    request_id,
    model,
    orch_url,
    orch_url_norm,
    subtype,
    streaming,
    prompt_tokens,
    completion_tokens,
    total_tokens,
    total_duration_ms,
    tokens_per_second,
    latency_score,
    price_per_unit,
    ttft_ms,
    finish_reason,
    error,
    data
from naap.normalized_ai_llm_request final
where event_id != ''
