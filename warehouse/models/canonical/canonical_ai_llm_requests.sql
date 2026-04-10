-- One row per completed LLM request within the AI batch pipeline.
-- Links to canonical_ai_batch_jobs via (org, request_id) for per-job
-- enrichment. This model must stay one-row-per-request so joins into
-- canonical_ai_batch_jobs cannot inflate job counts.

with {{ latest_rows_cte(
    'latest',
    ref('stg_ai_llm_requests'),
    ['org', 'request_id'],
    ['event_ts desc', 'event_id desc']
) }}

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
    error
from latest
where request_id != ''
