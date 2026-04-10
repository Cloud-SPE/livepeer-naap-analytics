-- One row per AI batch job request, attributed by the Go resolver.
--
-- Attribution (orchestrator identity, GPU hardware, pipeline/model) is owned
-- end-to-end by the resolver and written to canonical_ai_batch_job_store via
-- the selection-centered attribution engine. This view is a thin read layer
-- over that store; it adds LLM-specific supplementary metrics from the
-- deduped canonical_ai_llm_requests view. This model must stay one-row-per-job
-- keyed by (org, request_id).
--
-- Attribution status classes mirror the live V2V resolver:
--   resolved       — full orchestrator + hardware identity confirmed
--   hardware_less  — orchestrator identity confirmed, no GPU data available
--   stale          — only a stale capability snapshot was available
--   ambiguous      — multiple incompatible candidates
--   unresolved     — no matching candidate found

with {{ latest_rows_cte(
    'latest',
    'naap.canonical_ai_batch_job_store',
    ['org', 'request_id'],
    [
        'materialized_at desc',
        'completed_at desc',
        'resolver_run_id desc',
        "ifNull(capability_version_id, '') desc",
        "ifNull(attributed_orch_uri, '') desc"
    ]
) }}

select
    latest.request_id              as request_id,
    latest.org                     as org,
    latest.gateway                 as gateway,
    latest.pipeline                as pipeline,
    latest.model_id                as model_id,
    latest.received_at             as received_at,
    latest.completed_at            as completed_at,
    latest.success                 as success,
    latest.tries                   as tries,
    latest.duration_ms             as duration_ms,
    latest.orch_url                as orch_url,
    latest.orch_url_norm           as orch_url_norm,
    latest.latency_score           as latency_score,
    latest.price_per_unit          as price_per_unit,
    latest.error_type              as error_type,
    latest.error                   as error,
    latest.attribution_status      as attribution_status,
    latest.attribution_reason      as attribution_reason,
    latest.attribution_method      as attribution_method,
    latest.attribution_confidence  as attribution_confidence,
    latest.attributed_orch_uri     as attributed_orch_uri,
    latest.capability_version_id   as capability_version_id,
    latest.attribution_snapshot_ts as attribution_snapshot_ts,
    latest.gpu_id                  as gpu_id,
    latest.gpu_model_name          as gpu_model_name,
    latest.gpu_memory_bytes_total  as gpu_memory_bytes_total,
    latest.attributed_model        as attributed_model,
    -- LLM-specific supplementary metrics (null for non-LLM pipelines)
    l.model               as llm_model,
    l.prompt_tokens       as prompt_tokens,
    l.completion_tokens   as completion_tokens,
    l.total_tokens        as total_tokens,
    l.tokens_per_second   as tokens_per_second,
    l.ttft_ms             as ttft_ms,
    l.finish_reason       as finish_reason,
    l.streaming           as llm_streaming
from latest
left join {{ ref('canonical_ai_llm_requests') }} l
    on l.org = latest.org
   and l.request_id = latest.request_id
