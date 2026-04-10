-- One row per AI batch job request, attributed by the Go resolver.
--
-- Attribution (orchestrator identity, GPU hardware, pipeline/model) is owned
-- end-to-end by the resolver and written to canonical_ai_batch_job_store via
-- the selection-centered attribution engine. This view is a thin read layer
-- over that store; it adds LLM-specific supplementary metrics from
-- stg_ai_llm_requests (token counts, TPS, TTFT) via request_id.
--
-- Attribution status classes mirror the live V2V resolver:
--   resolved       — full orchestrator + hardware identity confirmed
--   hardware_less  — orchestrator identity confirmed, no GPU data available
--   stale          — only a stale capability snapshot was available
--   ambiguous      — multiple incompatible candidates
--   unresolved     — no matching candidate found

with {{ latest_value_cte('latest', 'naap.canonical_ai_batch_job_store', ['org', 'request_id'], 'materialized_at', 'materialized_at') }}

select
    -- Explicit aliases required: ClickHouse qualifies ambiguous column names with
    -- the table alias (e.g. s.request_id) when the same column exists in multiple
    -- joined tables (here: canonical_ai_batch_job_store and stg_ai_llm_requests).
    -- Without aliases the view schema exposes "s.request_id" instead of "request_id",
    -- which breaks any downstream view that references these columns by plain name.
    s.request_id              as request_id,
    s.org                     as org,
    s.gateway                 as gateway,
    s.pipeline                as pipeline,
    s.model_id                as model_id,
    s.received_at             as received_at,
    s.completed_at            as completed_at,
    s.success                 as success,
    s.tries                   as tries,
    s.duration_ms             as duration_ms,
    s.orch_url                as orch_url,
    s.orch_url_norm           as orch_url_norm,
    s.latency_score           as latency_score,
    s.price_per_unit          as price_per_unit,
    s.error_type              as error_type,
    s.error                   as error,
    s.attribution_status      as attribution_status,
    s.attribution_reason      as attribution_reason,
    s.attribution_method      as attribution_method,
    s.attribution_confidence  as attribution_confidence,
    s.attributed_orch_uri     as attributed_orch_uri,
    s.capability_version_id   as capability_version_id,
    s.attribution_snapshot_ts as attribution_snapshot_ts,
    s.gpu_id                  as gpu_id,
    s.gpu_model_name          as gpu_model_name,
    s.gpu_memory_bytes_total  as gpu_memory_bytes_total,
    s.attributed_model        as attributed_model,
    -- LLM-specific supplementary metrics (null for non-LLM pipelines)
    l.model               as llm_model,
    l.prompt_tokens       as prompt_tokens,
    l.completion_tokens   as completion_tokens,
    l.total_tokens        as total_tokens,
    l.tokens_per_second   as tokens_per_second,
    l.ttft_ms             as ttft_ms,
    l.finish_reason       as finish_reason,
    l.streaming           as llm_streaming
from naap.canonical_ai_batch_job_store s
inner join latest
    on latest.org = s.org
   and latest.request_id = s.request_id
   and latest.materialized_at = s.materialized_at
left join {{ ref('stg_ai_llm_requests') }} l
    on l.org = s.org
   and l.request_id = s.request_id
