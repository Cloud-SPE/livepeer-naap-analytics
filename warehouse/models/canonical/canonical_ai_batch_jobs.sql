-- One row per AI batch job request.
-- Covers all fixed pipelines (text-to-image, image-to-image, image-to-video,
-- upscale, audio-to-text, llm, segment-anything-2, image-to-text, text-to-speech).
--
-- LLM-specific metrics (token counts, TPS, TTFT) are joined from stg_ai_llm_requests
-- via request_id; columns are null for non-LLM pipelines.
--
-- GPU hardware attribution uses argMaxIf to find the capability interval that was
-- active at job completion time (valid_from_ts <= completed_at), joining on
-- orch_uri_norm equality. attribution_status reflects whether a match was found.

with received as (
    select
        request_id,
        org,
        gateway,
        pipeline,
        model_id,
        event_ts as received_at
    from {{ ref('stg_ai_batch_jobs') }}
    where subtype = 'ai_batch_request_received'
      and request_id != ''
),

completed as (
    select
        request_id,
        org,
        success,
        tries,
        duration_ms,
        orch_url,
        orch_url_norm,
        latency_score,
        price_per_unit,
        error_type,
        error,
        event_ts as completed_at
    from {{ ref('stg_ai_batch_jobs') }}
    where subtype = 'ai_batch_request_completed'
      and request_id != ''
),

llm as (
    select
        request_id,
        org,
        model                as llm_model,
        prompt_tokens,
        completion_tokens,
        total_tokens,
        total_duration_ms    as llm_duration_ms,
        tokens_per_second,
        ttft_ms,
        finish_reason,
        streaming            as llm_streaming
    from {{ ref('stg_ai_llm_requests') }}
    where request_id != ''
),

-- Time-valid attribution: for each completed job, find the capability interval
-- whose valid_from_ts is the largest value <= completed_at (i.e. was active at
-- job time). argMaxIf avoids range conditions in JOIN ON, which ClickHouse rejects.
attribution as (
    select
        c.request_id,
        argMaxIf(
            ci.gpu_id,
            ci.valid_from_ts,
            ci.valid_from_ts <= c.completed_at and ci.hardware_present = 1
        ) as gpu_id,
        argMaxIf(
            ci.gpu_model_name,
            ci.valid_from_ts,
            ci.valid_from_ts <= c.completed_at and ci.hardware_present = 1
        ) as gpu_model_name,
        argMaxIf(
            ci.gpu_memory_bytes_total,
            ci.valid_from_ts,
            ci.valid_from_ts <= c.completed_at and ci.hardware_present = 1
        ) as gpu_memory_bytes_total,
        argMaxIf(
            ci.canonical_model,
            ci.valid_from_ts,
            ci.valid_from_ts <= c.completed_at and ci.hardware_present = 1
        ) as attributed_model,
        if(
            countIf(ci.valid_from_ts <= c.completed_at and ci.hardware_present = 1) > 0,
            'resolved',
            'unresolved'
        ) as attribution_status
    from completed c
    left join naap.canonical_orch_capability_intervals ci
        on ci.orch_uri_norm = c.orch_url_norm
    group by c.request_id
)

select
    -- identity
    coalesce(c.request_id, r.request_id)     as request_id,
    coalesce(c.org, r.org)                    as org,
    coalesce(r.gateway, '')                   as gateway,

    -- pipeline
    coalesce(r.pipeline, '')                  as pipeline,
    coalesce(r.model_id, '')                  as model_id,

    -- lifecycle timestamps
    r.received_at                             as received_at,
    c.completed_at                            as completed_at,

    -- outcome
    c.success                                 as success,
    c.tries                                   as tries,
    c.duration_ms                             as duration_ms,
    c.orch_url                                as orch_url,
    c.orch_url_norm                           as orch_url_norm,
    c.latency_score                           as latency_score,
    c.price_per_unit                          as price_per_unit,
    c.error_type                              as error_type,
    c.error                                   as error,

    -- LLM-specific (null for non-LLM pipelines)
    l.llm_model                               as llm_model,
    l.prompt_tokens                           as prompt_tokens,
    l.completion_tokens                       as completion_tokens,
    l.total_tokens                            as total_tokens,
    l.tokens_per_second                       as tokens_per_second,
    l.ttft_ms                                 as ttft_ms,
    l.finish_reason                           as finish_reason,
    l.llm_streaming                           as llm_streaming,

    -- GPU hardware attribution (interval active at job completion time)
    a.gpu_id                                  as gpu_id,
    a.gpu_model_name                          as gpu_model_name,
    a.gpu_memory_bytes_total                  as gpu_memory_bytes_total,
    a.attributed_model                        as attributed_model,
    a.attribution_status                      as attribution_status

from completed c
left join received r
    on c.org = r.org
   and c.request_id = r.request_id
left join llm l
    on c.org = l.org
   and c.request_id = l.request_id
left join attribution a
    on c.request_id = a.request_id
