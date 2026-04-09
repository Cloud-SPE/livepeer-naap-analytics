-- One row per completed BYOC job.
-- Capabilities are dynamic (openai-chat-completions, openai-image-generation, etc.)
-- and are stored verbatim — never mapped to hardcoded names.
--
-- Note: BYOC events do not carry a meaningful request_id in the current event
-- schema (the field is always empty). Each job is therefore identified by its
-- event_id (the Kafka message UUID). Only job_gateway_completed events are used
-- as the canonical record; submitted events cannot be reliably joined.
--
-- Model attribution comes from normalized_worker_lifecycle (most recent worker
-- registration for this capability + orch_address, via argMax).
--
-- GPU hardware attribution: BYOC endpoints share the same orch_address as
-- standard Livepeer endpoints. Time-valid argMaxIf picks the interval active at
-- job completion time. Three-level attribution_status:
--   resolved  — URI match found (orch_uri_norm = orch_url_norm) at job time
--   inferred  — address-only match found at job time (URI missing or differs)
--   unresolved — no hardware interval found

with completed as (
    select
        event_id,
        org,
        gateway,
        capability,
        success,
        duration_ms,
        http_status,
        orch_address,
        lower(orch_address)  as orch_address_lower,
        orch_url,
        orch_url_norm,
        worker_url,
        charged_compute,
        error,
        event_ts             as completed_at
    from {{ ref('stg_byoc_jobs') }}
    -- event_subtype = JSONExtractString(data, 'type').
    -- For job_gateway completion events, data.type is 'job_gateway_completed'
    -- (the full composite value, NOT the short form 'completed').
    -- source_event_type discriminates job_gateway from job_orchestrator rows.
    where subtype = 'job_gateway_completed'
      and source_event_type = 'job_gateway'
      and capability != ''
),

-- Most recent worker for each (capability, orch_address).
-- argMax avoids a range JOIN condition (registered_at <= completed_at).
worker as (
    select
        capability,
        orch_address,
        argMax(worker_url, event_ts)     as worker_url,
        argMax(model, event_ts)          as model,
        argMax(price_per_unit, event_ts) as price_per_unit
    from {{ ref('stg_worker_lifecycle') }}
    where capability != ''
      and orch_address != ''
    group by capability, orch_address
),

-- Time-valid hardware attribution joined on orch_address (lowercase).
-- BYOC endpoints share orch_address with standard-reporting endpoints whose
-- GPU data is in canonical_orch_capability_intervals.
-- Three-level status: resolved (URI match), inferred (address-only), unresolved.
attribution as (
    select
        c.event_id,
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
        multiIf(
            countIf(
                ci.orch_uri_norm = c.orch_url_norm
                and c.orch_url_norm != ''
                and ci.valid_from_ts <= c.completed_at
                and ci.hardware_present = 1
            ) > 0, 'resolved',
            countIf(
                ci.valid_from_ts <= c.completed_at
                and ci.hardware_present = 1
            ) > 0, 'inferred',
            'unresolved'
        ) as attribution_status
    from completed c
    left join naap.canonical_orch_capability_intervals ci
        on lower(ci.orch_address) = c.orch_address_lower
    group by c.event_id
)

select
    -- identity (event_id is the stable key since request_id is always empty)
    c.event_id                                as request_id,
    c.org                                     as org,
    c.gateway                                 as gateway,

    -- capability (stored verbatim — dynamic)
    c.capability                              as capability,

    -- lifecycle timestamps (no submitted event available)
    cast(null as Nullable(DateTime64(3,'UTC'))) as submitted_at,
    c.completed_at                            as completed_at,

    -- outcome
    c.success                                 as success,
    c.duration_ms                             as duration_ms,
    c.http_status                             as http_status,
    c.orch_address                            as orch_address,
    c.orch_url                                as orch_url,
    c.orch_url_norm                           as orch_url_norm,
    c.worker_url                              as worker_url,
    c.charged_compute                         as charged_compute,
    c.error                                   as error,

    -- model from worker_lifecycle (most recent registration for this orch+capability)
    w.model                                   as model,
    w.price_per_unit                          as price_per_unit,

    -- GPU hardware inference via shared orch_address (time-valid)
    a.gpu_id                                  as gpu_id,
    a.gpu_model_name                          as gpu_model_name,
    a.gpu_memory_bytes_total                  as gpu_memory_bytes_total,
    a.attribution_status                      as attribution_status

from completed c
left join worker w
    on w.capability  = c.capability
   and w.orch_address = c.orch_address
left join attribution a
    on a.event_id = c.event_id
