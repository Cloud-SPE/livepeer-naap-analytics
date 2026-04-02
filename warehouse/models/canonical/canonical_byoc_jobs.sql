-- One row per BYOC job request.
-- Capabilities are dynamic (openai-chat-completions, openai-image-generation, etc.)
-- and are stored verbatim — never mapped to hardcoded names.
--
-- Model attribution comes from normalized_worker_lifecycle (most recent worker
-- registration for this capability + orch_address, via argMax).
--
-- GPU hardware attribution: BYOC endpoints share the same orch_address as
-- standard Livepeer endpoints. canonical_orch_capability_intervals (resolver-
-- maintained) has hardware data keyed by orch_address. We JOIN on lowercase
-- address (not URI) with hardware_present = 1 to infer GPU for BYOC jobs.
--
-- All JOINs use simple equality conditions. Range conditions (e.g. registered_at
-- <= completed_at) are resolved inside CTEs using argMax to keep a single row
-- per key, avoiding INVALID_JOIN_ON_EXPRESSION errors in ClickHouse 24.x.

with submitted as (
    select
        request_id,
        org,
        gateway,
        capability,
        orch_address,
        orch_url,
        orch_url_norm,
        event_ts as submitted_at
    from {{ ref('stg_byoc_jobs') }}
    where subtype = 'job_gateway_submitted'
      and request_id != ''
),

completed as (
    select
        request_id,
        org,
        capability,
        success,
        duration_ms,
        http_status,
        orch_address,
        orch_url,
        orch_url_norm,
        worker_url,
        charged_compute,
        error,
        event_ts as completed_at
    from {{ ref('stg_byoc_jobs') }}
    where subtype = 'job_gateway_completed'
      and request_id != ''
),

-- Resolve submitted + completed into a single row with stable keys before
-- joining further CTEs — avoids coalesce(left.col, right.col) in subsequent
-- JOIN ON expressions, which ClickHouse rejects.
job_base as (
    select
        coalesce(c.request_id, s.request_id)        as request_id,
        coalesce(c.org, s.org)                       as org,
        coalesce(s.gateway, '')                      as gateway,
        coalesce(c.capability, s.capability)         as capability,
        coalesce(c.orch_address, s.orch_address)     as orch_address,
        lower(coalesce(c.orch_address, s.orch_address)) as orch_address_lower,
        coalesce(c.orch_url, s.orch_url)             as orch_url,
        coalesce(c.orch_url_norm, s.orch_url_norm)   as orch_url_norm,
        s.submitted_at                               as submitted_at,
        c.completed_at                               as completed_at,
        c.success                                    as success,
        c.duration_ms                                as duration_ms,
        c.http_status                                as http_status,
        c.worker_url                                 as worker_url,
        c.charged_compute                            as charged_compute,
        c.error                                      as error
    from completed c
    left join submitted s
        on c.org = s.org
       and c.request_id = s.request_id
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

-- Hardware inference: latest GPU data per lowercase orch_address.
-- BYOC endpoints share orch_address with standard-reporting endpoints whose
-- GPU data is already present in canonical_orch_capability_intervals.
capability_intervals as (
    select
        lower(orch_address)                          as orch_address_lower,
        argMax(gpu_id, valid_from_ts)                as gpu_id,
        argMax(gpu_model_name, valid_from_ts)         as gpu_model_name,
        argMax(gpu_memory_bytes_total, valid_from_ts) as gpu_memory_bytes_total
    from naap.canonical_orch_capability_intervals
    where hardware_present = 1
    group by lower(orch_address)
)

select
    -- identity
    jb.request_id                             as request_id,
    jb.org                                    as org,
    jb.gateway                                as gateway,

    -- capability (stored verbatim — dynamic)
    jb.capability                             as capability,

    -- lifecycle timestamps
    jb.submitted_at                           as submitted_at,
    jb.completed_at                           as completed_at,

    -- outcome
    jb.success                                as success,
    jb.duration_ms                            as duration_ms,
    jb.http_status                            as http_status,
    jb.orch_address                           as orch_address,
    jb.orch_url                               as orch_url,
    jb.orch_url_norm                          as orch_url_norm,
    jb.worker_url                             as worker_url,
    jb.charged_compute                        as charged_compute,
    jb.error                                  as error,

    -- model from worker_lifecycle (most recent registration for this orch+capability)
    w.model                                   as model,
    w.price_per_unit                          as price_per_unit,

    -- GPU hardware inference via shared orch_address
    ci.gpu_id                                 as gpu_id,
    ci.gpu_model_name                         as gpu_model_name,
    ci.gpu_memory_bytes_total                 as gpu_memory_bytes_total

from job_base jb
left join worker w
    on w.capability = jb.capability
   and w.orch_address = jb.orch_address
left join capability_intervals ci
    on ci.orch_address_lower = jb.orch_address_lower
