-- Phase 4: unified capability spine. Reads the resolver-written
-- api_current_capability_store and picks the latest refresh slice per
-- (org, orch_address, capability_id, canonical_pipeline, model_id, gpu_id)
-- via argMax so callers see idempotent output across partial re-runs.
-- Replaces api_observed_capability_offer for /v1/requests/*,
-- /v1/streaming/*, and /v1/dashboard/pipeline-catalog.

{{ config(materialized='view') }}

with latest_slices as (
    select
        org,
        orch_address,
        capability_id,
        canonical_pipeline,
        model_id,
        gpu_id,
        argMax(refresh_run_id, (refreshed_at, refresh_run_id)) as refresh_run_id
    from naap.api_current_capability_store
    group by org, orch_address, capability_id, canonical_pipeline, model_id, gpu_id
)
select
    s.org,
    s.orch_address,
    s.orchestrator_uri,
    s.orchestrator_name,
    s.capability_id,
    s.capability_name,
    s.capability_family,
    s.canonical_pipeline,
    s.model_id,
    s.gpu_id,
    s.advertised_capacity,
    s.hardware_present,
    s.supports_request,
    s.supports_stream,
    s.price_per_unit,
    s.price_currency,
    s.last_seen
from naap.api_current_capability_store as s
inner join latest_slices as l
    on  s.org = l.org
    and s.orch_address = l.orch_address
    and s.capability_id = l.capability_id
    and s.canonical_pipeline = l.canonical_pipeline
    and s.model_id = l.model_id
    and s.gpu_id = l.gpu_id
    and s.refresh_run_id = l.refresh_run_id
