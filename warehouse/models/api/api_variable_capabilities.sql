-- Perf-pass: tiny template-variable source for the $capability dropdown.
-- Reads from the unified capability spine (api_current_capability_store,
-- written by the resolver on a 5-min throttle) so the dropdown list is a
-- primary-key scan over ~hundreds of rows instead of a distinct scan of
-- canonical_capability_offer_inventory_store (~1 GiB).

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
select distinct s.capability_name as capability
from naap.api_current_capability_store as s
inner join latest_slices as l
    on  s.org = l.org
    and s.orch_address = l.orch_address
    and s.capability_id = l.capability_id
    and s.canonical_pipeline = l.canonical_pipeline
    and s.model_id = l.model_id
    and s.gpu_id = l.gpu_id
    and s.refresh_run_id = l.refresh_run_id
where s.capability_name != ''
  and s.capability_family = 'byoc'
order by capability
