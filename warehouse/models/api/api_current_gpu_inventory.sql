-- Perf-pass: current 24h GPU inventory snapshot. Latest-slice reader over
-- api_current_gpu_inventory_store written by the resolver on a 5-min throttle.
-- Supply Inventory dashboard's GPU panels read this view instead of
-- full-scanning canonical_capability_offer_inventory on every refresh.

{{ config(materialized='view') }}

with latest_slices as (
    select orch_address, gpu_id, argMax(refresh_run_id, refreshed_at) as refresh_run_id
    from naap.api_current_gpu_inventory_store
    group by orch_address, gpu_id
)
select
    s.org,
    s.orch_address,
    s.gpu_id,
    s.gpu_model_name,
    s.gpu_memory_bytes_total,
    s.canonical_pipeline,
    s.model_id,
    s.last_seen
from naap.api_current_gpu_inventory_store as s
inner join latest_slices as l
    on  s.orch_address = l.orch_address
    and s.gpu_id = l.gpu_id
    and s.refresh_run_id = l.refresh_run_id
