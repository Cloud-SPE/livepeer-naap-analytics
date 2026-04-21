-- Uniqueness test on the raw store table that backs api_hourly_streaming_sla.
-- After Phase 1 of serving-layer-v2, the resolver writes one row per
-- (window_start, orchestrator_address, pipeline_id, model_id, gpu_id,
-- region, refresh_run_id) tuple; duplicates within a single refresh_run_id
-- indicate a resolver bug (the `api_base_sla_compliance_scored_by_org`
-- SELECT that feeds the store should group cleanly).
--
-- A row emerging here means the store has a key collision — a later
-- `refresh_run_id` resolving via argMax at read time masks it from the
-- API view, but the store itself is drifting.

select
    window_start,
    ifNull(org, '') as org_key,
    orchestrator_address,
    pipeline_id,
    ifNull(model_id, '') as model_id_key,
    ifNull(gpu_id, '') as gpu_id_key,
    ifNull(region, '') as region_key,
    refresh_run_id,
    count(*) as row_count
from naap.api_hourly_streaming_sla_store
group by 1, 2, 3, 4, 5, 6, 7, 8
having count(*) > 1
