-- Uniqueness test on api_hourly_request_demand_store. After Phase 6.1 the
-- resolver writes one row per (org, window_start, gateway, execution_mode,
-- capability_family, capability_name, ifNull(canonical_model, ''),
-- orchestrator_address, orchestrator_uri, refresh_run_id) tuple; duplicates
-- at that grain indicate a bug in insertHourlyRequestDemand's GROUP BY or
-- a misconfigured ORDER BY on the store.
--
-- refresh_run_id is part of the grain on purpose so successive resolver runs
-- are additive; the api_hourly_request_demand view picks the latest slice
-- via argMax(refresh_run_id, refreshed_at).

select
    org,
    window_start,
    gateway,
    execution_mode,
    capability_family,
    capability_name,
    ifNull(canonical_model, '') as model_key,
    orchestrator_address,
    orchestrator_uri,
    refresh_run_id,
    count(*) as row_count
from naap.api_hourly_request_demand_store
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
having count(*) > 1
