-- Uniqueness test on canonical_sla_benchmark_daily_store. After Phase 2
-- the resolver writes one row per (pipeline_id, ifNull(model_id, ''),
-- cohort_date, refresh_run_id) tuple; duplicates at that grain indicate
-- a bug in insertSLABenchmarkDaily's GROUP BY or a misconfigured
-- ReplacingMergeTree key.
--
-- The store's sort-key grain deliberately includes refresh_run_id so
-- successive resolver runs are additive (readers pick latest via
-- argMax); within a single run the upstream GROUP BY must collapse the
-- cohort cleanly.

select
    pipeline_id,
    ifNull(model_id, '') as model_id_key,
    cohort_date,
    refresh_run_id,
    count(*) as row_count
from naap.canonical_sla_benchmark_daily_store
group by 1, 2, 3, 4
having count(*) > 1
