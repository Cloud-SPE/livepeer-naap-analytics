-- Uniqueness test on api_current_orchestrator_store. Phase 6.3 writes one
-- row per (orch_address, refresh_run_id); duplicates at that grain
-- indicate a bug in insertCurrentOrchestratorState's GROUP BY.

select
    orch_address,
    refresh_run_id,
    count(*) as row_count
from naap.api_current_orchestrator_store
group by 1, 2
having count(*) > 1
