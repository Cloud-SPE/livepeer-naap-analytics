select
    last_seen,
    orch_address,
    count(*) as row_count
from {{ ref('api_latest_orchestrator_state') }}
group by 1, 2
having count(*) > 1
