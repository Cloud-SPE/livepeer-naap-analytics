select
    last_seen,
    org,
    orch_address,
    count(*) as row_count
from {{ ref('api_current_orchestrator') }}
group by 1, 2, 3
having count(*) > 1
