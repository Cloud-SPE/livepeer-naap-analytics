select
    orch_address,
    count(*) as row_count
from {{ ref('api_orchestrator_identity') }}
group by 1
having count(*) > 1
