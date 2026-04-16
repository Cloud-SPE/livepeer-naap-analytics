select
    org,
    orch_address,
    orch_uri_norm,
    capability_version_id,
    count(*) as row_count
from {{ ref('api_observed_orchestrator') }}
group by 1, 2, 3, 4
having count(*) > 1
