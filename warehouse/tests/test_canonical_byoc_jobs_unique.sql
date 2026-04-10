select
    org,
    request_id,
    count() as row_count
from {{ ref('canonical_byoc_jobs') }}
group by org, request_id
having count() > 1
