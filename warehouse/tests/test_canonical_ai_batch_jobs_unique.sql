select
    org,
    request_id,
    count() as row_count
from {{ ref('canonical_ai_batch_jobs') }}
group by org, request_id
having count() > 1
