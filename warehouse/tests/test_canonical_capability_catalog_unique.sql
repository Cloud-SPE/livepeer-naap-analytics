select
    capability_id,
    count(*) as row_count
from {{ ref('canonical_capability_catalog') }}
group by capability_id
having count(*) > 1
