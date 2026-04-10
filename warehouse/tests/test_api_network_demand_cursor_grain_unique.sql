select
    window_start,
    gateway,
    ifNull(region, '') as region_key,
    pipeline_id,
    ifNull(model_id, '') as model_id_key,
    count(*) as row_count
from {{ ref('api_network_demand') }}
group by 1, 2, 3, 4, 5
having count(*) > 1
