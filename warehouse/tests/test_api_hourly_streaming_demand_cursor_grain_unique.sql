select
    window_start,
    org,
    gateway,
    pipeline_id,
    ifNull(model_id, '') as model_id_key,
    count(*) as row_count
from {{ ref('api_hourly_streaming_demand') }}
group by 1, 2, 3, 4, 5
having count(*) > 1
