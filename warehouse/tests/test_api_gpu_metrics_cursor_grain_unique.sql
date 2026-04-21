select
    window_start,
    org,
    orchestrator_address,
    pipeline_id,
    ifNull(model_id, '') as model_id_key,
    ifNull(gpu_id, '') as gpu_id_key,
    count(*) as row_count
from {{ ref('api_hourly_streaming_gpu_metrics') }}
group by 1, 2, 3, 4, 5, 6
having count(*) > 1
