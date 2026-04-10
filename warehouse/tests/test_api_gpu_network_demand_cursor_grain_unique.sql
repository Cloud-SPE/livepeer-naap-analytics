select
    window_start,
    gateway,
    orchestrator_address,
    ifNull(region, '') as region_key,
    pipeline_id,
    ifNull(model_id, '') as model_id_key,
    ifNull(gpu_id, '') as gpu_id_key,
    gpu_identity_status,
    count(*) as row_count
from {{ ref('api_gpu_network_demand') }}
group by 1, 2, 3, 4, 5, 6, 7, 8
having count(*) > 1
