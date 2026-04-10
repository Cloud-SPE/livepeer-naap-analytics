select
    window_start,
    orchestrator_address,
    pipeline_id,
    ifNull(model_id, '') as model_id_key,
    ifNull(gpu_id, '') as gpu_id_key,
    ifNull(region, '') as region_key,
    count(*) as row_count
from {{ ref('api_sla_compliance') }}
group by 1, 2, 3, 4, 5, 6
having count(*) > 1
