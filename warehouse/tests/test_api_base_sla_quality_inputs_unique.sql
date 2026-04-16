select
    window_start,
    orchestrator_address,
    pipeline_id,
    ifNull(model_id, '') as model_id_key,
    ifNull(gpu_id, '') as gpu_id_key,
    count(*) as row_count
from {{ ref('api_base_sla_quality_inputs') }}
group by 1, 2, 3, 4, 5
having count(*) > 1
