select
    window_start,
    org,
    orchestrator_address,
    pipeline_id,
    ifNull(model_id, '') as model_id_key,
    ifNull(gpu_id, '') as gpu_id_key,
    count(*) as row_count
from {{ ref('api_base_sla_quality_benchmarks_by_org') }}
group by 1, 2, 3, 4, 5, 6
having count(*) > 1
