select
    i.window_start,
    i.org,
    i.orchestrator_address,
    i.pipeline_id,
    ifNull(i.model_id, '') as model_id_key,
    ifNull(i.gpu_id, '') as gpu_id_key
from {{ ref('api_base_sla_quality_inputs_by_org') }} i
left join {{ ref('api_base_sla_quality_benchmarks_by_org') }} b
    on i.window_start = b.window_start
   and i.org = b.org
   and i.orchestrator_address = b.orchestrator_address
   and i.pipeline_id = b.pipeline_id
   and ifNull(i.model_id, '') = ifNull(b.model_id, '')
   and ifNull(i.gpu_id, '') = ifNull(b.gpu_id, '')
where b.window_start is null
