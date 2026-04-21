select
    window_start,
    org,
    gateway,
    execution_mode,
    capability_family,
    capability_name,
    ifNull(toString(capability_id), '') as capability_id_key,
    ifNull(canonical_pipeline, '') as canonical_pipeline_key,
    ifNull(canonical_model, '') as canonical_model_key,
    orchestrator_address,
    orchestrator_uri,
    count(*) as row_count
from {{ ref('api_hourly_request_demand') }}
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
having count(*) > 1
