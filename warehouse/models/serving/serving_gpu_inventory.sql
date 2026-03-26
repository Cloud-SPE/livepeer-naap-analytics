{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by=['orch_address', 'orch_uri', 'pipeline', 'model_id']
) }}

select
    orch_address,
    orch_uri,
    org,
    pipeline_id as pipeline,
    model_id,
    ifNull(gpu_id, '') as gpu_id,
    gpu_model_name,
    ifNull(gpu_memory_bytes_total, toUInt64(0)) as memory_bytes,
    last_seen
from {{ ref('serving_latest_orchestrator_pipeline_models') }}
