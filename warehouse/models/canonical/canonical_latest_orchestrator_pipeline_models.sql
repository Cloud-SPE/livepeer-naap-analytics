select
    org,
    orch_address,
    pipeline_id,
    model_id,
    cast(nullIf(argMaxIfMerge(gpu_id_state), ''), 'Nullable(String)') as gpu_id,
    nullIf(argMaxIfMerge(gpu_model_name_state), '') as gpu_model_name,
    nullIf(argMaxIfMerge(gpu_memory_bytes_total_state), toUInt64(0)) as gpu_memory_bytes_total,
    nullIf(argMaxIfMerge(runner_version_state), '') as runner_version,
    nullIf(argMaxIfMerge(cuda_version_state), '') as cuda_version,
    maxMerge(last_seen_state) as last_seen
from naap.canonical_latest_orchestrator_pipeline_inventory_agg
group by org, orch_address, pipeline_id, model_id
