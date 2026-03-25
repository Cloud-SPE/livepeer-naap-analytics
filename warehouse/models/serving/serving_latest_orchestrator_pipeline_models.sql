with expanded as (
    select
        orch_address,
        org,
        snapshot_ts,
        arrayJoin(JSONExtractArrayRaw(raw_capabilities, 'hardware')) as hardware_json
    from {{ ref('capability_snapshots') }}
    where length(JSONExtractArrayRaw(raw_capabilities, 'hardware')) > 0
)
select
    orch_address,
    argMax(org, snapshot_ts) as org,
    JSONExtractString(hardware_json, 'pipeline') as pipeline_id,
    JSONExtractString(hardware_json, 'model_id') as model_id,
    cast(null as Nullable(String)) as gpu_id,
    cast(null as Nullable(String)) as gpu_model_name,
    cast(null as Nullable(UInt64)) as gpu_memory_bytes_total,
    cast(null as Nullable(String)) as runner_version,
    cast(null as Nullable(String)) as cuda_version,
    max(snapshot_ts) as last_seen
from expanded
group by orch_address, pipeline_id, model_id
