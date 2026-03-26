{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by=['orch_address', 'orch_uri', 'pipeline_id', 'model_id', "ifNull(gpu_id, '')"]
) }}

-- One row per (orch, pipeline, model, gpu).
-- gpu_info in raw_capabilities is an object keyed by GPU slot index ("0","1",...).
-- We expand each slot via arrayJoin so multi-GPU orches produce one row per GPU.
-- Rows without gpu_info (hardware entry lacks the key) produce a single NULL-gpu row
-- so the pipeline/model combination is still tracked.
with expanded_hardware as (
    select
        orch_address,
        orch_uri,
        org,
        snapshot_ts,
        arrayJoin(JSONExtractArrayRaw(raw_capabilities, 'hardware')) as hw
    from {{ ref('capability_snapshots') }}
    where length(JSONExtractArrayRaw(raw_capabilities, 'hardware')) > 0
),
with_gpu_keys as (
    select
        orch_address,
        orch_uri,
        org,
        snapshot_ts,
        hw,
        JSONExtractString(hw, 'pipeline') as pipeline_id,
        JSONExtractString(hw, 'model_id')  as model_id,
        if(
            JSONHas(hw, 'gpu_info'),
            JSONExtractKeys(JSONExtractRaw(hw, 'gpu_info')),
            []
        ) as gpu_keys
    from expanded_hardware
),
expanded_gpu as (
    -- Rows with gpu_info: one row per GPU slot
    select
        orch_address, orch_uri, org, snapshot_ts,
        pipeline_id, model_id,
        arrayJoin(gpu_keys) as gpu_slot,
        hw
    from with_gpu_keys
    where length(gpu_keys) > 0

    union all

    -- Rows without gpu_info: preserve the pipeline/model with null gpu
    select
        orch_address, orch_uri, org, snapshot_ts,
        pipeline_id, model_id,
        '' as gpu_slot,
        hw
    from with_gpu_keys
    where length(gpu_keys) = 0
)
select
    orch_address,
    orch_uri,
    argMax(org, snapshot_ts) as org,
    pipeline_id,
    model_id,
    nullIf(if(
        gpu_slot != '',
        JSONExtractString(hw, 'gpu_info', gpu_slot, 'id'),
        ''
    ), '') as gpu_id,
    nullIf(if(
        gpu_slot != '',
        JSONExtractString(hw, 'gpu_info', gpu_slot, 'name'),
        ''
    ), '') as gpu_model_name,
    if(
        gpu_slot != '',
        nullIf(JSONExtractUInt(hw, 'gpu_info', gpu_slot, 'memory_total'), 0),
        null
    ) as gpu_memory_bytes_total,
    cast(null as Nullable(String)) as runner_version,
    cast(null as Nullable(String)) as cuda_version,
    max(snapshot_ts) as last_seen
from expanded_gpu
group by orch_address, orch_uri, pipeline_id, model_id, gpu_slot, hw
