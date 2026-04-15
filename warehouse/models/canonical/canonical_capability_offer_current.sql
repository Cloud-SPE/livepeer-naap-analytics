with catalog as (
    select *
    from {{ ref('canonical_capability_catalog') }}
),
latest as (
    select
        org,
        orch_address,
        uri as orch_uri,
        last_seen,
        raw_capabilities
    from {{ ref('canonical_latest_orchestrator_state') }}
),
builtin_percap as (
    select
        l.org,
        l.orch_address,
        l.orch_uri,
        l.last_seen,
        c.capability_id,
        c.capability_name,
        c.capability_family,
        c.canonical_pipeline,
        c.capability_name as offered_name,
        nullIf(tupleElement(model_entry, 1), '') as model_id,
        cast(JSONExtractBool(tupleElement(model_entry, 2), 'warm'), 'Nullable(UInt8)') as warm,
        cast(nullIf(JSONExtractInt(tupleElement(model_entry, 2), 'capacity'), 0), 'Nullable(Int32)') as advertised_capacity,
        toUInt8(0) as hardware_present,
        cast(null, 'Nullable(String)') as gpu_id,
        cast(null, 'Nullable(String)') as gpu_model_name,
        cast(null, 'Nullable(UInt64)') as gpu_memory_bytes_total,
        c.supports_stream,
        c.supports_request
    from (
        select
            l.*,
            tupleElement(percap_entry, 1) as capability_key,
            tupleElement(percap_entry, 2) as capability_value,
            arrayJoin(
                if(
                    models_raw in ('', 'null', '{}'),
                    CAST([], 'Array(Tuple(String, String))'),
                    JSONExtractKeysAndValuesRaw(models_raw)
                )
            ) as model_entry
        from (
            select
                l.*,
                arrayJoin(
                    if(
                        percap_raw in ('', 'null', '{}'),
                        CAST([], 'Array(Tuple(String, String))'),
                        JSONExtractKeysAndValuesRaw(percap_raw)
                    )
                ) as percap_entry,
                JSONExtractRaw(tupleElement(percap_entry, 2), 'models') as models_raw
            from (
                select
                    l.*,
                    JSONExtractRaw(raw_capabilities, 'capabilities', 'constraints', 'PerCapability') as percap_raw
                from latest l
                where length(JSONExtractArrayRaw(raw_capabilities, 'hardware')) = 0
            ) l
        ) l
    ) l
    inner join catalog c
        on c.capability_id = toUInt16OrZero(l.capability_key)
    where c.capability_family = 'builtin'
      and tupleElement(model_entry, 1) != ''
),
hardware_rows as (
    select
        l.org,
        l.orch_address,
        l.orch_uri,
        l.last_seen,
        cast(
            ifNull(c.capability_id, toUInt16(37)),
            'Nullable(UInt16)'
        ) as capability_id,
        ifNull(c.capability_name, 'byoc') as capability_name,
        ifNull(c.capability_family, 'byoc') as capability_family,
        cast(c.canonical_pipeline, 'Nullable(String)') as canonical_pipeline,
        nullIf(JSONExtractString(hardware_json, 'pipeline'), '') as offered_name,
        cast(nullIf(JSONExtractString(hardware_json, 'model_id'), ''), 'Nullable(String)') as model_id,
        cast(null, 'Nullable(UInt8)') as warm,
        cast(null, 'Nullable(Int32)') as advertised_capacity,
        toUInt8(1) as hardware_present,
        cast(nullIf(JSONExtractString(gpu_json, 'id'), ''), 'Nullable(String)') as gpu_id,
        cast(nullIf(JSONExtractString(gpu_json, 'name'), ''), 'Nullable(String)') as gpu_model_name,
        cast(nullIf(JSONExtractUInt(gpu_json, 'memory_total'), 0), 'Nullable(UInt64)') as gpu_memory_bytes_total,
        toUInt8(ifNull(c.supports_stream, 1)) as supports_stream,
        toUInt8(ifNull(c.supports_request, 1)) as supports_request
    from (
        select
            l.*,
            hardware_json,
            if(
                gpu_info_raw in ('', 'null', '{}', '[]'),
                CAST(['{}'], 'Array(String)'),
                if(startsWith(gpu_info_raw, '['), JSONExtractArrayRaw(gpu_info_raw), tupleElement(JSONExtractKeysAndValuesRaw(gpu_info_raw), 2))
            ) as gpu_entries
        from (
            select
                l.*,
                arrayJoin(JSONExtractArrayRaw(raw_capabilities, 'hardware')) as hardware_json,
                JSONExtractRaw(hardware_json, 'gpu_info') as gpu_info_raw
            from latest l
            where length(JSONExtractArrayRaw(raw_capabilities, 'hardware')) > 0
        ) l
    ) l
    array join gpu_entries as gpu_json
    left join catalog c
        on lower(JSONExtractString(hardware_json, 'pipeline')) = lower(c.canonical_pipeline)
)
select *
from builtin_percap
union all
select *
from hardware_rows
where offered_name is not null
