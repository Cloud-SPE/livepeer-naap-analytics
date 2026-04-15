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
flags as (
    select
        l.org,
        l.orch_address,
        l.orch_uri,
        l.last_seen,
        toUInt16OrZero(tupleElement(cap_entry, 1)) as capability_id,
        toInt32OrNull(tupleElement(cap_entry, 2)) as supported_units
    from (
        select
            l.*,
            arrayJoin(
                if(
                    capacities_raw in ('', 'null', '{}'),
                    CAST([], 'Array(Tuple(String, String))'),
                    JSONExtractKeysAndValuesRaw(capacities_raw)
                )
            ) as cap_entry
        from (
            select
                l.*,
                JSONExtractRaw(raw_capabilities, 'capabilities', 'capacities') as capacities_raw
            from latest l
        ) l
    ) l
)
select
    f.org,
    f.orch_address,
    f.orch_uri,
    f.last_seen,
    c.capability_id,
    c.capability_name,
    c.capability_family,
    c.canonical_pipeline,
    f.supported_units
from flags f
inner join catalog c
    on c.capability_id = f.capability_id
where f.supported_units is not null
