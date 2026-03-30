select
    concat(event_id, '#', lower(JSONExtractString(candidate_json, 'address')), '#', toString(idx)) as row_id,
    event_id,
    event_ts,
    org,
    gateway,
    lower(JSONExtractString(candidate_json, 'address')) as orch_address,
    JSONExtractString(candidate_json, 'url') as orch_url,
    toUInt64OrDefault(JSONExtractString(candidate_json, 'latency_ms')) as latency_ms,
    candidate_json as data
from (
    select
        event_id,
        event_ts,
        org,
        gateway,
        arrayJoin(arrayEnumerate(JSONExtractArrayRaw(data))) as idx,
        arrayJoin(JSONExtractArrayRaw(data)) as candidate_json
    from naap.accepted_raw_events final
    where event_type = 'discovery_results'
      and data not in ('', '[]', 'null')
)
where orch_address != ''
