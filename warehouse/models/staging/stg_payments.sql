select
    event_id,
    event_ts,
    org,
    gateway,
    JSONExtractString(data, 'sessionID') as session_id,
    JSONExtractString(data, 'requestID') as request_id,
    JSONExtractString(data, 'manifestID') as manifest_id,
    replaceRegexpOne(JSONExtractString(data, 'manifestID'), '^[0-9]+_', '') as pipeline_hint,
    lower(JSONExtractString(data, 'sender')) as sender_address,
    lower(JSONExtractString(data, 'recipient')) as recipient_address,
    JSONExtractString(data, 'orchestrator') as orchestrator_url,
    toUInt64OrDefault(trimRight(replaceAll(JSONExtractString(data, 'faceValue'), ' WEI', ''))) as face_value_wei,
    toFloat64OrDefault(replaceRegexpOne(JSONExtractString(data, 'price'), ' wei/pixel$', '')) as price_wei_per_pixel,
    toFloat64OrDefault(JSONExtractString(data, 'winProb')) as win_prob,
    toUInt64OrDefault(JSONExtractString(data, 'numTickets')) as num_tickets,
    data
from naap.accepted_raw_events final
where event_type = 'create_new_payment'
  and event_id != ''
