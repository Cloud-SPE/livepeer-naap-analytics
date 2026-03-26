select
    toStartOfHour(event_ts) as hour,
    org,
    gateway,
    sender_address,
    sum(face_value_wei) as total_wei,
    count() as event_count,
    uniqExact(recipient_address) as unique_orchs
from {{ ref('fact_workflow_payment_links') }}
group by hour, org, gateway, sender_address
