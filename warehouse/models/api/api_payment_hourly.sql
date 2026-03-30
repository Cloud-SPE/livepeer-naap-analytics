select
    toStartOfHour(p.event_ts) as hour,
    p.org,
    coalesce(fs.canonical_pipeline, p.pipeline_hint) as pipeline,
    p.recipient_address as orch_address,
    sum(p.face_value_wei) as total_wei,
    count() as event_count,
    avg(p.price_wei_per_pixel) as avg_price_wei_per_pixel
from {{ ref('canonical_payment_links') }} p
left join {{ ref('canonical_session_current') }} fs on p.canonical_session_key = fs.canonical_session_key
group by hour, p.org, pipeline, orch_address
