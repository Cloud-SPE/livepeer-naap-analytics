with request_linked as (
    select
        p.event_id as event_id,
        p.event_ts as event_ts,
        p.org as org,
        p.gateway as gateway,
        p.session_id as session_id,
        p.request_id as request_id,
        p.manifest_id as manifest_id,
        p.pipeline_hint as pipeline_hint,
        p.sender_address as sender_address,
        p.recipient_address as recipient_address,
        p.orchestrator_url as orchestrator_url,
        p.face_value_wei as face_value_wei,
        p.price_wei_per_pixel as price_wei_per_pixel,
        p.win_prob as win_prob,
        p.num_tickets as num_tickets,
        fs.canonical_session_key as canonical_session_key,
        'request_id' as link_method,
        'resolved' as link_status
    from {{ ref('stg_payments') }} p
    inner join {{ ref('canonical_session_current') }} fs
        on p.org = fs.org
       and p.request_id != ''
       and p.request_id = fs.request_id
),
session_linked as (
    select
        p.event_id as event_id,
        any(p.event_ts) as event_ts,
        p.org as org,
        any(p.gateway) as gateway,
        p.session_id as session_id,
        any(p.request_id) as request_id,
        any(p.manifest_id) as manifest_id,
        any(p.pipeline_hint) as pipeline_hint,
        any(p.sender_address) as sender_address,
        any(p.recipient_address) as recipient_address,
        any(p.orchestrator_url) as orchestrator_url,
        any(p.face_value_wei) as face_value_wei,
        any(p.price_wei_per_pixel) as price_wei_per_pixel,
        any(p.win_prob) as win_prob,
        any(p.num_tickets) as num_tickets,
        any(fs.canonical_session_key) as canonical_session_key,
        'session_id' as link_method,
        'resolved' as link_status
    from {{ ref('stg_payments') }} p
    inner join naap.typed_payments anchor
        on p.org = anchor.org
       and p.request_id = ''
       and p.session_id != ''
       and p.session_id = anchor.session_id
       and anchor.request_id != ''
    inner join {{ ref('canonical_session_current') }} fs
        on anchor.org = fs.org
       and anchor.request_id = fs.request_id
    where p.event_id not in (select event_id from request_linked)
    group by p.event_id, p.org, p.session_id
),
unresolved as (
    select
        p.event_id as event_id,
        p.event_ts as event_ts,
        p.org as org,
        p.gateway as gateway,
        p.session_id as session_id,
        p.request_id as request_id,
        p.manifest_id as manifest_id,
        p.pipeline_hint as pipeline_hint,
        p.sender_address as sender_address,
        p.recipient_address as recipient_address,
        p.orchestrator_url as orchestrator_url,
        p.face_value_wei as face_value_wei,
        p.price_wei_per_pixel as price_wei_per_pixel,
        p.win_prob as win_prob,
        p.num_tickets as num_tickets,
        cast(null as Nullable(String)) as canonical_session_key,
        'unlinked' as link_method,
        'unresolved' as link_status
    from {{ ref('stg_payments') }} p
    where p.event_id not in (select event_id from request_linked)
      and p.event_id not in (select event_id from session_linked)
)
select * from request_linked
union all
select * from session_linked
union all
select * from unresolved
