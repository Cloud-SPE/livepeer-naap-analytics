with request_anchored as (
    select
        p.event_id,
        p.event_ts,
        p.org,
        p.gateway,
        p.session_id,
        p.request_id,
        p.manifest_id,
        p.pipeline_hint,
        p.sender_address,
        p.recipient_address,
        p.orchestrator_url,
        p.face_value_wei,
        p.price_wei_per_pixel,
        p.win_prob,
        p.num_tickets,
        fs.canonical_session_key,
        'request_id' as link_method,
        'resolved' as link_status
    from {{ ref('stg_payments') }} p
    inner join {{ ref('fact_workflow_sessions') }} fs on p.request_id != '' and p.org = fs.org and p.request_id = fs.request_id
),
session_anchor as (
    select
        org,
        session_id,
        any(canonical_session_key) as canonical_session_key
    from request_anchored
    where session_id != ''
    group by org, session_id
),
session_anchored as (
    select
        p.event_id,
        p.event_ts,
        p.org,
        p.gateway,
        p.session_id,
        p.request_id,
        p.manifest_id,
        p.pipeline_hint,
        p.sender_address,
        p.recipient_address,
        p.orchestrator_url,
        p.face_value_wei,
        p.price_wei_per_pixel,
        p.win_prob,
        p.num_tickets,
        sa.canonical_session_key,
        'session_id' as link_method,
        'resolved' as link_status
    from {{ ref('stg_payments') }} p
    inner join session_anchor sa on p.request_id = '' and p.session_id != '' and p.org = sa.org and p.session_id = sa.session_id
    where p.event_id not in (select event_id from request_anchored)
),
unresolved as (
    select
        p.event_id,
        p.event_ts,
        p.org,
        p.gateway,
        p.session_id,
        p.request_id,
        p.manifest_id,
        p.pipeline_hint,
        p.sender_address,
        p.recipient_address,
        p.orchestrator_url,
        p.face_value_wei,
        p.price_wei_per_pixel,
        p.win_prob,
        p.num_tickets,
        cast(null as Nullable(String)) as canonical_session_key,
        'unlinked' as link_method,
        'unresolved' as link_status
    from {{ ref('stg_payments') }} p
    where p.event_id not in (select event_id from request_anchored)
      and p.event_id not in (select event_id from session_anchored)
)
select * from request_anchored
union all
select * from session_anchored
union all
select * from unresolved
