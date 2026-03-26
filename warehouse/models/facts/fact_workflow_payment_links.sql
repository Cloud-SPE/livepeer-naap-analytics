{{ config(
    engine='MergeTree()',
    order_by=['org', 'event_ts'],
    partition_by='toYYYYMM(event_ts)'
) }}

-- ClickHouse 24.3 new analyzer: CREATE TABLE ... ORDER BY (org, event_ts) AS SELECT
-- fails with UNKNOWN_IDENTIFIER when a joined table contains a column named 'org'
-- (the name clashes with the ORDER BY reference). Fix: rename ALL columns from any
-- fact_workflow_sessions sub-select to unique prefixes so 'org' only appears once
-- (from stg_payments), making it unambiguous for ORDER BY resolution.
--
-- Session linking logic (two hops):
--   1. Direct: payment.request_id → fact_workflow_sessions.request_id
--   2. Via session: payment.session_id → (org, session_id) map derived from step-1
--      payments (different payments sharing the same session carry the same session_id)
with
-- Build (org, session_id) → canonical_session_key from payments that matched via request_id.
-- All fact_workflow_sessions columns renamed (fsc_*) to avoid CREATE TABLE ORDER BY ambiguity.
session_id_map as (
    select
        pay.org          as sim_org,
        pay.session_id   as sim_sess,
        any(fs.fsc_csk)  as sim_csk
    from {{ ref('stg_payments') }} pay
    inner join (
        select
            org          as fsc_org,
            request_id   as fsc_req,
            canonical_session_key as fsc_csk
        from {{ ref('fact_workflow_sessions') }}
        where request_id != ''
    ) fs on pay.request_id != '' and pay.org = fs.fsc_org and pay.request_id = fs.fsc_req
    where pay.session_id != ''
    group by pay.org, pay.session_id
)
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
    -- request_id match wins; fall back to session_id two-hop match
    coalesce(rm.rf_csk, sm.sim_csk) as canonical_session_key,
    multiIf(
        rm.rf_csk is not null, 'request_id',
        sm.sim_csk is not null, 'session_id',
        'unlinked'
    ) as link_method,
    if(coalesce(rm.rf_csk, sm.sim_csk) is not null, 'resolved', 'unresolved') as link_status
from {{ ref('stg_payments') }} p
-- Request-id match: all fact_workflow_sessions columns renamed rf_* for ORDER BY safety.
-- any() deduplicates in case multiple sessions share the same request_id.
left join (
    select
        org          as rf_org,
        request_id   as rf_req,
        any(canonical_session_key) as rf_csk
    from {{ ref('fact_workflow_sessions') }}
    where request_id != ''
    group by org, request_id
) rm on p.request_id != '' and p.org = rm.rf_org and p.request_id = rm.rf_req
-- Session-id fallback: only when request_id didn't match (rm.rf_csk is null)
left join session_id_map sm
    on rm.rf_csk is null and p.session_id != ''
    and p.org = sm.sim_org and p.session_id = sm.sim_sess
