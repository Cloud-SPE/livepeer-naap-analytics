-- canonical_payment_links
--
-- DEPLOYMENT NOTE: this model reads from canonical_payment_links_store, which is
-- populated incrementally by the resolver (insertPaymentLinkRows) and by the
-- one-time backfill in scripts/backfill_payment_links.sql.
--
-- DO NOT re-deploy this view until the backfill for all historical months has
-- completed.  Verify with:
--   SELECT toYYYYMM(event_ts) AS month, count() AS rows
--   FROM naap.canonical_payment_links_store
--   GROUP BY month ORDER BY month;
-- Row counts should match accepted_raw_events WHERE event_type = 'create_new_payment'.
--
-- Previous implementation full-scanned stg_payments (4 M+ rows) via NOT-IN
-- anti-joins on every query.  The store-backed version is an O(window) read.
with {{ latest_value_cte('latest_links', 'naap.canonical_payment_links_store', ['org', 'event_id'], 'refresh_run_id') }}
select
    s.event_id,
    s.event_ts,
    s.org,
    s.gateway,
    s.session_id,
    s.request_id,
    s.manifest_id,
    s.pipeline_hint,
    s.sender_address,
    s.recipient_address,
    s.orchestrator_url,
    s.face_value_wei,
    s.price_wei_per_pixel,
    s.win_prob,
    s.num_tickets,
    s.canonical_session_key,
    s.link_method,
    s.link_status
from naap.canonical_payment_links_store s
inner join latest_links l
    on {{ join_on_columns('s', 'l', ['org', 'event_id']) }}
   and s.refresh_run_id = l.refresh_run_id
