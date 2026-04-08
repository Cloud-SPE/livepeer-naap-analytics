-- backfill_payment_links.sql
--
-- One-time backfill that populates canonical_payment_links_store from
-- accepted_raw_events for all historical payment events.
--
-- Run this AFTER deploying the resolver with insertPaymentLinkRows so that
-- concurrent resolver writes and backfill writes do not conflict
-- (ReplacingMergeTree handles duplicates; latest refreshed_at wins).
--
-- Run each month separately to control memory.  Replace YYYYMM with the
-- target month (e.g. 202603, 202604).
--
-- Step 1 — run per month (repeat for each month in the data):
--
--   INSERT INTO naap.canonical_payment_links_store
--   ... (see query below) ...
--   WHERE toYYYYMM(event_ts) = 202603;   -- change per batch
--
-- Step 2 — verify counts match after each batch:
--
--   SELECT
--       toYYYYMM(event_ts) AS month,
--       count()            AS store_rows
--   FROM naap.canonical_payment_links_store
--   GROUP BY month ORDER BY month;
--
--   SELECT
--       toYYYYMM(event_ts) AS month,
--       org,
--       count()            AS raw_rows
--   FROM naap.accepted_raw_events
--   WHERE event_type = 'create_new_payment' AND event_id != ''
--   GROUP BY month, org ORDER BY month, org;
--
-- Step 3 — once ALL months are backfilled, swap the view:
--
--   CREATE OR REPLACE VIEW naap.canonical_payment_links AS
--   ... (run the dbt model via `dbt run --select canonical_payment_links`) ...
--
-- ─────────────────────────────────────────────────────────────────────────────
-- BACKFILL QUERY  (parameterise YYYYMM per batch)
-- ─────────────────────────────────────────────────────────────────────────────

INSERT INTO naap.canonical_payment_links_store
(
    event_id, event_ts, org, gateway,
    session_id, request_id, manifest_id, pipeline_hint,
    sender_address, recipient_address, orchestrator_url,
    face_value_wei, price_wei_per_pixel, win_prob, num_tickets,
    canonical_session_key, link_method, link_status,
    refresh_run_id, artifact_checksum, refreshed_at
)
SELECT
    p.event_id,
    p.event_ts,
    p.org,
    p.gateway,
    JSONExtractString(p.data, 'sessionID')                                                          AS session_id,
    JSONExtractString(p.data, 'requestID')                                                          AS request_id,
    JSONExtractString(p.data, 'manifestID')                                                         AS manifest_id,
    replaceRegexpOne(JSONExtractString(p.data, 'manifestID'), '^[0-9]+_', '')                       AS pipeline_hint,
    lower(JSONExtractString(p.data, 'sender'))                                                      AS sender_address,
    lower(JSONExtractString(p.data, 'recipient'))                                                   AS recipient_address,
    JSONExtractString(p.data, 'orchestrator')                                                       AS orchestrator_url,
    toUInt64OrDefault(trimRight(replaceAll(JSONExtractString(p.data, 'faceValue'), ' WEI', '')))    AS face_value_wei,
    toFloat64OrDefault(replaceRegexpOne(JSONExtractString(p.data, 'price'), ' wei/pixel$', ''))     AS price_wei_per_pixel,
    toFloat64OrDefault(JSONExtractString(p.data, 'winProb'))                                        AS win_prob,
    toUInt64OrDefault(JSONExtractString(p.data, 'numTickets'))                                      AS num_tickets,
    fs.canonical_session_key,
    if(
        JSONExtractString(p.data, 'requestID') != '' AND isNotNull(fs.canonical_session_key),
        'request_id',
        'unlinked'
    )                                                                                               AS link_method,
    if(isNotNull(fs.canonical_session_key), 'resolved', 'unresolved')                              AS link_status,
    'backfill-v1',
    '',
    now64()
FROM naap.accepted_raw_events p
LEFT JOIN naap.canonical_session_current_store fs FINAL
    ON  fs.org = p.org
    AND fs.request_id = JSONExtractString(p.data, 'requestID')
    AND JSONExtractString(p.data, 'requestID') != ''
WHERE p.event_type  = 'create_new_payment'
  AND p.event_id    != ''
  AND toYYYYMM(p.event_ts) = 202603;  -- ← change per batch (202603, 202604, ...)
