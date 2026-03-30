-- Migration 051: repoint accepted-raw aliases and payment typing to the
-- hard-cutover accepted_raw_events path.

DROP VIEW IF EXISTS naap.raw_events;

CREATE VIEW IF NOT EXISTS naap.raw_events AS
SELECT *
FROM naap.accepted_raw_events;

DROP TABLE IF EXISTS naap.mv_typed_payments;

CREATE MATERIALIZED VIEW IF NOT EXISTS naap.mv_typed_payments
TO naap.typed_payments
AS
SELECT
    event_id,
    event_ts,
    org,
    gateway,
    JSONExtractString(data, 'sessionID')                                                       AS session_id,
    JSONExtractString(data, 'requestID')                                                       AS request_id,
    JSONExtractString(data, 'manifestID')                                                      AS manifest_id,
    replaceRegexpOne(JSONExtractString(data, 'manifestID'), '^[0-9]+_', '')                    AS pipeline_hint,
    lower(JSONExtractString(data, 'sender'))                                                   AS sender_address,
    lower(JSONExtractString(data, 'recipient'))                                                AS recipient_address,
    JSONExtractString(data, 'orchestrator')                                                    AS orchestrator_url,
    toUInt64OrDefault(trimRight(replaceAll(JSONExtractString(data, 'faceValue'), ' WEI', ''))) AS face_value_wei,
    toFloat64OrDefault(replaceRegexpOne(JSONExtractString(data, 'price'), ' wei/pixel$', ''))  AS price_wei_per_pixel,
    toFloat64OrDefault(JSONExtractString(data, 'winProb'))                                     AS win_prob,
    toUInt64OrDefault(JSONExtractString(data, 'numTickets'))                                   AS num_tickets,
    data
FROM naap.accepted_raw_events
WHERE event_type = 'create_new_payment';
