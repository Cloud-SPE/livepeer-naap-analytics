-- Migration 007: payment aggregate views (R4 — PAY-001 through PAY-004)
--
-- Source events: create_new_payment (both orgs)
--
-- Payment model: probabilistic micropayments. face_value_wei is the ticket's
-- face value, NOT the expected value. See PAY-001 spec and ADR-001 for
-- reasoning. total_wei stored as UInt64 to avoid float precision loss (PAY-002-a).
--
-- Pipeline derivation: manifestID format is either:
--   "<N>_<pipeline_name>"  (e.g. "5_streamdiffusion-sdxl") → strip the "<N>_" prefix
--   "<pipeline_name>"      (no numeric prefix)               → use as-is
-- replaceRegexpOne handles both cases.
--
-- face_value_wei parsing: faceValue field is "2402400000000000 WEI".
-- Strip " WEI" suffix, then toUInt64OrDefault.

CREATE TABLE IF NOT EXISTS naap.agg_payment_hourly
(
    hour           DateTime,
    org            LowCardinality(String),
    pipeline       LowCardinality(String),
    orch_address   String,
    total_wei      UInt64,
    event_count    UInt64
)
ENGINE = SummingMergeTree((total_wei, event_count))
PARTITION BY toYYYYMM(hour)
ORDER BY (org, pipeline, orch_address, hour)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS naap.mv_payment_hourly
TO naap.agg_payment_hourly
AS
SELECT
    toStartOfHour(event_ts)                                               AS hour,
    org,
    -- Strip numeric capability prefix from manifestID (e.g. "5_streamdiffusion-sdxl" → "streamdiffusion-sdxl").
    replaceRegexpOne(
        JSONExtractString(data, 'manifestID'),
        '^[0-9]+_', ''
    )                                                                      AS pipeline,
    lower(JSONExtractString(data, 'recipient'))                           AS orch_address,
    -- Parse "2402400000000000 WEI" → 2402400000000000
    toUInt64OrDefault(
        trimRight(replaceAll(JSONExtractString(data, 'faceValue'), ' WEI', ''))
    )                                                                      AS total_wei,
    1                                                                      AS event_count
FROM naap.events
WHERE event_type = 'create_new_payment'
  AND JSONExtractString(data, 'recipient') != '';
