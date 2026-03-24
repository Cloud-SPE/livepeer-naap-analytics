-- Migration 004: ingestion materialized views (Kafka → events)
--
-- These MVs bridge the Kafka engine tables into the raw events table.
-- Each MV fires on every INSERT batch the Kafka engine produces.
--
-- timestamp parsing: Kafka events carry ISO8601 strings like
--   "2026-03-24T09:00:00.000Z". parseDateTime64BestEffort handles the
--   millisecond precision and UTC offset correctly.
--
-- org assignment: derived from topic (not the event payload) — this is
-- authoritative per ADR-001. network_events → 'daydream',
-- streaming_events → 'cloudspe'.
--
-- Deduplication: the target table (events) is ReplacingMergeTree keyed on
-- event_id. Duplicate event_ids from Kafka at-least-once delivery are
-- collapsed during background merges.

CREATE MATERIALIZED VIEW IF NOT EXISTS naap.mv_ingest_network_events
TO naap.events
AS
SELECT
    id                                       AS event_id,
    type                                     AS event_type,
    parseDateTime64BestEffort(timestamp)     AS event_ts,
    'daydream'                               AS org,
    gateway,
    data,
    now64()                                  AS ingested_at
FROM naap.kafka_network_events
WHERE id != '' AND type != '';

CREATE MATERIALIZED VIEW IF NOT EXISTS naap.mv_ingest_streaming_events
TO naap.events
AS
SELECT
    id                                       AS event_id,
    type                                     AS event_type,
    parseDateTime64BestEffort(timestamp)     AS event_ts,
    'cloudspe'                               AS org,
    gateway,
    data,
    now64()                                  AS ingested_at
FROM naap.kafka_streaming_events
WHERE id != '' AND type != '';
