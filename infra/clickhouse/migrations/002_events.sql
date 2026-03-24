-- Migration 002: raw events table
--
-- Design decisions (see phase-01-clickhouse-schema.md):
--   - ReplacingMergeTree: background deduplication on event_id.
--     Queries use FINAL or aggregate views (not raw table) for accurate counts.
--   - Partitioned by (org, month): partition pruning on org+time for all queries.
--   - ORDER BY (org, event_type, event_ts, event_id): supports time-range scans
--     filtered by org and/or event_type, which is the primary query pattern.
--   - data stored as raw JSON String: envelope fields are typed; payload is raw.
--     Aggregate MVs pre-extract the key fields. JSONExtract* used in ad-hoc queries.
--   - TTL 365 days: configurable via ALTER TABLE (see README.md).
--   - LowCardinality for org, event_type: both have ≤ 20 distinct values.

CREATE TABLE IF NOT EXISTS naap.events
(
    event_id    String,
    event_type  LowCardinality(String),
    event_ts    DateTime64(3, 'UTC'),
    org         LowCardinality(String),
    gateway     String,
    data        String,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = ReplacingMergeTree()
PARTITION BY (org, toYYYYMM(event_ts))
ORDER BY (org, event_type, event_ts, event_id)
TTL toDateTime(event_ts) + INTERVAL 365 DAY
SETTINGS index_granularity = 8192;
