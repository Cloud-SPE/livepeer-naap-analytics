-- Migration 001: add canonical_payment_links_store
--
-- Materialises payment-link data into a partitioned, indexed ReplacingMergeTree
-- so the resolver can write per-event rows incrementally instead of querying the
-- expensive canonical_payment_links view (which full-scans stg_payments on every
-- call via NOT-IN anti-joins).
--
-- After this table is populated (via the resolver + backfill), the
-- canonical_payment_links view should be swapped to read from this store.
--
-- Deployment order:
--   1. Apply this migration (CREATE TABLE)
--   2. Deploy resolver with insertPaymentLinkRows + updated insertPaymentHourlyRollups
--   3. Run backfill (see scripts/backfill_payment_links.sql)
--   4. Swap canonical_payment_links view (see step 4 comment in that script)

CREATE TABLE IF NOT EXISTS naap.canonical_payment_links_store
(
    `event_id`              String,
    `event_ts`              DateTime64(3, 'UTC'),
    `org`                   LowCardinality(String),
    `gateway`               String,
    `session_id`            String,
    `request_id`            String,
    `manifest_id`           String,
    `pipeline_hint`         String,
    `sender_address`        String,
    `recipient_address`     String,
    `orchestrator_url`      String,
    `face_value_wei`        UInt64,
    `price_wei_per_pixel`   Float64,
    `win_prob`              Float64,
    `num_tickets`           UInt64,
    `canonical_session_key` Nullable(String),
    `link_method`           LowCardinality(String),  -- 'request_id' | 'unlinked'
    `link_status`           LowCardinality(String),  -- 'resolved'   | 'unresolved'
    `refresh_run_id`        String,
    `artifact_checksum`     String          DEFAULT '',
    `refreshed_at`          DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = ReplacingMergeTree(refreshed_at)
PARTITION BY (org, toYYYYMM(event_ts))
ORDER BY (org, toStartOfHour(event_ts), event_id)
SETTINGS index_granularity = 8192;