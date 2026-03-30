-- Migration 056: physical backing table for hourly payment rollups.
--
-- This keeps payment-heavy API reads off the canonical payment-link view and
-- lets the resolver refresh only the affected (org, hour) slices.

CREATE TABLE IF NOT EXISTS naap.api_payment_hourly_store
(
    hour                    DateTime('UTC'),
    org                     LowCardinality(String),
    pipeline                String,
    orch_address            String,
    total_wei               UInt64,
    event_count             UInt64,
    avg_price_wei_per_pixel Float64,
    refresh_run_id          String,
    artifact_checksum       String DEFAULT '',
    refreshed_at            DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = MergeTree()
PARTITION BY (org, toYYYYMM(hour))
ORDER BY (org, hour, pipeline, orch_address, refreshed_at)
SETTINGS index_granularity = 8192;
