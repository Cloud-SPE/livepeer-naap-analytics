-- Restores exact settled-payment visibility on the public Jobs dashboard.
-- One row per (org, window_start, capability, payment_type, refresh_run_id)
-- aggregating canonical_byoc_payments.amount into hourly buckets so the
-- dashboard no longer falls back to a price-derived proxy from
-- api_fact_byoc_job.

CREATE TABLE IF NOT EXISTS naap.api_hourly_byoc_payments_store (`window_start` DateTime('UTC'), `org` LowCardinality(String), `capability` String, `payment_type` LowCardinality(String), `payment_count` UInt64 DEFAULT 0, `total_amount` Float64 DEFAULT 0, `currency` LowCardinality(String) DEFAULT '', `unique_orchs` UInt64 DEFAULT 0, `refresh_run_id` String, `artifact_checksum` String DEFAULT '', `refreshed_at` DateTime64(3, 'UTC') DEFAULT now64()) ENGINE = MergeTree PARTITION BY (org, toYYYYMM(window_start)) ORDER BY (org, window_start, capability, payment_type, refreshed_at) SETTINGS index_granularity = 8192;
