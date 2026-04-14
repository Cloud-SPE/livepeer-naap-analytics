-- Migration 007: add same-day dirty-window repair state, queued repair
-- requests, and resolver run timing needed by same-day auto-heal dashboards
-- and alerts.

CREATE TABLE IF NOT EXISTS naap.resolver_dirty_windows
(
    `org` LowCardinality(String),
    `window_start` DateTime64(3, 'UTC'),
    `window_end` DateTime64(3, 'UTC'),
    `status` LowCardinality(String),
    `reason` LowCardinality(String),
    `first_dirty_at` DateTime64(3, 'UTC'),
    `last_dirty_at` DateTime64(3, 'UTC'),
    `claim_owner` Nullable(String),
    `lease_expires_at` Nullable(DateTime64(3, 'UTC')),
    `attempt_count` UInt32,
    `last_error_summary` Nullable(String),
    `updated_at` DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(window_start)
ORDER BY (org, window_start, window_end)
TTL toDateTime(window_start) + toIntervalDay(30)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS naap.resolver_repair_requests
(
    `request_id` String,
    `org` Nullable(String),
    `window_start` DateTime64(3, 'UTC'),
    `window_end` DateTime64(3, 'UTC'),
    `status` LowCardinality(String),
    `requested_by` String,
    `reason` String,
    `dry_run` UInt8 DEFAULT 0,
    `claim_owner` Nullable(String),
    `lease_expires_at` Nullable(DateTime64(3, 'UTC')),
    `attempt_count` UInt32 DEFAULT 0,
    `started_at` Nullable(DateTime64(3, 'UTC')),
    `finished_at` Nullable(DateTime64(3, 'UTC')),
    `last_error_summary` Nullable(String),
    `created_at` DateTime64(3, 'UTC') DEFAULT now64(),
    `updated_at` DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(created_at)
ORDER BY (ifNull(org, ''), window_start, request_id)
TTL toDateTime(created_at) + toIntervalDay(30)
SETTINGS index_granularity = 8192;

ALTER TABLE naap.resolver_runs
    ADD COLUMN IF NOT EXISTS dirty_quiet_period_seconds UInt64 DEFAULT 0
    AFTER lateness_window_seconds;
