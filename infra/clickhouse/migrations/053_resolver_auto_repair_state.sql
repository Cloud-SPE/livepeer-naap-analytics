CREATE TABLE IF NOT EXISTS naap.resolver_dirty_partitions
(
    org                LowCardinality(String),
    event_date         Date,
    status             LowCardinality(String),
    reason             LowCardinality(String),
    first_dirty_at     DateTime64(3, 'UTC'),
    last_dirty_at      DateTime64(3, 'UTC'),
    claim_owner        Nullable(String),
    lease_expires_at   Nullable(DateTime64(3, 'UTC')),
    attempt_count      UInt32,
    last_error_summary Nullable(String),
    updated_at         DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(event_date)
ORDER BY (org, event_date)
TTL toDateTime(event_date) + INTERVAL 30 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS naap.resolver_runtime_state
(
    state_key         String,
    last_ingested_at  DateTime64(3, 'UTC'),
    last_event_id     String,
    updated_at        DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (state_key)
SETTINGS index_granularity = 8192;
