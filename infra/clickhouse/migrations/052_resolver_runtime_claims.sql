CREATE TABLE IF NOT EXISTS naap.resolver_window_claims
(
    claim_key                     String,
    claim_type                    LowCardinality(String),
    mode                          LowCardinality(String),
    owner_id                      String,
    org                           Nullable(String),
    window_start                  DateTime64(3, 'UTC'),
    window_end                    DateTime64(3, 'UTC'),
    lease_expires_at              DateTime64(3, 'UTC'),
    released_at                   Nullable(DateTime64(3, 'UTC')),
    created_at                    DateTime64(3, 'UTC'),
    updated_at                    DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(created_at)
ORDER BY (claim_type, ifNull(org, ''), window_start, window_end, claim_key)
TTL toDateTime(created_at) + INTERVAL 7 DAY DELETE
SETTINGS index_granularity = 8192;
