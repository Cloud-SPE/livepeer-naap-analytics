CREATE TABLE IF NOT EXISTS naap.resolver_query_window_slices
(
    query_id     String,
    org          LowCardinality(String),
    window_start DateTime('UTC'),
    created_at   DateTime('UTC') DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (query_id, org, window_start, created_at)
TTL created_at + INTERVAL 2 DAY
SETTINGS index_granularity = 8192;
