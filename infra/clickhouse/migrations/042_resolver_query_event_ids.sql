CREATE TABLE IF NOT EXISTS naap.resolver_query_event_ids
(
    query_id    String,
    event_id    String,
    created_at  DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(created_at)
ORDER BY (query_id, event_id)
TTL toDateTime(created_at) + INTERVAL 1 DAY DELETE
SETTINGS index_granularity = 8192;
