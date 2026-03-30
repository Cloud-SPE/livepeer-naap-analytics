-- Migration 037: resolver query key staging tables.
--
-- The selection-centered resolver must not rely on large IN (...) lists for
-- session-key or selection-id filtering because the affected key sets can grow
-- unpredictably during full-day backfill and repair runs. These staging tables
-- let the resolver upload bounded key sets once and use joins for all follow-on
-- lookups.

CREATE TABLE IF NOT EXISTS naap.resolver_query_session_keys
(
    query_id               String,
    org                    LowCardinality(String),
    canonical_session_key  String,
    created_at             DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(created_at)
ORDER BY (query_id, org, canonical_session_key)
TTL toDateTime(created_at) + INTERVAL 1 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS naap.resolver_query_selection_event_ids
(
    query_id             String,
    selection_event_id   String,
    created_at           DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(created_at)
ORDER BY (query_id, selection_event_id)
TTL toDateTime(created_at) + INTERVAL 1 DAY DELETE
SETTINGS index_granularity = 8192;
