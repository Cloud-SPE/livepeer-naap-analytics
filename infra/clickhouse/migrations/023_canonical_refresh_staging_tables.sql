-- Migration 023: staging tables for canonical refresh keys and slices.
--
-- The refresh worker stages affected session keys and org/window slices here so
-- it can join on bounded datasets instead of expanding large literal IN lists.

CREATE TABLE IF NOT EXISTS naap.canonical_refresh_session_keys
(
    run_id                String,
    canonical_session_key String,
    inserted_at           DateTime('UTC') DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(inserted_at)
ORDER BY (run_id, canonical_session_key)
TTL inserted_at + INTERVAL 1 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS naap.canonical_refresh_window_slices
(
    run_id       String,
    org          LowCardinality(String),
    window_start DateTime('UTC'),
    inserted_at  DateTime('UTC') DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(inserted_at)
ORDER BY (run_id, org, window_start)
TTL inserted_at + INTERVAL 1 DAY DELETE
SETTINGS index_granularity = 8192;
