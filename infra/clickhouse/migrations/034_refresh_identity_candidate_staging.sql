-- Migration 034: stage identity-bounded capability candidates per refresh chunk.
--
-- The canonical refresh worker uses these staging tables to materialize only
-- the URI/address identities and historical capability rows relevant to the
-- current chunk before attribution matching runs.

CREATE TABLE IF NOT EXISTS naap.canonical_refresh_uri_identity_bounds
(
    run_id           String,
    org              LowCardinality(String),
    orch_uri_norm    String,
    min_snapshot_ts  DateTime64(3, 'UTC'),
    max_snapshot_ts  DateTime64(3, 'UTC'),
    inserted_at      DateTime('UTC') DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(inserted_at)
ORDER BY (run_id, org, orch_uri_norm)
TTL inserted_at + INTERVAL 1 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS naap.canonical_refresh_address_identity_bounds
(
    run_id           String,
    org              LowCardinality(String),
    orch_address     String,
    min_snapshot_ts  DateTime64(3, 'UTC'),
    max_snapshot_ts  DateTime64(3, 'UTC'),
    inserted_at      DateTime('UTC') DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(inserted_at)
ORDER BY (run_id, org, orch_address)
TTL inserted_at + INTERVAL 1 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS naap.canonical_refresh_uri_candidates
(
    run_id          String,
    org             LowCardinality(String),
    orch_uri_norm   String,
    snapshot_row_id String,
    snapshot_ts     DateTime64(3, 'UTC'),
    orch_address    String,
    orch_uri        String,
    inserted_at     DateTime('UTC') DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(inserted_at)
ORDER BY (run_id, org, orch_uri_norm, snapshot_ts, snapshot_row_id)
TTL inserted_at + INTERVAL 1 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS naap.canonical_refresh_address_candidates
(
    run_id               String,
    org                  LowCardinality(String),
    orch_address         String,
    snapshot_row_id      String,
    snapshot_ts          DateTime64(3, 'UTC'),
    matched_orch_address String,
    orch_uri             String,
    inserted_at          DateTime('UTC') DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(inserted_at)
ORDER BY (run_id, org, orch_address, snapshot_ts, snapshot_row_id)
TTL inserted_at + INTERVAL 1 DAY DELETE
SETTINGS index_granularity = 8192;
