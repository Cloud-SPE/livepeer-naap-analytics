-- Migration 012: Livepeer API enrichment tables
-- Stores orchestrator and gateway metadata fetched periodically from the Livepeer API.
-- Used for ENS name resolution, stake info, and gateway financial data.

-- naap.orch_metadata: orchestrator identity and stake data from /api/orchestrator.
-- ReplacingMergeTree deduplicates by eth_address on merge; use FINAL for consistent reads.
CREATE TABLE IF NOT EXISTS naap.orch_metadata
(
    eth_address       LowCardinality(String),
    name              String,
    service_uri       String,
    avatar            String,
    total_stake       Float64,
    reward_cut        Float64,
    fee_cut           Float64,
    activation_status UInt8,
    updated_at        DateTime
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY eth_address
TTL updated_at + INTERVAL 7 DAY;

-- naap.gateway_metadata: gateway identity and financial data from /api/gateways.
CREATE TABLE IF NOT EXISTS naap.gateway_metadata
(
    eth_address LowCardinality(String),
    name        String,
    avatar      String,
    deposit     Float64,
    reserve     Float64,
    updated_at  DateTime
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY eth_address
TTL updated_at + INTERVAL 7 DAY;
