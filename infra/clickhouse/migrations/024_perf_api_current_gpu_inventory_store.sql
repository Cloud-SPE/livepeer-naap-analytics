-- Perf-pass: current 24h GPU inventory snapshot for the Supply Inventory
-- dashboard. Replaces full canonical_capability_offer_inventory scans on
-- the "GPU Count by Type" and "Latest GPU Inventory" panels (~40 GiB read
-- per request, p95 26 s).

CREATE TABLE IF NOT EXISTS naap.api_current_gpu_inventory_store (`org` LowCardinality(String), `orch_address` String, `gpu_id` String, `gpu_model_name` String DEFAULT '', `gpu_memory_bytes_total` UInt64 DEFAULT 0, `canonical_pipeline` String DEFAULT '', `model_id` String DEFAULT '', `last_seen` DateTime64(3, 'UTC'), `refresh_run_id` String, `artifact_checksum` String DEFAULT '', `refreshed_at` DateTime64(3, 'UTC') DEFAULT now64()) ENGINE = MergeTree PARTITION BY tuple() ORDER BY (orch_address, gpu_id, refreshed_at) SETTINGS index_granularity = 8192;
