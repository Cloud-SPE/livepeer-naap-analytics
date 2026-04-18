-- Canonical DDL for naap.api_current_gpu_inventory_store.
-- Source of truth: this file. The resolver writes rows; this declaration
-- governs the physical schema.
--
-- Perf-pass: one row per (orch_address, gpu_id) capturing the latest 24h
-- hardware observation — gpu_model_name, memory, pipeline/model the GPU is
-- most recently serving. Supports the Supply Inventory dashboard's GPU
-- panels without a full scan of canonical_capability_offer_inventory_store
-- (which was ~37 GiB / request under profiling).

CREATE TABLE IF NOT EXISTS naap.api_current_gpu_inventory_store (`org` LowCardinality(String), `orch_address` String, `gpu_id` String, `gpu_model_name` String DEFAULT '', `gpu_memory_bytes_total` UInt64 DEFAULT 0, `canonical_pipeline` String DEFAULT '', `model_id` String DEFAULT '', `last_seen` DateTime64(3, 'UTC'), `refresh_run_id` String, `artifact_checksum` String DEFAULT '', `refreshed_at` DateTime64(3, 'UTC') DEFAULT now64()) ENGINE = MergeTree PARTITION BY tuple() ORDER BY (orch_address, gpu_id, refreshed_at) SETTINGS index_granularity = 8192;
