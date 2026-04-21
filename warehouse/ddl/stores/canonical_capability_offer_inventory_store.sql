-- Canonical DDL for naap.canonical_capability_offer_inventory_store.
-- Source of truth: this file. The resolver and MVs write rows;
-- this declaration governs the physical schema.
-- Apply with scripts/apply-store-ddl.sh; drift is caught by
-- `make lint-store-ddl`.

CREATE TABLE IF NOT EXISTS naap.canonical_capability_offer_inventory_store (`snapshot_row_id` String, `source_event_id` String, `snapshot_ts` DateTime64(3, 'UTC'), `org` LowCardinality(String), `orch_address` String, `orch_uri` String, `orch_uri_norm` String, `capability_id` Nullable(UInt16), `offered_name` Nullable(String), `model_id` Nullable(String), `warm` Nullable(UInt8), `advertised_capacity` Nullable(Int32), `hardware_present` UInt8, `gpu_id` Nullable(String), `gpu_model_name` Nullable(String), `gpu_memory_bytes_total` Nullable(UInt64)) ENGINE = ReplacingMergeTree(snapshot_ts) PARTITION BY (org, toYYYYMM(snapshot_ts)) ORDER BY (org, snapshot_ts, orch_address, ifNull(model_id, ''), ifNull(gpu_id, ''), snapshot_row_id) SETTINGS index_granularity = 8192;
