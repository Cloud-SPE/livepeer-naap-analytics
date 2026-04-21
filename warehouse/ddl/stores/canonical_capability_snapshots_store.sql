-- Canonical DDL for naap.canonical_capability_snapshots_store.
-- Source of truth: this file. The resolver and MVs write rows;
-- this declaration governs the physical schema.
-- Apply with scripts/apply-store-ddl.sh; drift is caught by
-- `make lint-store-ddl`.

CREATE TABLE IF NOT EXISTS naap.canonical_capability_snapshots_store (`snapshot_row_id` String, `source_event_id` String, `snapshot_ts` DateTime64(3, 'UTC'), `org` LowCardinality(String), `orch_address` String, `orch_name` String, `orch_uri` String, `orch_uri_norm` String, `version` String, `raw_capabilities` String) ENGINE = ReplacingMergeTree(snapshot_ts) PARTITION BY (org, toYYYYMM(snapshot_ts)) ORDER BY (org, snapshot_ts, orch_address, orch_uri_norm, snapshot_row_id) SETTINGS index_granularity = 8192;
