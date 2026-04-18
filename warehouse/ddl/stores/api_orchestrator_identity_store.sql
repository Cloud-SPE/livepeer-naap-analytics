-- Canonical DDL for naap.api_orchestrator_identity_store.
-- Source of truth: this file. The resolver writes rows; this declaration
-- governs the physical schema. Apply with scripts/apply-store-ddl.sh; drift
-- is caught by warehouse/tests/test_store_ddl_drift.sql.
--
-- Perf-pass promotion of api_orchestrator_identity. The prior view
-- expanded into a multi-CTE argMaxIfMerge scan of canonical_capability_snapshot_latest
-- (~1.2 MiB read for a single row lookup, 315 GiB/2h under regular dashboard
-- load). Resolver now flattens that chain into one row per orch_address each
-- run; dashboards and template variables read the store directly.

CREATE TABLE IF NOT EXISTS naap.api_orchestrator_identity_store (`orch_address` String, `name` String DEFAULT '', `orch_name` String DEFAULT '', `orch_label` String DEFAULT '', `orchestrator_uri` String DEFAULT '', `orch_uri_norm` String DEFAULT '', `version` String DEFAULT '', `org` LowCardinality(String), `capability_version_id` String DEFAULT '', `snapshot_event_id` String DEFAULT '', `last_seen` DateTime64(3, 'UTC'), `refresh_run_id` String, `artifact_checksum` String DEFAULT '', `refreshed_at` DateTime64(3, 'UTC') DEFAULT now64()) ENGINE = MergeTree PARTITION BY tuple() ORDER BY (orch_address, refreshed_at) SETTINGS index_granularity = 8192;
