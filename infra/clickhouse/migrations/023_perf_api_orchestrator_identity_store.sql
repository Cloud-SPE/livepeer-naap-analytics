-- Perf-pass: promote api_orchestrator_identity from a view over the
-- multi-CTE canonical_capability_orchestrator_identity_latest (which drove
-- 315 GiB of reads per 2h of dashboard traffic in local profiling) to a
-- resolver-written store + latest-slice reader. Read path collapses to a
-- primary-key lookup.
--
-- Safe on fresh stacks (bootstrap has this table) and safe to re-run.

CREATE TABLE IF NOT EXISTS naap.api_orchestrator_identity_store (`orch_address` String, `name` String DEFAULT '', `orch_name` String DEFAULT '', `orch_label` String DEFAULT '', `orchestrator_uri` String DEFAULT '', `orch_uri_norm` String DEFAULT '', `version` String DEFAULT '', `org` LowCardinality(String), `capability_version_id` String DEFAULT '', `snapshot_event_id` String DEFAULT '', `last_seen` DateTime64(3, 'UTC'), `refresh_run_id` String, `artifact_checksum` String DEFAULT '', `refreshed_at` DateTime64(3, 'UTC') DEFAULT now64()) ENGINE = MergeTree PARTITION BY tuple() ORDER BY (orch_address, refreshed_at) SETTINGS index_granularity = 8192;
