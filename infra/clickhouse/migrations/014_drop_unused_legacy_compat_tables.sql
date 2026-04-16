-- Migration 014: remove legacy compatibility tables and redundant capability
-- snapshot helper indexes that are no longer read by the active runtime.

DROP VIEW IF EXISTS naap.mv_canonical_capability_snapshots_by_address;
DROP VIEW IF EXISTS naap.mv_canonical_capability_snapshots_by_uri;

DROP TABLE IF EXISTS naap.canonical_capability_snapshots_by_address;
DROP TABLE IF EXISTS naap.canonical_capability_snapshots_by_uri;

DROP TABLE IF EXISTS naap.canonical_session_attribution_audit;
DROP TABLE IF EXISTS naap.canonical_session_attribution_latest_store;
