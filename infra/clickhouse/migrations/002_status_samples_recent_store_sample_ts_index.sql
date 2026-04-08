-- Migration 002: add minmax skip index on sample_ts in canonical_status_samples_recent_store
--
-- The job-feed endpoint (GET /v1/dashboard/job-feed) filters the store by
-- sample_ts > now() - 120s, but the table is ORDER BY (event_id, refreshed_at)
-- so ClickHouse has no way to skip granules and must scan the entire current-month
-- partition (~378 M rows as of April 2026).
--
-- A minmax skip index lets ClickHouse skip 8192-row granules whose entire
-- sample_ts range is older than the query window.  Because rows are inserted in
-- refresh-run order (which tracks wall-clock time closely), recent rows tend to
-- cluster in the latest granules, making the index effective.
--
-- Deployment:
--   1. Apply the ALTER TABLE (online, no downtime — ClickHouse builds the index
--      lazily on new parts and via MATERIALIZE INDEX for existing parts).
--   2. Optionally force backfill on existing parts:
--        ALTER TABLE naap.canonical_status_samples_recent_store
--            MATERIALIZE INDEX idx_sample_ts;
--      This is a background operation and can be skipped; the index will be
--      populated naturally as parts are merged.

ALTER TABLE naap.canonical_status_samples_recent_store
    ADD INDEX IF NOT EXISTS idx_sample_ts sample_ts TYPE minmax GRANULARITY 8;
