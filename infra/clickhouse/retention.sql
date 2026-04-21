-- Data Retention Reference: NAAP Analytics
-- ============================================================
-- This file is the canonical reference for all ClickHouse TTL
-- settings in the naap database. It corresponds to the TTLs
-- set in infra/clickhouse/bootstrap/v1.sql and documents the
-- rationale for each tier.
--
-- For the full policy and Kafka alignment, see:
--   docs/operations/data-retention-policy.md
--
-- To apply a TTL change to a live cluster:
--   ALTER TABLE naap.<table> MODIFY TTL <expression>;
-- TTL mutations run asynchronously. Monitor via:
--   SELECT * FROM system.mutations WHERE is_done = 0;
-- ============================================================

-- ------------------------------------------------------------
-- Tier 1: Raw ingest (90 days)
-- Rationale: authoritative event archive inside the bounded
-- replay/audit window. ClickHouse is the sole source of truth
-- once Kafka retention has elapsed, but current production keeps
-- this layer aligned to a 90-day audit window rather than 365 days.
-- ------------------------------------------------------------

ALTER TABLE naap.accepted_raw_events
    MODIFY TTL toDateTime(event_ts) + toIntervalDay(90);

-- ignored_raw_events is the functional DLQ/quarantine equivalent.
-- No Kafka-level DLQ topics exist; all rejected events land here.
-- Same TTL as accepted events for symmetry and audit coverage.
ALTER TABLE naap.ignored_raw_events
    MODIFY TTL toDateTime(event_ts) + toIntervalDay(90);

-- ------------------------------------------------------------
-- Tier 2: Aggregate samples (RETIRED Phase 8)
-- The legacy agg_* tables were unreferenced after Phase 6 moved the
-- serving layer onto api_hourly_* and api_current_* stores; migration
-- 022 drops them. No TTL to carry forward.
-- ------------------------------------------------------------

-- ------------------------------------------------------------
-- Tier 3: Entity metadata / cache (7 days)
-- Rationale: these tables are refreshed continuously by the
-- resolver and Kafka ingest. 7 days covers any downtime window;
-- stale entries beyond that have no operational value.
-- ------------------------------------------------------------

ALTER TABLE naap.gateway_metadata
    MODIFY TTL updated_at + toIntervalDay(7);

ALTER TABLE naap.orch_metadata
    MODIFY TTL updated_at + toIntervalDay(7);

-- ------------------------------------------------------------
-- Tier 4: Resolver operational state (7–30 days)
-- ------------------------------------------------------------

ALTER TABLE naap.resolver_dirty_partitions
    MODIFY TTL toDateTime(event_date) + toIntervalDay(30);

ALTER TABLE naap.resolver_window_claims
    MODIFY TTL toDateTime(created_at) + toIntervalDay(7);

-- ------------------------------------------------------------
-- Tier 6: Resolver working tables (1–2 days)
-- Ephemeral per-query scratch tables.
-- ------------------------------------------------------------

ALTER TABLE naap.resolver_query_event_ids
    MODIFY TTL toDateTime(created_at) + toIntervalDay(1);

ALTER TABLE naap.resolver_query_identities
    MODIFY TTL toDateTime(created_at) + toIntervalDay(1);

ALTER TABLE naap.resolver_query_selection_event_ids
    MODIFY TTL toDateTime(created_at) + toIntervalDay(1);

ALTER TABLE naap.resolver_query_session_keys
    MODIFY TTL toDateTime(created_at) + toIntervalDay(1);

ALTER TABLE naap.resolver_query_window_slices
    MODIFY TTL created_at + toIntervalDay(2);

-- ------------------------------------------------------------
-- Tier 1b: Normalized event tables — AI Batch / BYOC (90 days)
-- Rationale: matches accepted_raw_events retention so that canonical
-- models can always be recomputed from normalized tables within the
-- same audit window.
-- ------------------------------------------------------------

ALTER TABLE naap.normalized_ai_batch_job
    MODIFY TTL toDateTime(event_ts) + toIntervalDay(90);

ALTER TABLE naap.normalized_ai_llm_request
    MODIFY TTL toDateTime(event_ts) + toIntervalDay(90);

ALTER TABLE naap.normalized_byoc_job
    MODIFY TTL toDateTime(event_ts) + toIntervalDay(90);

ALTER TABLE naap.normalized_byoc_auth
    MODIFY TTL toDateTime(event_ts) + toIntervalDay(90);

ALTER TABLE naap.normalized_worker_lifecycle
    MODIFY TTL toDateTime(event_ts) + toIntervalDay(90);

ALTER TABLE naap.normalized_byoc_payment
    MODIFY TTL toDateTime(event_ts) + toIntervalDay(90);

-- ------------------------------------------------------------
-- KNOWN GAPS — Tables with no TTL (unbounded growth)
-- These tables grow indefinitely and require a separate
-- decision before a TTL can be applied safely.
--
-- Candidate TTL: toDateTime(<timestamp_col>) + toIntervalDay(90)
-- to align with raw event retention. File a follow-up ticket
-- before applying any ALTER to these tables in production.
--
-- ALTER TABLE naap.api_current_capability_store         MODIFY TTL ...;
-- ALTER TABLE naap.api_current_gpu_inventory_store      MODIFY TTL ...;
-- ALTER TABLE naap.api_current_orchestrator_store       MODIFY TTL ...;
-- ALTER TABLE naap.api_hourly_request_demand_store      MODIFY TTL ...;
-- ALTER TABLE naap.api_hourly_byoc_auth_store           MODIFY TTL ...;
-- ALTER TABLE naap.api_hourly_byoc_payments_store       MODIFY TTL ...;
-- ALTER TABLE naap.canonical_payment_links_store        MODIFY TTL ...;
-- ALTER TABLE naap.canonical_session_current_store      MODIFY TTL ...;
-- ALTER TABLE naap.canonical_sla_benchmark_daily_store  MODIFY TTL ...;
-- ------------------------------------------------------------
