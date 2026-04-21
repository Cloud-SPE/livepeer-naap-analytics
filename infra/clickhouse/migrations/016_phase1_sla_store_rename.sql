-- Phase 1 of serving-layer-v2: rename canonical_streaming_sla_hourly_store
-- to api_hourly_streaming_sla_store. The resolver has been writing
-- fully-scored rows here all along, but the API view was re-computing
-- the 7-day benchmark cohort on every request instead of reading this
-- pre-scored store.
--
-- After this migration:
--   - Resolver writes to naap.api_hourly_streaming_sla_store
--   - api_hourly_streaming_sla view reads pre-scored rows directly
--   - api_base_sla_compliance_scored_by_org becomes a resolver-internal
--     input (still owns the scoring math for the resolver's write; Phase 2
--     pre-computes the benchmark cohort)
--
-- Data-preserving rename — no row is dropped. The `IF EXISTS` guard
-- makes the migration a safe no-op on fresh stacks where bootstrap
-- v1.sql already creates the target under its new name.

RENAME TABLE IF EXISTS naap.canonical_streaming_sla_hourly_store TO naap.api_hourly_streaming_sla_store;
