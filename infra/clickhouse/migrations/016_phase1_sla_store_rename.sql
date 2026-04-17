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
-- Data-preserving rename — no row is dropped. Fails loudly on fresh
-- stacks where the source does not exist; in that case the bootstrap
-- already has the new name and this migration is a no-op to run.

RENAME TABLE naap.canonical_streaming_sla_hourly_store TO naap.api_hourly_streaming_sla_store;
