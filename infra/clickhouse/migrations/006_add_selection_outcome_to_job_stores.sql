-- Migration 006: add first-class job selection outcomes to resolver-owned job stores.
--
-- This mirrors the live-video selection_outcome enum and keeps request/response
-- job denominator semantics explicit in canonical job facts.

ALTER TABLE naap.canonical_ai_batch_job_store
    ADD COLUMN IF NOT EXISTS `selection_outcome` LowCardinality(String) DEFAULT 'unknown'
    AFTER `orch_url_norm`;

ALTER TABLE naap.canonical_byoc_job_store
    ADD COLUMN IF NOT EXISTS `selection_outcome` LowCardinality(String) DEFAULT 'unknown'
    AFTER `orch_url_norm`;
