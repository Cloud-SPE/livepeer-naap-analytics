-- Canonical DDL for naap.api_current_capability_store.
-- Source of truth: this file. The resolver writes rows; this declaration
-- governs the physical schema.
--
-- Phase 4 of serving-layer-v2: unified capability spine. One row per
-- (org, orch_address, capability_id, canonical_pipeline, model_id, gpu_id)
-- denormalizing builtin offers (canonical_capability_offer_inventory),
-- byoc worker registrations (canonical_byoc_workers), pricing
-- (canonical_capability_pricing_inventory), and orchestrator identity
-- (canonical_capability_orchestrator_identity_latest) so /v1/requests/*,
-- /v1/streaming/*, and /v1/dashboard/pipeline-catalog read a single spine
-- instead of scanning canonical_capability_offer_inventory_store (~1 GiB
-- per refresh).

CREATE TABLE IF NOT EXISTS naap.api_current_capability_store (`org` LowCardinality(String), `orch_address` String, `orchestrator_uri` String DEFAULT '', `orchestrator_name` String DEFAULT '', `capability_id` UInt16 DEFAULT 0, `capability_name` String DEFAULT '', `capability_family` LowCardinality(String) DEFAULT '', `canonical_pipeline` String DEFAULT '', `model_id` String DEFAULT '', `gpu_id` String DEFAULT '', `advertised_capacity` UInt32 DEFAULT 0, `hardware_present` UInt8 DEFAULT 0, `supports_request` UInt8 DEFAULT 0, `supports_stream` UInt8 DEFAULT 0, `price_per_unit` Float64 DEFAULT 0, `price_currency` LowCardinality(String) DEFAULT '', `last_seen` DateTime64(3, 'UTC'), `refresh_run_id` String, `artifact_checksum` String DEFAULT '', `refreshed_at` DateTime64(3, 'UTC') DEFAULT now64()) ENGINE = MergeTree PARTITION BY tuple() ORDER BY (org, orch_address, capability_id, canonical_pipeline, model_id, gpu_id, refreshed_at) SETTINGS index_granularity = 8192;
