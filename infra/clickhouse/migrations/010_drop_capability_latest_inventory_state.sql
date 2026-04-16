-- Migration 010: remove deprecated capability current/latest inventory state.

DROP VIEW IF EXISTS naap.mv_canonical_latest_orchestrator_pipeline_inventory_agg;
DROP TABLE IF EXISTS naap.canonical_latest_orchestrator_pipeline_inventory_agg;
