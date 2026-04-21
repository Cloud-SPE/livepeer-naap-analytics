-- Phase 8 of serving-layer-v2: drop the nine legacy agg_* aggregation
-- tables from the pre-canonical era.
--
-- Nothing reads these after Phase 6 (serving layer moved to api_hourly_*
-- and api_current_*) and Phase 7 (the one remaining dashboard reader,
-- naap-performance-drilldown's Jitter by Orchestrator panel that touched
-- agg_webrtc_hourly, was removed when debug panels migrated into
-- infra/grafana/dashboards/internal/naap-internal-debug.json). No
-- resolver writer, no dbt model, no Go handler, and no surviving alert
-- rule references any agg_* table — drop is free.
--
-- Safe to re-run (DROP IF EXISTS).

-- MVs must go first so DROP TABLE on their targets does not race.
DROP VIEW IF EXISTS naap.mv_discovery_latency_hourly;
DROP VIEW IF EXISTS naap.mv_fps_hourly;
DROP VIEW IF EXISTS naap.mv_orch_reliability_hourly;
DROP VIEW IF EXISTS naap.mv_orch_state;
DROP VIEW IF EXISTS naap.mv_payment_hourly;
DROP VIEW IF EXISTS naap.mv_stream_hourly;
DROP VIEW IF EXISTS naap.mv_stream_state;
DROP VIEW IF EXISTS naap.mv_stream_status_samples;
DROP VIEW IF EXISTS naap.mv_webrtc_hourly;

DROP TABLE IF EXISTS naap.agg_discovery_latency_hourly;
DROP TABLE IF EXISTS naap.agg_fps_hourly;
DROP TABLE IF EXISTS naap.agg_orch_reliability_hourly;
DROP TABLE IF EXISTS naap.agg_orch_state;
DROP TABLE IF EXISTS naap.agg_payment_hourly;
DROP TABLE IF EXISTS naap.agg_stream_hourly;
DROP TABLE IF EXISTS naap.agg_stream_state;
DROP TABLE IF EXISTS naap.agg_stream_status_samples;
DROP TABLE IF EXISTS naap.agg_webrtc_hourly;
