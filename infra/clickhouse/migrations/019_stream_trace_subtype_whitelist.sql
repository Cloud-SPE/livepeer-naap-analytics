-- Migration 019: stream_trace subtype whitelist
--
-- PROBLEM
-- ───────
-- mv_typed_stream_trace previously admitted every stream_trace event regardless
-- of data.type (subtype). The Kafka topic carries both contracted gateway/runner
-- lifecycle events and client-side app_* instrumentation events. Because app_*
-- events carry stream_id but NO request_id, they produced a distinct canonical
-- session key (org|stream_id|_missing_request) that shadowed the real gateway
-- session (org|stream_id|request_id) in fact_workflow_sessions. This created
-- ~264 K phantom duplicate sessions — all marked unresolved — inflating the
-- unresolved rate from ~2% to ~38% of all sessions.
--
-- ROOT CAUSE (RULE-INGEST-003)
-- ────────────────────────────
-- RULE-INGEST-003 requires ignored raw families and subtypes to be explicitly
-- classified. The MV was not applying any subtype filter, so every unknown or
-- client-side subtype passed silently into the canonical typed table.
--
-- FIX: WHITELIST APPROACH
-- ───────────────────────
-- Replace the open filter with an explicit whitelist of contracted subtypes.
-- Any subtype not in this list is rejected at ingest time; it never enters
-- typed_stream_trace, never produces a phantom session, and never appears in
-- downstream facts or serving views.
--
-- Adding a new contracted subtype requires a deliberate change to this file —
-- unknown subtypes are detectable via the naap.events table and the
-- TestRuleIngest003_UnknownTraceTypesAreDetectable validation test.
--
-- CONTRACTED SUBTYPES
-- ───────────────────
-- Source of truth: docs/data/EVENT_CATALOG.md (stream_trace subtypes table).
-- All subtypes documented there are listed here. Subtypes observed in
-- production but not yet in the catalog are noted inline.
--
--   Gateway lifecycle (authoritative — emitted by gateway, carry request_id):
--     gateway_receive_stream_request         stream accepted at gateway
--     gateway_send_first_ingest_segment      orch selected; anchor for cap join
--     gateway_ingest_stream_closed           ingest connection closed
--     gateway_receive_first_processed_segment  first output segment back
--     gateway_receive_few_processed_segments   readiness signal (3rd segment)
--     gateway_receive_first_data_segment       first BYOC data segment
--     gateway_no_orchestrators_available     selection failure
--     orchestrator_swap                      mid-stream orch replacement
--
--   Runner-side informational (carry orch_raw_address; used for session→orch
--   attribution. Not yet in EVENT_CATALOG.md — pending catalog update):
--     runner_receive_stream_request
--     runner_receive_first_ingest_segment
--     runner_send_first_processed_segment
--
-- EXCLUDED (examples — not exhaustive)
-- ─────────────────────────────────────
--   app_send_stream_request, app_start_broadcast_stream, app_param_update,
--   app_receive_first_segment, app_user_page_unload,
--   app_capacity_query_response, app_capacity_error_shown,
--   stream_heartbeat, pipeline_load_start, pipeline_loaded,
--   session_created, session_closed, stream_started, stream_stopped,
--   playback_ready, pipeline_unloaded, websocket_connected,
--   websocket_disconnected, error (scope-client errors)
--
-- REHYDRATION
-- ───────────
-- typed_stream_trace is truncated and rebuilt from naap.events so that
-- existing phantom rows created by previously-unfiltered subtypes are removed.
-- On a fresh install this produces zero rows (no events yet) — safe to run.

-- ============================================================================
-- 1. Recreate mv_typed_stream_trace with subtype whitelist
-- ============================================================================

DROP VIEW IF EXISTS naap.mv_typed_stream_trace;

CREATE MATERIALIZED VIEW naap.mv_typed_stream_trace
TO naap.typed_stream_trace
AS
SELECT
    event_id,
    event_ts,
    org,
    gateway,
    JSONExtractString(data, 'stream_id')                            AS stream_id,
    JSONExtractString(data, 'request_id')                           AS request_id,
    JSONExtractString(data, 'type')                                 AS trace_type,
    JSONExtractString(data, 'pipeline')                             AS raw_pipeline_hint,
    JSONExtractString(data, 'pipeline_id')                          AS pipeline_id,
    lower(JSONExtractString(data, 'orchestrator_info', 'address'))  AS orch_raw_address,
    JSONExtractString(data, 'orchestrator_info', 'url')             AS orch_url,
    JSONExtractString(data, 'message')                              AS message,
    data
FROM naap.events
WHERE event_type = 'stream_trace'
  AND JSONExtractString(data, 'type') IN (
      -- Gateway lifecycle (EVENT_CATALOG.md §stream_trace subtypes)
      'gateway_receive_stream_request',
      'gateway_send_first_ingest_segment',
      'gateway_ingest_stream_closed',
      'gateway_receive_first_processed_segment',
      'gateway_receive_few_processed_segments',
      'gateway_receive_first_data_segment',
      'gateway_no_orchestrators_available',
      'orchestrator_swap',
      -- Runner-side informational (observed in production; pending catalog entry)
      'runner_receive_stream_request',
      'runner_receive_first_ingest_segment',
      'runner_send_first_processed_segment'
  );

-- ============================================================================
-- 2. Rehydrate typed_stream_trace from naap.events (whitelist applied)
-- ============================================================================

TRUNCATE TABLE naap.typed_stream_trace;

INSERT INTO naap.typed_stream_trace
SELECT
    event_id,
    event_ts,
    org,
    gateway,
    JSONExtractString(data, 'stream_id')                            AS stream_id,
    JSONExtractString(data, 'request_id')                           AS request_id,
    JSONExtractString(data, 'type')                                 AS trace_type,
    JSONExtractString(data, 'pipeline')                             AS raw_pipeline_hint,
    JSONExtractString(data, 'pipeline_id')                          AS pipeline_id,
    lower(JSONExtractString(data, 'orchestrator_info', 'address'))  AS orch_raw_address,
    JSONExtractString(data, 'orchestrator_info', 'url')             AS orch_url,
    JSONExtractString(data, 'message')                              AS message,
    data
FROM naap.events
WHERE event_type = 'stream_trace'
  AND JSONExtractString(data, 'type') IN (
      'gateway_receive_stream_request',
      'gateway_send_first_ingest_segment',
      'gateway_ingest_stream_closed',
      'gateway_receive_first_processed_segment',
      'gateway_receive_few_processed_segments',
      'gateway_receive_first_data_segment',
      'gateway_no_orchestrators_available',
      'orchestrator_swap',
      'runner_receive_stream_request',
      'runner_receive_first_ingest_segment',
      'runner_send_first_processed_segment'
  );
