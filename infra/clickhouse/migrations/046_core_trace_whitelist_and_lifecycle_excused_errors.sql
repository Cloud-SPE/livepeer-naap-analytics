-- Migration 046: tighten core stream_trace whitelist and expose lifecycle
-- excusable-error counts on canonical session current rows.

ALTER TABLE naap.canonical_session_current_store
    ADD COLUMN IF NOT EXISTS startup_error_count UInt64 DEFAULT 0
    AFTER status_error_sample_count;

ALTER TABLE naap.canonical_session_current_store
    ADD COLUMN IF NOT EXISTS excusable_error_count UInt64 DEFAULT 0
    AFTER startup_error_count;

DROP VIEW IF EXISTS naap.mv_normalized_stream_trace;

CREATE MATERIALIZED VIEW IF NOT EXISTS naap.mv_normalized_stream_trace
TO naap.normalized_stream_trace
AS
SELECT
    event_id,
    event_ts,
    org,
    gateway,
    JSONExtractString(data, 'stream_id') AS stream_id,
    JSONExtractString(data, 'request_id') AS request_id,
    multiIf(
        org = '', '',
        JSONExtractString(data, 'stream_id') != '' AND JSONExtractString(data, 'request_id') != '',
        concat(org, '|', JSONExtractString(data, 'stream_id'), '|', JSONExtractString(data, 'request_id')),
        JSONExtractString(data, 'stream_id') != '',
        concat(org, '|', JSONExtractString(data, 'stream_id'), '|_missing_request'),
        JSONExtractString(data, 'request_id') != '',
        concat(org, '|_missing_stream|', JSONExtractString(data, 'request_id')),
        ''
    ) AS canonical_session_key,
    JSONExtractString(data, 'type') AS trace_type,
    JSONExtractString(data, 'pipeline') AS raw_pipeline_hint,
    JSONExtractString(data, 'pipeline_id') AS pipeline_id,
    lower(JSONExtractString(data, 'orchestrator_info', 'address')) AS orch_raw_address,
    JSONExtractString(data, 'orchestrator_info', 'url') AS orch_url,
    lower(JSONExtractString(data, 'orchestrator_info', 'url')) AS orch_url_norm,
    JSONExtractString(data, 'message') AS message,
    data
FROM naap.events
WHERE event_type = 'stream_trace'
  AND JSONExtractString(data, 'type') IN (
      'gateway_receive_stream_request',
      'gateway_ingest_stream_closed',
      'gateway_send_first_ingest_segment',
      'gateway_receive_first_processed_segment',
      'gateway_receive_few_processed_segments',
      'gateway_receive_first_data_segment',
      'gateway_no_orchestrators_available',
      'orchestrator_swap',
      'runner_receive_first_ingest_segment',
      'runner_send_first_processed_segment'
  );

DROP VIEW IF EXISTS naap.ignored_raw_event_diagnostics;

CREATE VIEW IF NOT EXISTS naap.ignored_raw_event_diagnostics AS
SELECT
    event_id,
    event_ts,
    org,
    event_type,
    multiIf(
        event_type NOT IN (
            'stream_trace', 'ai_stream_status', 'ai_stream_events',
            'stream_ingest_metrics', 'network_capabilities',
            'discovery_results', 'create_new_payment'
        ),
        'unsupported_event_type',
        event_type = 'stream_trace'
            AND startsWith(lowerUTF8(JSONExtractString(data, 'type')), 'app_'),
        'ignored_stream_trace_non_core_app',
        event_type = 'stream_trace'
            AND lowerUTF8(JSONExtractString(data, 'client_source')) = 'scope'
            AND JSONExtractString(data, 'type') IN (
                'stream_heartbeat', 'pipeline_load_start', 'pipeline_loaded',
                'session_created', 'stream_started', 'playback_ready',
                'session_closed', 'stream_stopped', 'pipeline_unloaded',
                'websocket_connected', 'websocket_disconnected', 'error'
            ),
        'ignored_stream_trace_scope_client_noise',
        event_type = 'stream_trace'
            AND JSONExtractString(data, 'type') NOT IN (
                'gateway_receive_stream_request',
                'gateway_ingest_stream_closed',
                'gateway_send_first_ingest_segment',
                'gateway_receive_first_processed_segment',
                'gateway_receive_few_processed_segments',
                'gateway_receive_first_data_segment',
                'gateway_no_orchestrators_available',
                'orchestrator_swap',
                'runner_receive_first_ingest_segment',
                'runner_send_first_processed_segment'
            ),
        'unsupported_stream_trace_type',
        ''
    ) AS ignored_reason
FROM naap.events
WHERE ignored_reason != '';
