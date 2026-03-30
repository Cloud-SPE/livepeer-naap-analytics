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
        event_type = 'stream_trace' AND JSONExtractString(data, 'type') = 'app_send_stream_request',
        'ignored_stream_trace_app_send_stream_request',
        event_type = 'stream_trace'
            AND JSONExtractString(data, 'sender', 'type') = 'app'
            AND lowerUTF8(JSONExtractString(data, 'sender', 'id')) = 'daydream'
            AND startsWith(JSONExtractString(data, 'type'), 'app_')
            AND (
                JSONExtractString(data, 'stream_id') = ''
                OR JSONExtractString(data, 'request_id') = ''
            ),
        'ignored_stream_trace_daydream_app_missing_identity',
        event_type = 'stream_trace'
            AND JSONExtractString(data, 'sender', 'type') = ''
            AND JSONExtractString(data, 'sender', 'id') = ''
            AND lowerUTF8(JSONExtractString(data, 'client_source')) = 'scope'
            AND JSONExtractString(data, 'stream_id') = ''
            AND JSONExtractString(data, 'type') IN (
                'stream_heartbeat', 'pipeline_load_start', 'pipeline_loaded',
                'session_created', 'stream_started', 'playback_ready',
                'session_closed', 'stream_stopped', 'pipeline_unloaded',
                'websocket_connected', 'websocket_disconnected', 'error'
            ),
        'ignored_stream_trace_scope_client_noise',
        event_type = 'stream_trace'
            AND JSONExtractString(data, 'type') NOT IN (
                'gateway_receive_stream_request', 'gateway_ingest_stream_closed',
                'gateway_send_first_ingest_segment', 'gateway_receive_first_processed_segment',
                'gateway_receive_few_processed_segments', 'gateway_receive_first_data_segment',
                'gateway_no_orchestrators_available', 'orchestrator_swap',
                'runner_receive_first_ingest_segment', 'runner_send_first_processed_segment',
                'app_send_stream_request', 'stream_heartbeat', 'pipeline_load_start',
                'pipeline_loaded', 'session_created', 'stream_started', 'playback_ready',
                'session_closed', 'stream_stopped', 'pipeline_unloaded',
                'websocket_connected', 'websocket_disconnected', 'error'
            ),
        'unsupported_stream_trace_type',
        ''
    ) AS ignored_reason
FROM naap.events
WHERE ignored_reason != '';
