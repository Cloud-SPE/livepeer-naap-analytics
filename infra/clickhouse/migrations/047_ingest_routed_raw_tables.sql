-- Migration 047: hard-cutover raw ingest routing.
--
-- Accepted analytics traffic now lands in accepted_raw_events, while ignored
-- traffic lands in ignored_raw_events with explicit RULE-INGEST-003 reasons.
-- Downstream normalized views read only from accepted_raw_events.

CREATE TABLE IF NOT EXISTS naap.accepted_raw_events
(
    event_id         String,
    event_type       LowCardinality(String),
    event_subtype    LowCardinality(String) DEFAULT multiIf(
        event_type IN ('stream_trace', 'ai_stream_status', 'ai_stream_events'),
        JSONExtractString(data, 'type'),
        ''
    ),
    event_ts         DateTime64(3, 'UTC'),
    org              LowCardinality(String),
    gateway          String,
    data             String,
    source_topic     LowCardinality(String) DEFAULT '',
    source_partition Int32 DEFAULT 0,
    source_offset    Int64 DEFAULT 0,
    payload_hash     String DEFAULT hex(MD5(data)),
    schema_version   Nullable(String) DEFAULT nullIf(JSONExtractString(data, 'schema_version'), ''),
    ingested_at      DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = ReplacingMergeTree()
PARTITION BY (org, toYYYYMM(event_ts))
ORDER BY (org, event_type, event_ts, event_id)
TTL toDateTime(event_ts) + INTERVAL 365 DAY
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS naap.ignored_raw_events
(
    event_id         String,
    event_type       LowCardinality(String),
    event_subtype    LowCardinality(String) DEFAULT multiIf(
        event_type IN ('stream_trace', 'ai_stream_status', 'ai_stream_events'),
        JSONExtractString(data, 'type'),
        ''
    ),
    ignore_reason    LowCardinality(String),
    event_ts         DateTime64(3, 'UTC'),
    org              LowCardinality(String),
    gateway          String,
    data             String,
    source_topic     LowCardinality(String) DEFAULT '',
    source_partition Int32 DEFAULT 0,
    source_offset    Int64 DEFAULT 0,
    payload_hash     String DEFAULT hex(MD5(data)),
    schema_version   Nullable(String) DEFAULT nullIf(JSONExtractString(data, 'schema_version'), ''),
    ingested_at      DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = ReplacingMergeTree()
PARTITION BY (org, toYYYYMM(event_ts))
ORDER BY (org, ignore_reason, event_type, event_ts, event_id)
TTL toDateTime(event_ts) + INTERVAL 365 DAY
SETTINGS index_granularity = 8192;

DROP VIEW IF EXISTS naap.mv_ingest_network_events;
DROP VIEW IF EXISTS naap.mv_ingest_streaming_events;

CREATE MATERIALIZED VIEW IF NOT EXISTS naap.mv_ingest_network_events_accepted
TO naap.accepted_raw_events
AS
SELECT
    id AS event_id,
    type AS event_type,
    multiIf(
        type IN ('stream_trace', 'ai_stream_status', 'ai_stream_events'),
        JSONExtractString(data, 'type'),
        ''
    ) AS event_subtype,
    parseDateTime64BestEffort(timestamp) AS event_ts,
    'daydream' AS org,
    gateway,
    data,
    _topic AS source_topic,
    toInt32(_partition) AS source_partition,
    toInt64(_offset) AS source_offset,
    hex(MD5(data)) AS payload_hash,
    nullIf(JSONExtractString(data, 'schema_version'), '') AS schema_version,
    now64() AS ingested_at
FROM naap.kafka_network_events
WHERE id != ''
  AND type IN (
      'stream_trace', 'ai_stream_status', 'ai_stream_events',
      'stream_ingest_metrics', 'network_capabilities',
      'discovery_results', 'create_new_payment'
  )
  AND (
      type != 'stream_trace'
      OR JSONExtractString(data, 'type') IN (
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
      )
  );

CREATE MATERIALIZED VIEW IF NOT EXISTS naap.mv_ingest_network_events_ignored
TO naap.ignored_raw_events
AS
SELECT
    id AS event_id,
    type AS event_type,
    multiIf(
        type IN ('stream_trace', 'ai_stream_status', 'ai_stream_events'),
        JSONExtractString(data, 'type'),
        ''
    ) AS event_subtype,
    multiIf(
        type NOT IN (
            'stream_trace', 'ai_stream_status', 'ai_stream_events',
            'stream_ingest_metrics', 'network_capabilities',
            'discovery_results', 'create_new_payment'
        ),
        'unsupported_event_family',
        type = 'stream_trace' AND startsWith(lowerUTF8(JSONExtractString(data, 'type')), 'app_'),
        'ignored_stream_trace_non_core_app',
        type = 'stream_trace'
            AND lowerUTF8(JSONExtractString(data, 'client_source')) = 'scope'
            AND JSONExtractString(data, 'type') IN (
                'stream_heartbeat', 'pipeline_load_start', 'pipeline_loaded',
                'session_created', 'stream_started', 'playback_ready',
                'session_closed', 'stream_stopped', 'pipeline_unloaded',
                'websocket_connected', 'websocket_disconnected', 'error'
            ),
        'ignored_stream_trace_scope_client_noise',
        type = 'stream_trace',
        'unsupported_stream_trace_type',
        'malformed_or_invalid_payload'
    ) AS ignore_reason,
    parseDateTime64BestEffort(timestamp) AS event_ts,
    'daydream' AS org,
    gateway,
    data,
    _topic AS source_topic,
    toInt32(_partition) AS source_partition,
    toInt64(_offset) AS source_offset,
    hex(MD5(data)) AS payload_hash,
    nullIf(JSONExtractString(data, 'schema_version'), '') AS schema_version,
    now64() AS ingested_at
FROM naap.kafka_network_events
WHERE id != ''
  AND (
      type NOT IN (
          'stream_trace', 'ai_stream_status', 'ai_stream_events',
          'stream_ingest_metrics', 'network_capabilities',
          'discovery_results', 'create_new_payment'
      )
      OR (
          type = 'stream_trace'
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
          )
      )
  );

CREATE MATERIALIZED VIEW IF NOT EXISTS naap.mv_ingest_streaming_events_accepted
TO naap.accepted_raw_events
AS
SELECT
    id AS event_id,
    type AS event_type,
    multiIf(
        type IN ('stream_trace', 'ai_stream_status', 'ai_stream_events'),
        JSONExtractString(data, 'type'),
        ''
    ) AS event_subtype,
    parseDateTime64BestEffort(timestamp) AS event_ts,
    'cloudspe' AS org,
    gateway,
    data,
    _topic AS source_topic,
    toInt32(_partition) AS source_partition,
    toInt64(_offset) AS source_offset,
    hex(MD5(data)) AS payload_hash,
    nullIf(JSONExtractString(data, 'schema_version'), '') AS schema_version,
    now64() AS ingested_at
FROM naap.kafka_streaming_events
WHERE id != ''
  AND type IN (
      'stream_trace', 'ai_stream_status', 'ai_stream_events',
      'stream_ingest_metrics', 'network_capabilities',
      'discovery_results', 'create_new_payment'
  )
  AND (
      type != 'stream_trace'
      OR JSONExtractString(data, 'type') IN (
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
      )
  );

CREATE MATERIALIZED VIEW IF NOT EXISTS naap.mv_ingest_streaming_events_ignored
TO naap.ignored_raw_events
AS
SELECT
    id AS event_id,
    type AS event_type,
    multiIf(
        type IN ('stream_trace', 'ai_stream_status', 'ai_stream_events'),
        JSONExtractString(data, 'type'),
        ''
    ) AS event_subtype,
    multiIf(
        type NOT IN (
            'stream_trace', 'ai_stream_status', 'ai_stream_events',
            'stream_ingest_metrics', 'network_capabilities',
            'discovery_results', 'create_new_payment'
        ),
        'unsupported_event_family',
        type = 'stream_trace' AND startsWith(lowerUTF8(JSONExtractString(data, 'type')), 'app_'),
        'ignored_stream_trace_non_core_app',
        type = 'stream_trace'
            AND lowerUTF8(JSONExtractString(data, 'client_source')) = 'scope'
            AND JSONExtractString(data, 'type') IN (
                'stream_heartbeat', 'pipeline_load_start', 'pipeline_loaded',
                'session_created', 'stream_started', 'playback_ready',
                'session_closed', 'stream_stopped', 'pipeline_unloaded',
                'websocket_connected', 'websocket_disconnected', 'error'
            ),
        'ignored_stream_trace_scope_client_noise',
        type = 'stream_trace',
        'unsupported_stream_trace_type',
        'malformed_or_invalid_payload'
    ) AS ignore_reason,
    parseDateTime64BestEffort(timestamp) AS event_ts,
    'cloudspe' AS org,
    gateway,
    data,
    _topic AS source_topic,
    toInt32(_partition) AS source_partition,
    toInt64(_offset) AS source_offset,
    hex(MD5(data)) AS payload_hash,
    nullIf(JSONExtractString(data, 'schema_version'), '') AS schema_version,
    now64() AS ingested_at
FROM naap.kafka_streaming_events
WHERE id != ''
  AND (
      type NOT IN (
          'stream_trace', 'ai_stream_status', 'ai_stream_events',
          'stream_ingest_metrics', 'network_capabilities',
          'discovery_results', 'create_new_payment'
      )
      OR (
          type = 'stream_trace'
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
          )
      )
  );

DROP VIEW IF EXISTS naap.mv_normalized_stream_trace;
DROP VIEW IF EXISTS naap.mv_normalized_ai_stream_status;
DROP VIEW IF EXISTS naap.mv_normalized_ai_stream_events;
DROP VIEW IF EXISTS naap.mv_normalized_network_capabilities;

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
    event_subtype AS trace_type,
    JSONExtractString(data, 'pipeline') AS raw_pipeline_hint,
    JSONExtractString(data, 'pipeline_id') AS pipeline_id,
    lower(JSONExtractString(data, 'orchestrator_info', 'address')) AS orch_raw_address,
    JSONExtractString(data, 'orchestrator_info', 'url') AS orch_url,
    lower(JSONExtractString(data, 'orchestrator_info', 'url')) AS orch_url_norm,
    JSONExtractString(data, 'message') AS message,
    data
FROM naap.accepted_raw_events
WHERE event_type = 'stream_trace';

CREATE MATERIALIZED VIEW IF NOT EXISTS naap.mv_normalized_ai_stream_status
TO naap.normalized_ai_stream_status
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
    JSONExtractString(data, 'pipeline') AS raw_pipeline_hint,
    JSONExtractString(data, 'state') AS state,
    lower(JSONExtractString(data, 'orchestrator_info', 'address')) AS orch_raw_address,
    JSONExtractString(data, 'orchestrator_info', 'url') AS orch_url,
    lower(JSONExtractString(data, 'orchestrator_info', 'url')) AS orch_url_norm,
    JSONExtractFloat(data, 'inference_status', 'fps') AS output_fps,
    JSONExtractFloat(data, 'input_status', 'fps') AS input_fps,
    JSONExtractFloat(data, 'inference_status', 'latency_ms') AS e2e_latency_ms,
    JSONExtractUInt(data, 'inference_status', 'restart_count') AS restart_count,
    JSONExtractString(data, 'inference_status', 'last_error') AS last_error,
    coalesce(
        parseDateTime64BestEffortOrNull(JSONExtractString(data, 'inference_status', 'last_error_time')),
        toDateTime64(0, 3, 'UTC')
    ) AS last_error_ts,
    data
FROM naap.accepted_raw_events
WHERE event_type = 'ai_stream_status';

CREATE MATERIALIZED VIEW IF NOT EXISTS naap.mv_normalized_ai_stream_events
TO naap.normalized_ai_stream_events
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
    JSONExtractString(data, 'pipeline') AS raw_pipeline_hint,
    event_subtype AS event_name,
    lower(JSONExtractString(data, 'orchestrator_info', 'address')) AS orch_raw_address,
    JSONExtractString(data, 'orchestrator_info', 'url') AS orch_url,
    lower(JSONExtractString(data, 'orchestrator_info', 'url')) AS orch_url_norm,
    JSONExtractString(data, 'message') AS message,
    data
FROM naap.accepted_raw_events
WHERE event_type = 'ai_stream_events';

CREATE MATERIALIZED VIEW IF NOT EXISTS naap.mv_normalized_network_capabilities
TO naap.normalized_network_capabilities
AS
SELECT
    concat(event_id, '#', lower(JSONExtractString(orch_json, 'address'))) AS row_id,
    event_id,
    event_ts,
    org,
    lower(JSONExtractString(orch_json, 'address')) AS orch_address,
    if(
        JSONExtractString(orch_json, 'local_address') != '',
        JSONExtractString(orch_json, 'local_address'),
        JSONExtractString(orch_json, 'address')
    ) AS orch_name,
    coalesce(
        nullIf(JSONExtractString(orch_json, 'orch_uri'), ''),
        JSONExtractString(orch_json, 'uri')
    ) AS orch_uri,
    lower(coalesce(
        nullIf(JSONExtractString(orch_json, 'orch_uri'), ''),
        JSONExtractString(orch_json, 'uri')
    )) AS orch_uri_norm,
    JSONExtractString(orch_json, 'version') AS version,
    orch_json AS raw_capabilities
FROM (
    SELECT
        event_id,
        event_ts,
        org,
        arrayJoin(JSONExtractArrayRaw(data)) AS orch_json
    FROM naap.accepted_raw_events
    WHERE event_type = 'network_capabilities'
      AND data NOT IN ('', '[]', 'null')
)
WHERE orch_address != '';

DROP VIEW IF EXISTS naap.ignored_raw_event_diagnostics;

CREATE VIEW IF NOT EXISTS naap.ignored_raw_event_diagnostics AS
SELECT
    event_id,
    event_ts,
    org,
    event_type,
    event_subtype,
    ignore_reason AS ignored_reason,
    source_topic,
    source_partition,
    source_offset,
    payload_hash,
    schema_version
FROM naap.ignored_raw_events;
