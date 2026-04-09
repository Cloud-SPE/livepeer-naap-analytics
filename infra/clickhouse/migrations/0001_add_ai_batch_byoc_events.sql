-- Migration 0001: Add AI Batch and BYOC event type tracking
-- Adds support for: ai_batch_request, ai_llm_request, job_gateway,
--   job_payment, job_orchestrator, job_auth, worker_lifecycle
--
-- Changes:
--   1. Expand event_subtype DEFAULT in accepted_raw_events + ignored_raw_events
--   2. Drop + recreate routing MVs (network + streaming) with expanded type list
--   3. Create 5 new normalization tables
--   4. Create 5 new normalization materialized views

-- ─── Step 1: Expand event_subtype DEFAULT ─────────────────────────────────────

ALTER TABLE naap.accepted_raw_events
  MODIFY COLUMN event_subtype LowCardinality(String) DEFAULT multiIf(
    event_type IN ('stream_trace', 'ai_stream_status', 'ai_stream_events',
                   'ai_batch_request', 'ai_llm_request', 'job_gateway',
                   'job_payment', 'job_orchestrator', 'job_auth', 'worker_lifecycle'),
      JSONExtractString(data, 'type'),
    ''
  );

ALTER TABLE naap.ignored_raw_events
  MODIFY COLUMN event_subtype LowCardinality(String) DEFAULT multiIf(
    event_type IN ('stream_trace', 'ai_stream_status', 'ai_stream_events',
                   'ai_batch_request', 'ai_llm_request', 'job_gateway',
                   'job_payment', 'job_orchestrator', 'job_auth', 'worker_lifecycle'),
      JSONExtractString(data, 'type'),
    ''
  );

-- ─── Step 2: Drop + recreate routing MVs ──────────────────────────────────────
-- ClickHouse does not support ALTER on materialized views; must drop + recreate.

DROP VIEW IF EXISTS naap.mv_ingest_network_events_accepted;
DROP VIEW IF EXISTS naap.mv_ingest_network_events_ignored;
DROP VIEW IF EXISTS naap.mv_ingest_streaming_events_accepted;
DROP VIEW IF EXISTS naap.mv_ingest_streaming_events_ignored;

-- network_events → accepted (org = 'daydream')
CREATE MATERIALIZED VIEW naap.mv_ingest_network_events_accepted
TO naap.accepted_raw_events (
  `event_id` String, `event_type` String, `event_subtype` String,
  `event_ts` DateTime64(3), `org` String, `gateway` String, `data` String,
  `source_topic` LowCardinality(String), `source_partition` Int32,
  `source_offset` Int64, `payload_hash` String,
  `schema_version` Nullable(String), `ingested_at` DateTime64(3)
) AS
WITH
  if(type IN (
    'stream_trace', 'ai_stream_status', 'ai_stream_events',
    'ai_batch_request', 'ai_llm_request', 'job_gateway',
    'job_payment', 'job_orchestrator', 'job_auth', 'worker_lifecycle'
  ), JSONExtractString(data, 'type'), '') AS derived_event_subtype,
  nullIf(JSONExtractString(data, 'schema_version'), '') AS derived_schema_version
SELECT
  id AS event_id,
  type AS event_type,
  derived_event_subtype AS event_subtype,
  parseDateTime64BestEffort(timestamp) AS event_ts,
  'daydream' AS org,
  gateway,
  data,
  _topic AS source_topic,
  toInt32(_partition) AS source_partition,
  toInt64(_offset) AS source_offset,
  hex(MD5(data)) AS payload_hash,
  derived_schema_version AS schema_version,
  now64() AS ingested_at
FROM naap.kafka_network_events
WHERE (id != '')
  AND (type IN (
    'stream_trace', 'ai_stream_status', 'ai_stream_events',
    'stream_ingest_metrics', 'network_capabilities', 'discovery_results',
    'create_new_payment',
    'ai_batch_request', 'ai_llm_request', 'job_gateway',
    'job_payment', 'job_orchestrator', 'job_auth', 'worker_lifecycle'
  ))
  AND ((type != 'stream_trace') OR (derived_event_subtype IN (
    'gateway_receive_stream_request', 'gateway_ingest_stream_closed',
    'gateway_send_first_ingest_segment', 'gateway_receive_first_processed_segment',
    'gateway_receive_few_processed_segments', 'gateway_receive_first_data_segment',
    'gateway_no_orchestrators_available', 'orchestrator_swap',
    'runner_receive_first_ingest_segment', 'runner_send_first_processed_segment'
  )));

-- network_events → ignored (org = 'daydream')
CREATE MATERIALIZED VIEW naap.mv_ingest_network_events_ignored
TO naap.ignored_raw_events (
  `event_id` String, `event_type` String, `event_subtype` String,
  `ignore_reason` String, `event_ts` DateTime64(3), `org` String,
  `gateway` String, `data` String, `source_topic` LowCardinality(String),
  `source_partition` Int32, `source_offset` Int64, `payload_hash` String,
  `schema_version` Nullable(String), `ingested_at` DateTime64(3)
) AS
WITH
  if(type IN (
    'stream_trace', 'ai_stream_status', 'ai_stream_events',
    'ai_batch_request', 'ai_llm_request', 'job_gateway',
    'job_payment', 'job_orchestrator', 'job_auth', 'worker_lifecycle'
  ), JSONExtractString(data, 'type'), '') AS derived_event_subtype,
  lowerUTF8(JSONExtractString(data, 'client_source')) AS derived_client_source,
  nullIf(JSONExtractString(data, 'schema_version'), '') AS derived_schema_version
SELECT
  id AS event_id,
  type AS event_type,
  derived_event_subtype AS event_subtype,
  multiIf(
    type NOT IN (
      'stream_trace', 'ai_stream_status', 'ai_stream_events',
      'stream_ingest_metrics', 'network_capabilities', 'discovery_results',
      'create_new_payment',
      'ai_batch_request', 'ai_llm_request', 'job_gateway',
      'job_payment', 'job_orchestrator', 'job_auth', 'worker_lifecycle'
    ), 'unsupported_event_family',
    (type = 'stream_trace') AND startsWith(lowerUTF8(derived_event_subtype), 'app_'), 'ignored_stream_trace_non_core_app',
    (type = 'stream_trace') AND (derived_client_source = 'scope') AND (derived_event_subtype IN (
      'stream_heartbeat', 'pipeline_load_start', 'pipeline_loaded',
      'session_created', 'stream_started', 'playback_ready', 'session_closed',
      'stream_stopped', 'pipeline_unloaded', 'websocket_connected',
      'websocket_disconnected', 'error'
    )), 'ignored_stream_trace_scope_client_noise',
    type = 'stream_trace', 'unsupported_stream_trace_type',
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
  derived_schema_version AS schema_version,
  now64() AS ingested_at
FROM naap.kafka_network_events
WHERE (id != '')
  AND ((type NOT IN (
    'stream_trace', 'ai_stream_status', 'ai_stream_events',
    'stream_ingest_metrics', 'network_capabilities', 'discovery_results',
    'create_new_payment',
    'ai_batch_request', 'ai_llm_request', 'job_gateway',
    'job_payment', 'job_orchestrator', 'job_auth', 'worker_lifecycle'
  )) OR (
    (type = 'stream_trace') AND (derived_event_subtype NOT IN (
      'gateway_receive_stream_request', 'gateway_ingest_stream_closed',
      'gateway_send_first_ingest_segment', 'gateway_receive_first_processed_segment',
      'gateway_receive_few_processed_segments', 'gateway_receive_first_data_segment',
      'gateway_no_orchestrators_available', 'orchestrator_swap',
      'runner_receive_first_ingest_segment', 'runner_send_first_processed_segment'
    ))
  ));

-- streaming_events → accepted (org = 'cloudspe')
CREATE MATERIALIZED VIEW naap.mv_ingest_streaming_events_accepted
TO naap.accepted_raw_events (
  `event_id` String, `event_type` String, `event_subtype` String,
  `event_ts` DateTime64(3), `org` String, `gateway` String, `data` String,
  `source_topic` LowCardinality(String), `source_partition` Int32,
  `source_offset` Int64, `payload_hash` String,
  `schema_version` Nullable(String), `ingested_at` DateTime64(3)
) AS
WITH
  if(type IN (
    'stream_trace', 'ai_stream_status', 'ai_stream_events',
    'ai_batch_request', 'ai_llm_request', 'job_gateway',
    'job_payment', 'job_orchestrator', 'job_auth', 'worker_lifecycle'
  ), JSONExtractString(data, 'type'), '') AS derived_event_subtype,
  nullIf(JSONExtractString(data, 'schema_version'), '') AS derived_schema_version
SELECT
  id AS event_id,
  type AS event_type,
  derived_event_subtype AS event_subtype,
  parseDateTime64BestEffort(timestamp) AS event_ts,
  'cloudspe' AS org,
  gateway,
  data,
  _topic AS source_topic,
  toInt32(_partition) AS source_partition,
  toInt64(_offset) AS source_offset,
  hex(MD5(data)) AS payload_hash,
  derived_schema_version AS schema_version,
  now64() AS ingested_at
FROM naap.kafka_streaming_events
WHERE (id != '')
  AND (type IN (
    'stream_trace', 'ai_stream_status', 'ai_stream_events',
    'stream_ingest_metrics', 'network_capabilities', 'discovery_results',
    'create_new_payment',
    'ai_batch_request', 'ai_llm_request', 'job_gateway',
    'job_payment', 'job_orchestrator', 'job_auth', 'worker_lifecycle'
  ))
  AND ((type != 'stream_trace') OR (derived_event_subtype IN (
    'gateway_receive_stream_request', 'gateway_ingest_stream_closed',
    'gateway_send_first_ingest_segment', 'gateway_receive_first_processed_segment',
    'gateway_receive_few_processed_segments', 'gateway_receive_first_data_segment',
    'gateway_no_orchestrators_available', 'orchestrator_swap',
    'runner_receive_first_ingest_segment', 'runner_send_first_processed_segment'
  )));

-- streaming_events → ignored (org = 'cloudspe')
CREATE MATERIALIZED VIEW naap.mv_ingest_streaming_events_ignored
TO naap.ignored_raw_events (
  `event_id` String, `event_type` String, `event_subtype` String,
  `ignore_reason` String, `event_ts` DateTime64(3), `org` String,
  `gateway` String, `data` String, `source_topic` LowCardinality(String),
  `source_partition` Int32, `source_offset` Int64, `payload_hash` String,
  `schema_version` Nullable(String), `ingested_at` DateTime64(3)
) AS
WITH
  if(type IN (
    'stream_trace', 'ai_stream_status', 'ai_stream_events',
    'ai_batch_request', 'ai_llm_request', 'job_gateway',
    'job_payment', 'job_orchestrator', 'job_auth', 'worker_lifecycle'
  ), JSONExtractString(data, 'type'), '') AS derived_event_subtype,
  lowerUTF8(JSONExtractString(data, 'client_source')) AS derived_client_source,
  nullIf(JSONExtractString(data, 'schema_version'), '') AS derived_schema_version
SELECT
  id AS event_id,
  type AS event_type,
  derived_event_subtype AS event_subtype,
  multiIf(
    type NOT IN (
      'stream_trace', 'ai_stream_status', 'ai_stream_events',
      'stream_ingest_metrics', 'network_capabilities', 'discovery_results',
      'create_new_payment',
      'ai_batch_request', 'ai_llm_request', 'job_gateway',
      'job_payment', 'job_orchestrator', 'job_auth', 'worker_lifecycle'
    ), 'unsupported_event_family',
    (type = 'stream_trace') AND startsWith(lowerUTF8(derived_event_subtype), 'app_'), 'ignored_stream_trace_non_core_app',
    (type = 'stream_trace') AND (derived_client_source = 'scope') AND (derived_event_subtype IN (
      'stream_heartbeat', 'pipeline_load_start', 'pipeline_loaded',
      'session_created', 'stream_started', 'playback_ready', 'session_closed',
      'stream_stopped', 'pipeline_unloaded', 'websocket_connected',
      'websocket_disconnected', 'error'
    )), 'ignored_stream_trace_scope_client_noise',
    type = 'stream_trace', 'unsupported_stream_trace_type',
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
  derived_schema_version AS schema_version,
  now64() AS ingested_at
FROM naap.kafka_streaming_events
WHERE (id != '')
  AND ((type NOT IN (
    'stream_trace', 'ai_stream_status', 'ai_stream_events',
    'stream_ingest_metrics', 'network_capabilities', 'discovery_results',
    'create_new_payment',
    'ai_batch_request', 'ai_llm_request', 'job_gateway',
    'job_payment', 'job_orchestrator', 'job_auth', 'worker_lifecycle'
  )) OR (
    (type = 'stream_trace') AND (derived_event_subtype NOT IN (
      'gateway_receive_stream_request', 'gateway_ingest_stream_closed',
      'gateway_send_first_ingest_segment', 'gateway_receive_first_processed_segment',
      'gateway_receive_few_processed_segments', 'gateway_receive_first_data_segment',
      'gateway_no_orchestrators_available', 'orchestrator_swap',
      'runner_receive_first_ingest_segment', 'runner_send_first_processed_segment'
    ))
  ));

-- ─── Step 3: Normalization tables ─────────────────────────────────────────────

-- AI batch job lifecycle (fixed pipelines: text-to-image, llm, audio-to-text, etc.)
CREATE TABLE naap.normalized_ai_batch_job (
  `event_id` String,
  `event_ts` DateTime64(3, 'UTC'),
  `org` LowCardinality(String),
  `gateway` String,
  `request_id` String,
  `pipeline` LowCardinality(String),
  `model_id` String,
  `subtype` LowCardinality(String),
  `success` Nullable(UInt8),
  `tries` UInt16,
  `duration_ms` Int64,
  `orch_url` String,
  `orch_url_norm` String,
  `latency_score` Float64,
  `price_per_unit` Float64,
  `error_type` String,
  `error` String
) ENGINE = ReplacingMergeTree(event_ts)
PARTITION BY (org, toYYYYMM(event_ts))
ORDER BY (org, request_id, event_ts, event_id)
TTL toDateTime(event_ts) + toIntervalDay(90)
SETTINGS index_granularity = 8192;

-- LLM-specific metrics for the 'llm' AI batch pipeline (links via request_id)
CREATE TABLE naap.normalized_ai_llm_request (
  `event_id` String,
  `event_ts` DateTime64(3, 'UTC'),
  `org` LowCardinality(String),
  `gateway` String,
  `request_id` String,
  `model` String,
  `orch_url` String,
  `orch_url_norm` String,
  `subtype` LowCardinality(String),
  `streaming` UInt8,
  `prompt_tokens` UInt32,
  `completion_tokens` UInt32,
  `total_tokens` UInt32,
  `total_duration_ms` Int64,
  `tokens_per_second` Float64,
  `latency_score` Float64,
  `price_per_unit` Float64,
  `ttft_ms` Int64,
  `finish_reason` String,
  `error` String
) ENGINE = ReplacingMergeTree(event_ts)
PARTITION BY (org, toYYYYMM(event_ts))
ORDER BY (org, request_id, event_ts, event_id)
TTL toDateTime(event_ts) + toIntervalDay(90)
SETTINGS index_granularity = 8192;

-- BYOC job lifecycle (dynamic capabilities stored verbatim)
CREATE TABLE naap.normalized_byoc_job (
  `event_id` String,
  `event_ts` DateTime64(3, 'UTC'),
  `org` LowCardinality(String),
  `gateway` String,
  `request_id` String,
  `capability` String,
  `subtype` LowCardinality(String),
  `source_event_type` LowCardinality(String),
  `success` Nullable(UInt8),
  `duration_ms` Int64,
  `http_status` UInt16,
  `orch_address` String,
  `orch_url` String,
  `orch_url_norm` String,
  `worker_url` String,
  `charged_compute` UInt8,
  `latency_ms` Int64,
  `available_capacity` Int32,
  `error` String
) ENGINE = ReplacingMergeTree(event_ts)
PARTITION BY (org, toYYYYMM(event_ts))
ORDER BY (org, capability, request_id, event_ts, event_id)
TTL toDateTime(event_ts) + toIntervalDay(90)
SETTINGS index_granularity = 8192;

-- BYOC auth events (job_auth)
CREATE TABLE naap.normalized_byoc_auth (
  `event_id` String,
  `event_ts` DateTime64(3, 'UTC'),
  `org` LowCardinality(String),
  `gateway` String,
  `request_id` String,
  `capability` String,
  `subtype` LowCardinality(String),
  `orch_address` String,
  `orch_url` String,
  `orch_url_norm` String,
  `success` UInt8,
  `error` String
) ENGINE = ReplacingMergeTree(event_ts)
PARTITION BY (org, toYYYYMM(event_ts))
ORDER BY (org, capability, request_id, event_ts, event_id)
TTL toDateTime(event_ts) + toIntervalDay(90)
SETTINGS index_granularity = 8192;

-- BYOC worker/model inventory per orchestrator (worker_lifecycle)
CREATE TABLE naap.normalized_worker_lifecycle (
  `event_id` String,
  `event_ts` DateTime64(3, 'UTC'),
  `org` LowCardinality(String),
  `gateway` String,
  `capability` String,
  `orch_address` String,
  `orch_url` String,
  `orch_url_norm` String,
  `worker_url` String,
  `price_per_unit` Float64,
  `model` String,
  `worker_options_raw` String
) ENGINE = ReplacingMergeTree(event_ts)
PARTITION BY (org, toYYYYMM(event_ts))
ORDER BY (org, capability, orch_address, event_ts, event_id)
TTL toDateTime(event_ts) + toIntervalDay(90)
SETTINGS index_granularity = 8192;

-- ─── Step 4: Normalization materialized views ──────────────────────────────────

CREATE MATERIALIZED VIEW naap.mv_normalized_ai_batch_job
TO naap.normalized_ai_batch_job (
  `event_id` String, `event_ts` DateTime64(3, 'UTC'), `org` LowCardinality(String),
  `gateway` String, `request_id` String, `pipeline` LowCardinality(String),
  `model_id` String, `subtype` LowCardinality(String), `success` Nullable(UInt8),
  `tries` UInt16, `duration_ms` Int64, `orch_url` String, `orch_url_norm` String,
  `latency_score` Float64, `price_per_unit` Float64, `error_type` String,
  `error` String
) AS
SELECT
  event_id,
  event_ts,
  org,
  gateway,
  JSONExtractString(data, 'request_id') AS request_id,
  JSONExtractString(data, 'pipeline') AS pipeline,
  JSONExtractString(data, 'model_id') AS model_id,
  event_subtype AS subtype,
  if(event_subtype = 'ai_batch_request_completed',
    toUInt8(JSONExtractBool(data, 'success')),
    CAST(NULL, 'Nullable(UInt8)')
  ) AS success,
  toUInt16(JSONExtractUInt(data, 'tries')) AS tries,
  JSONExtractInt(data, 'duration_ms') AS duration_ms,
  JSONExtractString(data, 'orch_url') AS orch_url,
  lower(JSONExtractString(data, 'orch_url')) AS orch_url_norm,
  JSONExtractFloat(data, 'latency_score') AS latency_score,
  JSONExtractFloat(data, 'price_per_unit') AS price_per_unit,
  JSONExtractString(data, 'error_type') AS error_type,
  JSONExtractString(data, 'error') AS error
FROM naap.accepted_raw_events
WHERE event_type = 'ai_batch_request';

CREATE MATERIALIZED VIEW naap.mv_normalized_ai_llm_request
TO naap.normalized_ai_llm_request (
  `event_id` String, `event_ts` DateTime64(3, 'UTC'), `org` LowCardinality(String),
  `gateway` String, `request_id` String, `model` String, `orch_url` String,
  `orch_url_norm` String, `subtype` LowCardinality(String), `streaming` UInt8,
  `prompt_tokens` UInt32, `completion_tokens` UInt32, `total_tokens` UInt32,
  `total_duration_ms` Int64, `tokens_per_second` Float64, `latency_score` Float64,
  `price_per_unit` Float64, `ttft_ms` Int64, `finish_reason` String,
  `error` String
) AS
SELECT
  event_id,
  event_ts,
  org,
  gateway,
  JSONExtractString(data, 'request_id') AS request_id,
  JSONExtractString(data, 'model') AS model,
  JSONExtractString(data, 'orch_url') AS orch_url,
  lower(JSONExtractString(data, 'orch_url')) AS orch_url_norm,
  event_subtype AS subtype,
  toUInt8(JSONExtractBool(data, 'streaming')) AS streaming,
  toUInt32(JSONExtractUInt(data, 'prompt_tokens')) AS prompt_tokens,
  toUInt32(JSONExtractUInt(data, 'completion_tokens')) AS completion_tokens,
  toUInt32(JSONExtractUInt(data, 'total_tokens')) AS total_tokens,
  JSONExtractInt(data, 'total_duration_ms') AS total_duration_ms,
  JSONExtractFloat(data, 'tokens_per_second') AS tokens_per_second,
  JSONExtractFloat(data, 'latency_score') AS latency_score,
  JSONExtractFloat(data, 'price_per_unit') AS price_per_unit,
  JSONExtractInt(data, 'ttft_ms') AS ttft_ms,
  JSONExtractString(data, 'finish_reason') AS finish_reason,
  JSONExtractString(data, 'error') AS error
FROM naap.accepted_raw_events
WHERE event_type = 'ai_llm_request'
  AND event_subtype IN ('llm_request_completed', 'llm_stream_completed');

CREATE MATERIALIZED VIEW naap.mv_normalized_byoc_job
TO naap.normalized_byoc_job (
  `event_id` String, `event_ts` DateTime64(3, 'UTC'), `org` LowCardinality(String),
  `gateway` String, `request_id` String, `capability` String,
  `subtype` LowCardinality(String), `source_event_type` LowCardinality(String),
  `success` Nullable(UInt8), `duration_ms` Int64, `http_status` UInt16,
  `orch_address` String, `orch_url` String, `orch_url_norm` String,
  `worker_url` String, `charged_compute` UInt8, `latency_ms` Int64,
  `available_capacity` Int32, `error` String
) AS
SELECT
  event_id,
  event_ts,
  org,
  gateway,
  JSONExtractString(data, 'request_id') AS request_id,
  JSONExtractString(data, 'capability') AS capability,
  event_subtype AS subtype,
  event_type AS source_event_type,
  multiIf(
    -- event_subtype = JSONExtractString(data, 'type').
    -- For job_gateway completion events, data.type = 'job_gateway_completed'
    -- (full composite form). Verified against live event payloads.
    event_subtype = 'job_gateway_completed',
      toUInt8(JSONExtractBool(data, 'success')),
    CAST(NULL, 'Nullable(UInt8)')
  ) AS success,
  JSONExtractInt(data, 'duration_ms') AS duration_ms,
  toUInt16(JSONExtractUInt(data, 'http_status')) AS http_status,
  lower(JSONExtractString(data, 'orchestrator_info', 'address')) AS orch_address,
  JSONExtractString(data, 'orchestrator_info', 'url') AS orch_url,
  lower(JSONExtractString(data, 'orchestrator_info', 'url')) AS orch_url_norm,
  JSONExtractString(data, 'worker_url') AS worker_url,
  toUInt8(JSONExtractBool(data, 'charged_compute')) AS charged_compute,
  JSONExtractInt(data, 'latency_ms') AS latency_ms,
  toInt32(JSONExtractInt(data, 'available_capacity')) AS available_capacity,
  JSONExtractString(data, 'error') AS error
FROM naap.accepted_raw_events
WHERE event_type IN ('job_gateway', 'job_orchestrator');

CREATE MATERIALIZED VIEW naap.mv_normalized_byoc_auth
TO naap.normalized_byoc_auth (
  `event_id` String, `event_ts` DateTime64(3, 'UTC'), `org` LowCardinality(String),
  `gateway` String, `request_id` String, `capability` String,
  `subtype` LowCardinality(String), `orch_address` String, `orch_url` String,
  `orch_url_norm` String, `success` UInt8, `error` String
) AS
SELECT
  event_id,
  event_ts,
  org,
  gateway,
  JSONExtractString(data, 'request_id') AS request_id,
  JSONExtractString(data, 'capability') AS capability,
  event_subtype AS subtype,
  lower(JSONExtractString(data, 'orchestrator_info', 'address')) AS orch_address,
  JSONExtractString(data, 'orchestrator_info', 'url') AS orch_url,
  lower(JSONExtractString(data, 'orchestrator_info', 'url')) AS orch_url_norm,
  toUInt8(JSONExtractBool(data, 'success')) AS success,
  JSONExtractString(data, 'error') AS error
FROM naap.accepted_raw_events
WHERE event_type = 'job_auth';

CREATE MATERIALIZED VIEW naap.mv_normalized_worker_lifecycle
TO naap.normalized_worker_lifecycle (
  `event_id` String, `event_ts` DateTime64(3, 'UTC'), `org` LowCardinality(String),
  `gateway` String, `capability` String, `orch_address` String, `orch_url` String,
  `orch_url_norm` String, `worker_url` String, `price_per_unit` Float64,
  `model` String, `worker_options_raw` String
) AS
SELECT
  event_id,
  event_ts,
  org,
  gateway,
  JSONExtractString(data, 'capability') AS capability,
  lower(JSONExtractString(data, 'orchestrator_info', 'address')) AS orch_address,
  JSONExtractString(data, 'orchestrator_info', 'url') AS orch_url,
  lower(JSONExtractString(data, 'orchestrator_info', 'url')) AS orch_url_norm,
  JSONExtractString(data, 'worker_url') AS worker_url,
  JSONExtractFloat(data, 'price_per_unit') AS price_per_unit,
  JSONExtractString(data, 'worker_options', 1, 'model') AS model,
  JSONExtractRaw(data, 'worker_options') AS worker_options_raw
FROM naap.accepted_raw_events
WHERE event_type = 'worker_lifecycle';

-- ─── Step 5: BYOC payment normalization ───────────────────────────────────────

CREATE TABLE IF NOT EXISTS naap.normalized_byoc_payment (
  `event_id`     String,
  `event_ts`     DateTime64(3, 'UTC'),
  `org`          LowCardinality(String),
  `gateway`      String,
  `request_id`   String,
  `capability`   String,
  `orch_address` String,
  `amount`       Float64,
  `currency`     LowCardinality(String),
  `payment_type` LowCardinality(String)
) ENGINE = ReplacingMergeTree(event_ts)
PARTITION BY (org, toYYYYMM(event_ts))
ORDER BY (org, capability, orch_address, event_ts, event_id)
TTL toDateTime(event_ts) + toIntervalDay(90)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW naap.mv_normalized_byoc_payment
TO naap.normalized_byoc_payment (
  `event_id` String, `event_ts` DateTime64(3, 'UTC'), `org` LowCardinality(String),
  `gateway` String, `request_id` String, `capability` String,
  `orch_address` String, `amount` Float64, `currency` LowCardinality(String),
  `payment_type` LowCardinality(String)
) AS
SELECT
  event_id,
  event_ts,
  org,
  gateway,
  JSONExtractString(data, 'request_id')                        AS request_id,
  JSONExtractString(data, 'capability')                        AS capability,
  lower(JSONExtractString(data, 'orchestrator_info', 'address')) AS orch_address,
  JSONExtractFloat(data, 'amount')                             AS amount,
  JSONExtractString(data, 'currency')                          AS currency,
  event_subtype                                                AS payment_type
FROM naap.accepted_raw_events
WHERE event_type = 'job_payment';
