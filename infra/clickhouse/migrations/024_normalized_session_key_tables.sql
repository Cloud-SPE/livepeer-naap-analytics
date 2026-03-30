-- Migration 024: session-key-oriented normalized tables
--
-- The original typed_* tables are append-friendly but are ordered by event_id,
-- which makes canonical refresh scans expensive even when the worker stages a
-- bounded session-key set. These normalized_* tables are the session- and
-- capability-oriented physical layer for canonical refresh and dbt staging.

DROP VIEW IF EXISTS naap.normalized_stream_trace;
DROP VIEW IF EXISTS naap.normalized_ai_stream_status;
DROP VIEW IF EXISTS naap.normalized_ai_stream_events;
DROP VIEW IF EXISTS naap.normalized_network_capabilities;

CREATE TABLE IF NOT EXISTS naap.normalized_stream_trace
(
    event_id               String,
    event_ts               DateTime64(3, 'UTC'),
    org                    LowCardinality(String),
    gateway                String,
    stream_id              String,
    request_id             String,
    canonical_session_key  String,
    trace_type             LowCardinality(String),
    raw_pipeline_hint      String,
    pipeline_id            String,
    orch_raw_address       String,
    orch_url               String,
    orch_url_norm          String,
    message                String,
    data                   String
)
ENGINE = ReplacingMergeTree(event_ts)
PARTITION BY (org, toYYYYMM(event_ts))
ORDER BY (org, canonical_session_key, event_ts, event_id)
SETTINGS index_granularity = 8192;

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
WHERE event_type = 'stream_trace';

CREATE TABLE IF NOT EXISTS naap.normalized_ai_stream_status
(
    event_id               String,
    event_ts               DateTime64(3, 'UTC'),
    org                    LowCardinality(String),
    gateway                String,
    stream_id              String,
    request_id             String,
    canonical_session_key  String,
    raw_pipeline_hint      String,
    state                  LowCardinality(String),
    orch_raw_address       String,
    orch_url               String,
    orch_url_norm          String,
    output_fps             Float64,
    input_fps              Float64,
    e2e_latency_ms         Float64,
    restart_count          UInt64,
    last_error             String,
    last_error_ts          DateTime64(3, 'UTC'),
    data                   String
)
ENGINE = ReplacingMergeTree(event_ts)
PARTITION BY (org, toYYYYMM(event_ts))
ORDER BY (org, canonical_session_key, event_ts, event_id)
SETTINGS index_granularity = 8192;

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
FROM naap.events
WHERE event_type = 'ai_stream_status';

CREATE TABLE IF NOT EXISTS naap.normalized_ai_stream_events
(
    event_id               String,
    event_ts               DateTime64(3, 'UTC'),
    org                    LowCardinality(String),
    gateway                String,
    stream_id              String,
    request_id             String,
    canonical_session_key  String,
    raw_pipeline_hint      String,
    event_name             LowCardinality(String),
    orch_raw_address       String,
    orch_url               String,
    orch_url_norm          String,
    message                String,
    data                   String
)
ENGINE = ReplacingMergeTree(event_ts)
PARTITION BY (org, toYYYYMM(event_ts))
ORDER BY (org, canonical_session_key, event_ts, event_id)
SETTINGS index_granularity = 8192;

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
    JSONExtractString(data, 'type') AS event_name,
    lower(JSONExtractString(data, 'orchestrator_info', 'address')) AS orch_raw_address,
    JSONExtractString(data, 'orchestrator_info', 'url') AS orch_url,
    lower(JSONExtractString(data, 'orchestrator_info', 'url')) AS orch_url_norm,
    JSONExtractString(data, 'message') AS message,
    data
FROM naap.events
WHERE event_type = 'ai_stream_events';

CREATE TABLE IF NOT EXISTS naap.normalized_network_capabilities
(
    row_id             String,
    event_id           String,
    event_ts           DateTime64(3, 'UTC'),
    org                LowCardinality(String),
    orch_address       String,
    orch_name          String,
    orch_uri           String,
    orch_uri_norm      String,
    version            String,
    raw_capabilities   String
)
ENGINE = ReplacingMergeTree(event_ts)
PARTITION BY (org, toYYYYMM(event_ts))
ORDER BY (org, orch_address, orch_uri_norm, event_ts, row_id)
SETTINGS index_granularity = 8192;

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
    FROM naap.events
    WHERE event_type = 'network_capabilities'
      AND data NOT IN ('', '[]', 'null')
)
WHERE orch_address != '';
