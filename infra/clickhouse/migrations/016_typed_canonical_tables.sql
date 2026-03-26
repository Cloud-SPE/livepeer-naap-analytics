-- Migration 016: typed canonical source tables
--
-- These tables are the only ClickHouse-owned layer downstream of raw events.
-- They normalize raw JSON once while preserving lineage. dbt owns all
-- semantics downstream of these tables.

CREATE TABLE IF NOT EXISTS naap.typed_stream_trace
(
    event_id           String,
    event_ts           DateTime64(3, 'UTC'),
    org                LowCardinality(String),
    gateway            String,
    stream_id          String,
    request_id         String,
    trace_type         LowCardinality(String),
    raw_pipeline_hint  String,
    pipeline_id        String,
    orch_raw_address   String,
    orch_url           String,
    message            String,
    data               String
)
ENGINE = ReplacingMergeTree(event_ts)
PARTITION BY (org, toYYYYMM(event_ts))
PRIMARY KEY (org, event_ts)
ORDER BY (org, event_ts, event_id)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS naap.mv_typed_stream_trace
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
WHERE event_type = 'stream_trace';

CREATE TABLE IF NOT EXISTS naap.typed_ai_stream_status
(
    event_id           String,
    event_ts           DateTime64(3, 'UTC'),
    org                LowCardinality(String),
    gateway            String,
    stream_id          String,
    request_id         String,
    raw_pipeline_hint  String,
    state              LowCardinality(String),
    orch_raw_address   String,
    orch_url           String,
    output_fps         Float64,
    input_fps          Float64,
    e2e_latency_ms     Float64,
    restart_count      UInt64,
    last_error         String,
    last_error_ts      DateTime64(3, 'UTC'),
    data               String
)
ENGINE = ReplacingMergeTree(event_ts)
PARTITION BY (org, toYYYYMM(event_ts))
PRIMARY KEY (org, event_ts)
ORDER BY (org, event_ts, event_id)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS naap.mv_typed_ai_stream_status
TO naap.typed_ai_stream_status
AS
SELECT
    event_id,
    event_ts,
    org,
    gateway,
    JSONExtractString(data, 'stream_id')                            AS stream_id,
    JSONExtractString(data, 'request_id')                           AS request_id,
    JSONExtractString(data, 'pipeline')                             AS raw_pipeline_hint,
    JSONExtractString(data, 'state')                                AS state,
    lower(JSONExtractString(data, 'orchestrator_info', 'address'))  AS orch_raw_address,
    JSONExtractString(data, 'orchestrator_info', 'url')             AS orch_url,
    JSONExtractFloat(data, 'inference_status', 'fps')               AS output_fps,
    JSONExtractFloat(data, 'input_status', 'fps')                   AS input_fps,
    JSONExtractFloat(data, 'inference_status', 'latency_ms')        AS e2e_latency_ms,
    JSONExtractUInt(data, 'inference_status', 'restart_count')      AS restart_count,
    JSONExtractString(data, 'inference_status', 'last_error')       AS last_error,
    parseDateTime64BestEffortOrNull(
        JSONExtractString(data, 'inference_status', 'last_error_time')
    )                                                               AS last_error_ts,
    data
FROM naap.events
WHERE event_type = 'ai_stream_status';

CREATE TABLE IF NOT EXISTS naap.typed_ai_stream_events
(
    event_id           String,
    event_ts           DateTime64(3, 'UTC'),
    org                LowCardinality(String),
    gateway            String,
    stream_id          String,
    request_id         String,
    raw_pipeline_hint  String,
    event_name         LowCardinality(String),
    orch_raw_address   String,
    orch_url           String,
    message            String,
    data               String
)
ENGINE = ReplacingMergeTree(event_ts)
PARTITION BY (org, toYYYYMM(event_ts))
PRIMARY KEY (org, event_ts)
ORDER BY (org, event_ts, event_id)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS naap.mv_typed_ai_stream_events
TO naap.typed_ai_stream_events
AS
SELECT
    event_id,
    event_ts,
    org,
    gateway,
    JSONExtractString(data, 'stream_id')                            AS stream_id,
    JSONExtractString(data, 'request_id')                           AS request_id,
    JSONExtractString(data, 'pipeline')                             AS raw_pipeline_hint,
    JSONExtractString(data, 'type')                                 AS event_name,
    lower(JSONExtractString(data, 'orchestrator_info', 'address'))  AS orch_raw_address,
    JSONExtractString(data, 'orchestrator_info', 'url')             AS orch_url,
    JSONExtractString(data, 'message')                              AS message,
    data
FROM naap.events
WHERE event_type = 'ai_stream_events';

CREATE TABLE IF NOT EXISTS naap.typed_discovery_results
(
    row_id       String,
    event_id     String,
    event_ts     DateTime64(3, 'UTC'),
    org          LowCardinality(String),
    gateway      String,
    orch_address String,
    orch_url     String,
    latency_ms   UInt64,
    data         String
)
ENGINE = ReplacingMergeTree(event_ts)
PARTITION BY (org, toYYYYMM(event_ts))
PRIMARY KEY (org, event_ts)
ORDER BY (org, event_ts, row_id)
SETTINGS index_granularity = 8192;

-- Single arrayJoin on a zipped (index, value) tuple avoids the N² cross-product
-- that two independent arrayJoin calls on the same array would produce.
CREATE MATERIALIZED VIEW IF NOT EXISTS naap.mv_typed_discovery_results
TO naap.typed_discovery_results
AS
SELECT
    concat(event_id, '#', lower(JSONExtractString(pair.2, 'address')), '#', toString(pair.1)) AS row_id,
    event_id,
    event_ts,
    org,
    gateway,
    lower(JSONExtractString(pair.2, 'address')) AS orch_address,
    JSONExtractString(pair.2, 'url')            AS orch_url,
    toUInt64OrDefault(JSONExtractString(pair.2, 'latency_ms')) AS latency_ms,
    pair.2                                      AS data
FROM (
    SELECT
        event_id,
        event_ts,
        org,
        gateway,
        arrayJoin(
            arrayMap((x, i) -> tuple(i, x),
                JSONExtractArrayRaw(data),
                arrayEnumerate(JSONExtractArrayRaw(data)))
        ) AS pair
    FROM naap.events
    WHERE event_type = 'discovery_results'
      AND data NOT IN ('', '[]', 'null')
)
WHERE lower(JSONExtractString(pair.2, 'address')) != '';

CREATE TABLE IF NOT EXISTS naap.typed_stream_ingest_metrics
(
    event_id                String,
    event_ts                DateTime64(3, 'UTC'),
    org                     LowCardinality(String),
    gateway                 String,
    stream_id               String,
    request_id              String,
    conn_quality            LowCardinality(String),
    video_jitter_ms         Float64,
    video_packets_lost      UInt64,
    video_packets_received  UInt64,
    audio_jitter_ms         Float64,
    audio_packets_lost      UInt64,
    audio_packets_received  UInt64,
    data                    String
)
ENGINE = ReplacingMergeTree(event_ts)
PARTITION BY (org, toYYYYMM(event_ts))
PRIMARY KEY (org, event_ts)
ORDER BY (org, event_ts, event_id)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS naap.mv_typed_stream_ingest_metrics
TO naap.typed_stream_ingest_metrics
AS
WITH
    JSONExtractArrayRaw(data, 'stats', 'track_stats') AS tracks,
    arrayFirst(x -> JSONExtractString(x, 'type') = 'video', tracks) AS video_track,
    arrayFirst(x -> JSONExtractString(x, 'type') = 'audio', tracks) AS audio_track
SELECT
    event_id,
    event_ts,
    org,
    gateway,
    JSONExtractString(data, 'stream_id')                                                        AS stream_id,
    JSONExtractString(data, 'request_id')                                                       AS request_id,
    JSONExtractString(data, 'stats', 'conn_quality')                                            AS conn_quality,
    if(video_track != '', JSONExtractFloat(video_track, 'jitter'), 0.0)                         AS video_jitter_ms,
    if(video_track != '', toUInt64OrDefault(toString(JSONExtractUInt(video_track, 'packets_lost'))), 0)      AS video_packets_lost,
    if(video_track != '', toUInt64OrDefault(toString(JSONExtractUInt(video_track, 'packets_received'))), 0)  AS video_packets_received,
    if(audio_track != '', JSONExtractFloat(audio_track, 'jitter'), 0.0)                         AS audio_jitter_ms,
    if(audio_track != '', toUInt64OrDefault(toString(JSONExtractUInt(audio_track, 'packets_lost'))), 0)      AS audio_packets_lost,
    if(audio_track != '', toUInt64OrDefault(toString(JSONExtractUInt(audio_track, 'packets_received'))), 0)  AS audio_packets_received,
    data
FROM naap.events
WHERE event_type = 'stream_ingest_metrics';

CREATE TABLE IF NOT EXISTS naap.typed_network_capabilities
(
    row_id            String,
    event_id          String,
    event_ts          DateTime64(3, 'UTC'),
    org               LowCardinality(String),
    orch_address      String,
    orch_name         String,
    orch_uri          String,
    version           String,
    raw_capabilities  String
)
ENGINE = ReplacingMergeTree(event_ts)
PARTITION BY (org, toYYYYMM(event_ts))
PRIMARY KEY (org, event_ts)
ORDER BY (org, event_ts, row_id)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS naap.mv_typed_network_capabilities
TO naap.typed_network_capabilities
AS
SELECT
    concat(event_id, '#', lower(JSONExtractString(orch_json, 'address'))) AS row_id,
    event_id,
    event_ts,
    org,
    lower(JSONExtractString(orch_json, 'address'))                        AS orch_address,
    if(
        JSONExtractString(orch_json, 'local_address') != '',
        JSONExtractString(orch_json, 'local_address'),
        JSONExtractString(orch_json, 'address')
    )                                                                     AS orch_name,
    coalesce(
        nullIf(JSONExtractString(orch_json, 'orch_uri'), ''),
        JSONExtractString(orch_json, 'uri')
    )                                                                     AS orch_uri,
    JSONExtractString(orch_json, 'version')                               AS version,
    orch_json                                                             AS raw_capabilities
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

CREATE TABLE IF NOT EXISTS naap.typed_payments
(
    event_id              String,
    event_ts              DateTime64(3, 'UTC'),
    org                   LowCardinality(String),
    gateway               String,
    session_id            String,
    request_id            String,
    manifest_id           String,
    pipeline_hint         String,
    sender_address        String,
    recipient_address     String,
    orchestrator_url      String,
    face_value_wei        UInt64,
    price_wei_per_pixel   Float64,
    win_prob              Float64,
    num_tickets           UInt64,
    data                  String
)
ENGINE = ReplacingMergeTree(event_ts)
PARTITION BY (org, toYYYYMM(event_ts))
PRIMARY KEY (org, event_ts)
ORDER BY (org, event_ts, event_id)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS naap.mv_typed_payments
TO naap.typed_payments
AS
SELECT
    event_id,
    event_ts,
    org,
    gateway,
    JSONExtractString(data, 'sessionID')                                                       AS session_id,
    JSONExtractString(data, 'requestID')                                                       AS request_id,
    JSONExtractString(data, 'manifestID')                                                      AS manifest_id,
    replaceRegexpOne(JSONExtractString(data, 'manifestID'), '^[0-9]+_', '')                    AS pipeline_hint,
    lower(JSONExtractString(data, 'sender'))                                                   AS sender_address,
    lower(JSONExtractString(data, 'recipient'))                                                AS recipient_address,
    JSONExtractString(data, 'orchestrator')                                                    AS orchestrator_url,
    toUInt64OrDefault(trimRight(replaceAll(JSONExtractString(data, 'faceValue'), ' WEI', ''))) AS face_value_wei,
    toFloat64OrDefault(replaceRegexpOne(JSONExtractString(data, 'price'), ' wei/pixel$', ''))  AS price_wei_per_pixel,
    toFloat64OrDefault(JSONExtractString(data, 'winProb'))                                     AS win_prob,
    toUInt64OrDefault(JSONExtractString(data, 'numTickets'))                                   AS num_tickets,
    data
FROM naap.events
WHERE event_type = 'create_new_payment';
