-- Migration 031: attribution input/current helpers for lean canonical refresh.
--
-- Canonical attribution refresh should not reconstruct session evidence from
-- multiple rollups on every chunk. This migration adds a session-grain
-- attribution input store, a hot current-attribution table, and narrow helper
-- views for historical snapshot matching and snapshot/model lookup.

CREATE TABLE IF NOT EXISTS naap.normalized_session_attribution_input_latest_store
(
    org                          LowCardinality(String),
    canonical_session_key        String,
    status_pipeline_state        AggregateFunction(argMaxIf, String, DateTime64(3, 'UTC'), UInt8),
    event_pipeline_hint_state    AggregateFunction(argMaxIf, String, DateTime64(3, 'UTC'), UInt8),
    trace_pipeline_id_state      AggregateFunction(argMaxIf, String, DateTime64(3, 'UTC'), UInt8),
    trace_raw_pipeline_hint_state AggregateFunction(argMaxIf, String, DateTime64(3, 'UTC'), UInt8),
    observed_orch_address_state  AggregateFunction(argMaxIf, String, DateTime64(3, 'UTC'), UInt8),
    observed_orch_url_state      AggregateFunction(argMaxIf, String, DateTime64(3, 'UTC'), UInt8),
    observed_orch_address_uniq   AggregateFunction(uniqExactIf, String, UInt8),
    observed_orch_url_uniq       AggregateFunction(uniqExactIf, String, UInt8),
    last_seen_state              AggregateFunction(max, DateTime64(3, 'UTC')),
    swap_count_state             AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
PARTITION BY org
ORDER BY (org, canonical_session_key)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS naap.mv_normalized_session_attribution_input_from_trace
TO naap.normalized_session_attribution_input_latest_store
AS
SELECT
    org,
    canonical_session_key,
    argMaxIfState(cast('', 'String'), event_ts, toUInt8(0)) AS status_pipeline_state,
    argMaxIfState(cast('', 'String'), event_ts, toUInt8(0)) AS event_pipeline_hint_state,
    argMaxIfState(pipeline_id, event_ts, toUInt8(pipeline_id != '')) AS trace_pipeline_id_state,
    argMaxIfState(raw_pipeline_hint, event_ts, toUInt8(raw_pipeline_hint != '')) AS trace_raw_pipeline_hint_state,
    argMaxIfState(orch_raw_address, event_ts, toUInt8(orch_raw_address != '')) AS observed_orch_address_state,
    argMaxIfState(orch_url, event_ts, toUInt8(orch_url != '')) AS observed_orch_url_state,
    uniqExactIfState(orch_raw_address, toUInt8(orch_raw_address != '')) AS observed_orch_address_uniq,
    uniqExactIfState(lower(orch_url), toUInt8(orch_url != '')) AS observed_orch_url_uniq,
    maxState(event_ts) AS last_seen_state,
    sumState(toUInt64(trace_type = 'orchestrator_swap')) AS swap_count_state
FROM naap.normalized_stream_trace
WHERE canonical_session_key != ''
GROUP BY org, canonical_session_key;

CREATE MATERIALIZED VIEW IF NOT EXISTS naap.mv_normalized_session_attribution_input_from_status
TO naap.normalized_session_attribution_input_latest_store
AS
SELECT
    org,
    canonical_session_key,
    argMaxIfState(raw_pipeline_hint, event_ts, toUInt8(raw_pipeline_hint != '')) AS status_pipeline_state,
    argMaxIfState(cast('', 'String'), event_ts, toUInt8(0)) AS event_pipeline_hint_state,
    argMaxIfState(cast('', 'String'), event_ts, toUInt8(0)) AS trace_pipeline_id_state,
    argMaxIfState(cast('', 'String'), event_ts, toUInt8(0)) AS trace_raw_pipeline_hint_state,
    argMaxIfState(orch_raw_address, event_ts, toUInt8(orch_raw_address != '')) AS observed_orch_address_state,
    argMaxIfState(orch_url, event_ts, toUInt8(orch_url != '')) AS observed_orch_url_state,
    uniqExactIfState(orch_raw_address, toUInt8(orch_raw_address != '')) AS observed_orch_address_uniq,
    uniqExactIfState(lower(orch_url), toUInt8(orch_url != '')) AS observed_orch_url_uniq,
    maxState(event_ts) AS last_seen_state,
    sumState(toUInt64(0)) AS swap_count_state
FROM naap.normalized_ai_stream_status
WHERE canonical_session_key != ''
GROUP BY org, canonical_session_key;

CREATE MATERIALIZED VIEW IF NOT EXISTS naap.mv_normalized_session_attribution_input_from_events
TO naap.normalized_session_attribution_input_latest_store
AS
SELECT
    org,
    canonical_session_key,
    argMaxIfState(cast('', 'String'), event_ts, toUInt8(0)) AS status_pipeline_state,
    argMaxIfState(raw_pipeline_hint, event_ts, toUInt8(raw_pipeline_hint != '')) AS event_pipeline_hint_state,
    argMaxIfState(cast('', 'String'), event_ts, toUInt8(0)) AS trace_pipeline_id_state,
    argMaxIfState(cast('', 'String'), event_ts, toUInt8(0)) AS trace_raw_pipeline_hint_state,
    argMaxIfState(orch_raw_address, event_ts, toUInt8(orch_raw_address != '')) AS observed_orch_address_state,
    argMaxIfState(orch_url, event_ts, toUInt8(orch_url != '')) AS observed_orch_url_state,
    uniqExactIfState(orch_raw_address, toUInt8(orch_raw_address != '')) AS observed_orch_address_uniq,
    uniqExactIfState(lower(orch_url), toUInt8(orch_url != '')) AS observed_orch_url_uniq,
    maxState(event_ts) AS last_seen_state,
    sumState(toUInt64(0)) AS swap_count_state
FROM naap.normalized_ai_stream_events
WHERE canonical_session_key != ''
GROUP BY org, canonical_session_key;

CREATE VIEW IF NOT EXISTS naap.normalized_session_attribution_input_latest AS
SELECT
    canonical_session_key,
    org,
    canonical_pipeline,
    observed_orch_address,
    observed_orch_url,
    lower(ifNull(observed_orch_url, '')) AS observed_orch_url_norm,
    observed_orch_address_count,
    observed_orch_url_count,
    last_seen,
    swap_count,
    hex(MD5(concat(
        canonical_session_key, '|',
        org, '|',
        ifNull(canonical_pipeline, ''), '|',
        ifNull(observed_orch_address, ''), '|',
        ifNull(observed_orch_url, ''), '|',
        toString(observed_orch_address_count), '|',
        toString(observed_orch_url_count), '|',
        toString(toUnixTimestamp64Milli(last_seen)), '|',
        toString(swap_count)
    ))) AS attribution_input_hash
FROM (
    SELECT
        canonical_session_key,
        org,
        coalesce(
            nullIf(argMaxIfMerge(status_pipeline_state), ''),
            nullIf(argMaxIfMerge(event_pipeline_hint_state), ''),
            nullIf(argMaxIfMerge(trace_pipeline_id_state), ''),
            nullIf(argMaxIfMerge(trace_raw_pipeline_hint_state), '')
        ) AS canonical_pipeline,
        nullIf(argMaxIfMerge(observed_orch_address_state), '') AS observed_orch_address,
        nullIf(argMaxIfMerge(observed_orch_url_state), '') AS observed_orch_url,
        toUInt64(uniqExactIfMerge(observed_orch_address_uniq)) AS observed_orch_address_count,
        toUInt64(uniqExactIfMerge(observed_orch_url_uniq)) AS observed_orch_url_count,
        maxMerge(last_seen_state) AS last_seen,
        toUInt64(sumMerge(swap_count_state)) AS swap_count
    FROM naap.normalized_session_attribution_input_latest_store
    GROUP BY org, canonical_session_key
);

CREATE VIEW IF NOT EXISTS naap.canonical_capability_snapshot_history_by_uri AS
SELECT
    snapshot_row_id,
    snapshot_ts,
    org,
    orch_address,
    orch_uri,
    orch_uri_norm
FROM naap.canonical_capability_snapshots_by_uri;

CREATE VIEW IF NOT EXISTS naap.canonical_capability_snapshot_history_by_address AS
SELECT
    snapshot_row_id,
    snapshot_ts,
    org,
    orch_address,
    orch_uri,
    orch_address AS orch_identity
FROM naap.canonical_capability_snapshots_by_address;

CREATE VIEW IF NOT EXISTS naap.canonical_capability_snapshot_model_summary AS
SELECT
    snapshot_row_id,
    countIf(pipeline_id != '') AS hardware_row_count,
    uniqExactIf(model_id, model_id != '') AS matched_model_count,
    argMaxIf(model_id, snapshot_ts, model_id != '') AS latest_model_id
FROM naap.canonical_capability_hardware_inventory_by_snapshot
GROUP BY snapshot_row_id;

CREATE VIEW IF NOT EXISTS naap.canonical_capability_pipeline_model_lookup AS
WITH pipeline_rows AS (
    SELECT
        snapshot_row_id,
        pipeline_id,
        countIf(model_id != '') AS pipeline_hardware_row_count,
        argMaxIf(model_id, snapshot_ts, model_id != '') AS pipeline_model_id
    FROM naap.canonical_capability_hardware_inventory_by_snapshot
    GROUP BY snapshot_row_id, pipeline_id
),
snapshot_summary AS (
    SELECT
        snapshot_row_id,
        countIf(pipeline_id != '') AS hardware_row_count,
        uniqExactIf(model_id, model_id != '') AS matched_model_count,
        argMaxIf(model_id, snapshot_ts, model_id != '') AS latest_model_id
    FROM naap.canonical_capability_hardware_inventory_by_snapshot
    GROUP BY snapshot_row_id
)
SELECT
    p.snapshot_row_id,
    p.pipeline_id,
    s.hardware_row_count,
    s.matched_model_count,
    p.pipeline_model_id,
    s.latest_model_id
FROM pipeline_rows p
LEFT JOIN snapshot_summary s USING (snapshot_row_id);

CREATE TABLE IF NOT EXISTS naap.canonical_session_attribution_current
(
    canonical_session_key         String,
    org                           LowCardinality(String),
    decision_anchor_ts            DateTime64(3, 'UTC'),
    canonical_model               Nullable(String),
    has_ambiguous_identity        UInt8,
    has_snapshot_match            UInt8,
    is_hardware_less              UInt8,
    is_stale                      UInt8,
    attribution_reason            LowCardinality(String),
    attribution_status            LowCardinality(String),
    attributed_orch_address       Nullable(String),
    attributed_orch_uri           Nullable(String),
    attribution_snapshot_row_id   Nullable(String),
    attribution_snapshot_ts       Nullable(DateTime64(3, 'UTC')),
    attribution_method            LowCardinality(String),
    selection_confidence          LowCardinality(String),
    attribution_input_hash        String DEFAULT '',
    refresh_run_id                String,
    artifact_checksum             String DEFAULT '',
    decided_at                    DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = ReplacingMergeTree(decided_at)
PARTITION BY org
ORDER BY (org, canonical_session_key)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS naap.canonical_refresh_changed_sessions
(
    run_id                String,
    canonical_session_key String,
    inserted_at           DateTime('UTC') DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(inserted_at)
ORDER BY (run_id, canonical_session_key)
TTL inserted_at + INTERVAL 1 DAY DELETE
SETTINGS index_granularity = 8192;
