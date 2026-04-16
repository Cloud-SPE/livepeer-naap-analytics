-- Migration 008: remove API-side GPU inventory enrichment and rebuild
-- canonical hardware inventory from normalized network capabilities.

DROP VIEW IF EXISTS naap.mv_canonical_capability_hardware_inventory;
DROP VIEW IF EXISTS naap.mv_canonical_capability_hardware_inventory_by_snapshot;

DROP TABLE IF EXISTS naap.agg_gpu_inventory;

CREATE MATERIALIZED VIEW naap.mv_canonical_capability_hardware_inventory
TO naap.canonical_capability_hardware_inventory
(
    `snapshot_row_id` String,
    `snapshot_ts` DateTime64(3, 'UTC'),
    `org` LowCardinality(String),
    `orch_address` String,
    `orch_uri_norm` String,
    `pipeline_id` String,
    `model_id` String,
    `gpu_id` String,
    `gpu_model_name` Nullable(String),
    `gpu_memory_bytes_total` Nullable(UInt64),
    `runner_version` Nullable(String),
    `cuda_version` Nullable(String)
)
AS
SELECT
    row_id AS snapshot_row_id,
    event_ts AS snapshot_ts,
    org,
    orch_address,
    orch_uri_norm,
    pipeline_id,
    model_id,
    JSONExtractString(gpu_json, 'id') AS gpu_id,
    nullIf(JSONExtractString(gpu_json, 'name'), '') AS gpu_model_name,
    nullIf(JSONExtractUInt(gpu_json, 'memory_total'), 0) AS gpu_memory_bytes_total,
    CAST(NULL, 'Nullable(String)') AS runner_version,
    CAST(NULL, 'Nullable(String)') AS cuda_version
FROM
(
    SELECT
        row_id,
        event_ts,
        org,
        orch_address,
        orch_uri_norm,
        JSONExtractString(hardware_json, 'pipeline') AS pipeline_id,
        JSONExtractString(hardware_json, 'model_id') AS model_id,
        arrayJoin(
            if(
                gpu_info_raw IN ('', 'null', '{}', '[]'),
                CAST([], 'Array(String)'),
                if(
                    startsWith(gpu_info_raw, '['),
                    JSONExtractArrayRaw(gpu_info_raw),
                    tupleElement(JSONExtractKeysAndValuesRaw(gpu_info_raw), 2)
                )
            )
        ) AS gpu_json
    FROM
    (
        SELECT
            row_id,
            event_ts,
            org,
            orch_address,
            orch_uri_norm,
            hardware_json,
            JSONExtractRaw(hardware_json, 'gpu_info') AS gpu_info_raw
        FROM
        (
            SELECT
                row_id,
                event_ts,
                org,
                orch_address,
                orch_uri_norm,
                arrayJoin(JSONExtractArrayRaw(raw_capabilities, 'hardware')) AS hardware_json
            FROM naap.normalized_network_capabilities
            WHERE length(JSONExtractArrayRaw(raw_capabilities, 'hardware')) > 0
        )
    )
)
WHERE pipeline_id != ''
  AND JSONExtractString(gpu_json, 'id') != '';

CREATE MATERIALIZED VIEW naap.mv_canonical_capability_hardware_inventory_by_snapshot
TO naap.canonical_capability_hardware_inventory_by_snapshot
(
    `snapshot_row_id` String,
    `snapshot_ts` DateTime64(3, 'UTC'),
    `org` LowCardinality(String),
    `orch_address` String,
    `orch_uri_norm` String,
    `pipeline_id` String,
    `model_id` String,
    `gpu_id` String,
    `gpu_model_name` Nullable(String),
    `gpu_memory_bytes_total` Nullable(UInt64),
    `runner_version` Nullable(String),
    `cuda_version` Nullable(String)
)
AS
SELECT
    row_id AS snapshot_row_id,
    event_ts AS snapshot_ts,
    org,
    orch_address,
    orch_uri_norm,
    pipeline_id,
    model_id,
    JSONExtractString(gpu_json, 'id') AS gpu_id,
    nullIf(JSONExtractString(gpu_json, 'name'), '') AS gpu_model_name,
    nullIf(JSONExtractUInt(gpu_json, 'memory_total'), 0) AS gpu_memory_bytes_total,
    CAST(NULL, 'Nullable(String)') AS runner_version,
    CAST(NULL, 'Nullable(String)') AS cuda_version
FROM
(
    SELECT
        row_id,
        event_ts,
        org,
        orch_address,
        orch_uri_norm,
        JSONExtractString(hardware_json, 'pipeline') AS pipeline_id,
        JSONExtractString(hardware_json, 'model_id') AS model_id,
        arrayJoin(
            if(
                gpu_info_raw IN ('', 'null', '{}', '[]'),
                CAST([], 'Array(String)'),
                if(
                    startsWith(gpu_info_raw, '['),
                    JSONExtractArrayRaw(gpu_info_raw),
                    tupleElement(JSONExtractKeysAndValuesRaw(gpu_info_raw), 2)
                )
            )
        ) AS gpu_json
    FROM
    (
        SELECT
            row_id,
            event_ts,
            org,
            orch_address,
            orch_uri_norm,
            hardware_json,
            JSONExtractRaw(hardware_json, 'gpu_info') AS gpu_info_raw
        FROM
        (
            SELECT
                row_id,
                event_ts,
                org,
                orch_address,
                orch_uri_norm,
                arrayJoin(JSONExtractArrayRaw(raw_capabilities, 'hardware')) AS hardware_json
            FROM naap.normalized_network_capabilities
            WHERE length(JSONExtractArrayRaw(raw_capabilities, 'hardware')) > 0
        )
    )
)
WHERE pipeline_id != ''
  AND JSONExtractString(gpu_json, 'id') != '';

TRUNCATE TABLE naap.canonical_capability_hardware_inventory;
TRUNCATE TABLE naap.canonical_capability_hardware_inventory_by_snapshot;
TRUNCATE TABLE IF EXISTS naap.canonical_latest_orchestrator_pipeline_inventory_agg;

INSERT INTO naap.canonical_capability_hardware_inventory
SELECT
    row_id AS snapshot_row_id,
    event_ts AS snapshot_ts,
    org,
    orch_address,
    orch_uri_norm,
    pipeline_id,
    model_id,
    JSONExtractString(gpu_json, 'id') AS gpu_id,
    nullIf(JSONExtractString(gpu_json, 'name'), '') AS gpu_model_name,
    nullIf(JSONExtractUInt(gpu_json, 'memory_total'), 0) AS gpu_memory_bytes_total,
    CAST(NULL, 'Nullable(String)') AS runner_version,
    CAST(NULL, 'Nullable(String)') AS cuda_version
FROM
(
    SELECT
        row_id,
        event_ts,
        org,
        orch_address,
        orch_uri_norm,
        JSONExtractString(hardware_json, 'pipeline') AS pipeline_id,
        JSONExtractString(hardware_json, 'model_id') AS model_id,
        arrayJoin(
            if(
                gpu_info_raw IN ('', 'null', '{}', '[]'),
                CAST([], 'Array(String)'),
                if(
                    startsWith(gpu_info_raw, '['),
                    JSONExtractArrayRaw(gpu_info_raw),
                    tupleElement(JSONExtractKeysAndValuesRaw(gpu_info_raw), 2)
                )
            )
        ) AS gpu_json
    FROM
    (
        SELECT
            row_id,
            event_ts,
            org,
            orch_address,
            orch_uri_norm,
            hardware_json,
            JSONExtractRaw(hardware_json, 'gpu_info') AS gpu_info_raw
        FROM
        (
            SELECT
                row_id,
                event_ts,
                org,
                orch_address,
                orch_uri_norm,
                arrayJoin(JSONExtractArrayRaw(raw_capabilities, 'hardware')) AS hardware_json
            FROM naap.normalized_network_capabilities
            WHERE length(JSONExtractArrayRaw(raw_capabilities, 'hardware')) > 0
        )
    )
)
WHERE pipeline_id != ''
  AND JSONExtractString(gpu_json, 'id') != '';

INSERT INTO naap.canonical_capability_hardware_inventory_by_snapshot
SELECT
    row_id AS snapshot_row_id,
    event_ts AS snapshot_ts,
    org,
    orch_address,
    orch_uri_norm,
    pipeline_id,
    model_id,
    JSONExtractString(gpu_json, 'id') AS gpu_id,
    nullIf(JSONExtractString(gpu_json, 'name'), '') AS gpu_model_name,
    nullIf(JSONExtractUInt(gpu_json, 'memory_total'), 0) AS gpu_memory_bytes_total,
    CAST(NULL, 'Nullable(String)') AS runner_version,
    CAST(NULL, 'Nullable(String)') AS cuda_version
FROM
(
    SELECT
        row_id,
        event_ts,
        org,
        orch_address,
        orch_uri_norm,
        JSONExtractString(hardware_json, 'pipeline') AS pipeline_id,
        JSONExtractString(hardware_json, 'model_id') AS model_id,
        arrayJoin(
            if(
                gpu_info_raw IN ('', 'null', '{}', '[]'),
                CAST([], 'Array(String)'),
                if(
                    startsWith(gpu_info_raw, '['),
                    JSONExtractArrayRaw(gpu_info_raw),
                    tupleElement(JSONExtractKeysAndValuesRaw(gpu_info_raw), 2)
                )
            )
        ) AS gpu_json
    FROM
    (
        SELECT
            row_id,
            event_ts,
            org,
            orch_address,
            orch_uri_norm,
            hardware_json,
            JSONExtractRaw(hardware_json, 'gpu_info') AS gpu_info_raw
        FROM
        (
            SELECT
                row_id,
                event_ts,
                org,
                orch_address,
                orch_uri_norm,
                arrayJoin(JSONExtractArrayRaw(raw_capabilities, 'hardware')) AS hardware_json
            FROM naap.normalized_network_capabilities
            WHERE length(JSONExtractArrayRaw(raw_capabilities, 'hardware')) > 0
        )
    )
)
WHERE pipeline_id != ''
  AND JSONExtractString(gpu_json, 'id') != '';
