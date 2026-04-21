-- Migration 013: repair capability observed inventory stores by reseeding
-- historical rows before recreating the live MVs. This avoids backfill/MV
-- overlap duplicates on existing volumes.

DROP VIEW IF EXISTS naap.mv_canonical_capability_snapshots_store;
DROP VIEW IF EXISTS naap.mv_canonical_capability_offer_inventory_store_builtin;
DROP VIEW IF EXISTS naap.mv_canonical_capability_offer_inventory_store_hardware;
DROP VIEW IF EXISTS naap.mv_canonical_capability_pricing_inventory_store_capability;
DROP VIEW IF EXISTS naap.mv_canonical_capability_pricing_inventory_store_global;

TRUNCATE TABLE naap.canonical_capability_snapshots_store;
TRUNCATE TABLE naap.canonical_capability_offer_inventory_store;
TRUNCATE TABLE naap.canonical_capability_pricing_inventory_store;

INSERT INTO naap.canonical_capability_snapshots_store
SELECT
    row_id AS snapshot_row_id,
    event_id AS source_event_id,
    event_ts AS snapshot_ts,
    org,
    orch_address,
    orch_name,
    orch_uri,
    orch_uri_norm,
    version,
    raw_capabilities
FROM naap.normalized_network_capabilities
WHERE row_id != ''
  AND orch_address != '';

INSERT INTO naap.canonical_capability_offer_inventory_store
SELECT
    row_id AS snapshot_row_id,
    event_id AS source_event_id,
    event_ts AS snapshot_ts,
    org,
    orch_address,
    orch_uri,
    orch_uri_norm,
    cast(toUInt16OrZero(capability_key), 'Nullable(UInt16)') AS capability_id,
    CAST(NULL, 'Nullable(String)') AS offered_name,
    cast(nullIf(tupleElement(model_entry, 1), ''), 'Nullable(String)') AS model_id,
    cast(JSONExtractBool(tupleElement(model_entry, 2), 'warm'), 'Nullable(UInt8)') AS warm,
    cast(nullIf(JSONExtractInt(tupleElement(model_entry, 2), 'capacity'), 0), 'Nullable(Int32)') AS advertised_capacity,
    toUInt8(0) AS hardware_present,
    CAST(NULL, 'Nullable(String)') AS gpu_id,
    CAST(NULL, 'Nullable(String)') AS gpu_model_name,
    CAST(NULL, 'Nullable(UInt64)') AS gpu_memory_bytes_total
FROM (
    SELECT
        row_id,
        event_id,
        event_ts,
        org,
        orch_address,
        orch_uri,
        orch_uri_norm,
        tupleElement(percap_entry, 1) AS capability_key,
        arrayJoin(
            if(
                models_raw IN ('', 'null', '{}'),
                CAST([], 'Array(Tuple(String, String))'),
                JSONExtractKeysAndValuesRaw(models_raw)
            )
        ) AS model_entry
    FROM (
        SELECT
            row_id,
            event_id,
            event_ts,
            org,
            orch_address,
            orch_uri,
            orch_uri_norm,
            percap_entry,
            JSONExtractRaw(tupleElement(percap_entry, 2), 'models') AS models_raw
        FROM (
            SELECT
                row_id,
                event_id,
                event_ts,
                org,
                orch_address,
                orch_uri,
                orch_uri_norm,
                arrayJoin(
                    if(
                        percap_raw IN ('', 'null', '{}'),
                        CAST([], 'Array(Tuple(String, String))'),
                        JSONExtractKeysAndValuesRaw(percap_raw)
                    )
                ) AS percap_entry
            FROM (
                SELECT
                    row_id,
                    event_id,
                    event_ts,
                    org,
                    orch_address,
                    orch_uri,
                    orch_uri_norm,
                    JSONExtractRaw(raw_capabilities, 'capabilities', 'constraints', 'PerCapability') AS percap_raw
                FROM naap.normalized_network_capabilities
                WHERE length(JSONExtractArrayRaw(raw_capabilities, 'hardware')) = 0
            )
        )
    )
)
WHERE toUInt16OrZero(capability_key) > 0
  AND tupleElement(model_entry, 1) != ''
UNION ALL
SELECT
    row_id AS snapshot_row_id,
    event_id AS source_event_id,
    event_ts AS snapshot_ts,
    org,
    orch_address,
    orch_uri,
    orch_uri_norm,
    CAST(NULL, 'Nullable(UInt16)') AS capability_id,
    cast(nullIf(JSONExtractString(hardware_json, 'pipeline'), ''), 'Nullable(String)') AS offered_name,
    cast(nullIf(JSONExtractString(hardware_json, 'model_id'), ''), 'Nullable(String)') AS model_id,
    CAST(NULL, 'Nullable(UInt8)') AS warm,
    CAST(NULL, 'Nullable(Int32)') AS advertised_capacity,
    toUInt8(1) AS hardware_present,
    cast(nullIf(JSONExtractString(gpu_json, 'id'), ''), 'Nullable(String)') AS gpu_id,
    cast(nullIf(JSONExtractString(gpu_json, 'name'), ''), 'Nullable(String)') AS gpu_model_name,
    cast(nullIf(JSONExtractUInt(gpu_json, 'memory_total'), 0), 'Nullable(UInt64)') AS gpu_memory_bytes_total
FROM (
    SELECT
        row_id,
        event_id,
        event_ts,
        org,
        orch_address,
        orch_uri,
        orch_uri_norm,
        hardware_json,
        arrayJoin(
            if(
                gpu_info_raw IN ('', 'null', '{}', '[]'),
                CAST(['{}'], 'Array(String)'),
                if(startsWith(gpu_info_raw, '['), JSONExtractArrayRaw(gpu_info_raw), tupleElement(JSONExtractKeysAndValuesRaw(gpu_info_raw), 2))
            )
        ) AS gpu_json
    FROM (
        SELECT
            row_id,
            event_id,
            event_ts,
            org,
            orch_address,
            orch_uri,
            orch_uri_norm,
            hardware_json,
            JSONExtractRaw(hardware_json, 'gpu_info') AS gpu_info_raw
        FROM (
            SELECT
                row_id,
                event_id,
                event_ts,
                org,
                orch_address,
                orch_uri,
                orch_uri_norm,
                arrayJoin(JSONExtractArrayRaw(raw_capabilities, 'hardware')) AS hardware_json
            FROM naap.normalized_network_capabilities
            WHERE length(JSONExtractArrayRaw(raw_capabilities, 'hardware')) > 0
        )
    )
)
WHERE offered_name IS NOT NULL;

INSERT INTO naap.canonical_capability_pricing_inventory_store
SELECT
    row_id AS snapshot_row_id,
    event_id AS source_event_id,
    event_ts AS snapshot_ts,
    org,
    orch_address,
    orch_uri,
    orch_uri_norm,
    cast(toUInt16(JSONExtractUInt(price_json, 'capability')), 'Nullable(UInt16)') AS capability_id,
    cast(nullIf(JSONExtractString(price_json, 'constraint'), ''), 'Nullable(String)') AS constraint_value,
    toInt64(JSONExtractInt(price_json, 'pricePerUnit')) AS price_per_unit,
    toInt64(JSONExtractInt(price_json, 'pixelsPerUnit')) AS pixels_per_unit
FROM naap.normalized_network_capabilities
ARRAY JOIN JSONExtractArrayRaw(raw_capabilities, 'capabilities_prices') AS price_json
WHERE JSONExtractUInt(price_json, 'capability') > 0
  AND JSONExtractInt(price_json, 'pricePerUnit') > 0
  AND JSONExtractInt(price_json, 'pixelsPerUnit') > 0
UNION ALL
SELECT
    row_id AS snapshot_row_id,
    event_id AS source_event_id,
    event_ts AS snapshot_ts,
    org,
    orch_address,
    orch_uri,
    orch_uri_norm,
    CAST(NULL, 'Nullable(UInt16)') AS capability_id,
    CAST(NULL, 'Nullable(String)') AS constraint_value,
    toInt64(JSONExtractInt(raw_capabilities, 'price_info', 'pricePerUnit')) AS price_per_unit,
    toInt64(JSONExtractInt(raw_capabilities, 'price_info', 'pixelsPerUnit')) AS pixels_per_unit
FROM naap.normalized_network_capabilities
WHERE JSONExtractInt(raw_capabilities, 'price_info', 'pricePerUnit') > 0
  AND JSONExtractInt(raw_capabilities, 'price_info', 'pixelsPerUnit') > 0;

CREATE MATERIALIZED VIEW naap.mv_canonical_capability_snapshots_store
TO naap.canonical_capability_snapshots_store (
    `snapshot_row_id` String,
    `source_event_id` String,
    `snapshot_ts` DateTime64(3, 'UTC'),
    `org` LowCardinality(String),
    `orch_address` String,
    `orch_name` String,
    `orch_uri` String,
    `orch_uri_norm` String,
    `version` String,
    `raw_capabilities` String
)
AS
SELECT
    row_id AS snapshot_row_id,
    event_id AS source_event_id,
    event_ts AS snapshot_ts,
    org,
    orch_address,
    orch_name,
    orch_uri,
    orch_uri_norm,
    version,
    raw_capabilities
FROM naap.normalized_network_capabilities
WHERE row_id != ''
  AND orch_address != '';

CREATE MATERIALIZED VIEW naap.mv_canonical_capability_offer_inventory_store_builtin
TO naap.canonical_capability_offer_inventory_store (
    `snapshot_row_id` String,
    `source_event_id` String,
    `snapshot_ts` DateTime64(3, 'UTC'),
    `org` LowCardinality(String),
    `orch_address` String,
    `orch_uri` String,
    `orch_uri_norm` String,
    `capability_id` Nullable(UInt16),
    `offered_name` Nullable(String),
    `model_id` Nullable(String),
    `warm` Nullable(UInt8),
    `advertised_capacity` Nullable(Int32),
    `hardware_present` UInt8,
    `gpu_id` Nullable(String),
    `gpu_model_name` Nullable(String),
    `gpu_memory_bytes_total` Nullable(UInt64)
)
AS
SELECT
    row_id AS snapshot_row_id,
    event_id AS source_event_id,
    event_ts AS snapshot_ts,
    org,
    orch_address,
    orch_uri,
    orch_uri_norm,
    cast(toUInt16OrZero(capability_key), 'Nullable(UInt16)') AS capability_id,
    CAST(NULL, 'Nullable(String)') AS offered_name,
    cast(nullIf(tupleElement(model_entry, 1), ''), 'Nullable(String)') AS model_id,
    cast(JSONExtractBool(tupleElement(model_entry, 2), 'warm'), 'Nullable(UInt8)') AS warm,
    cast(nullIf(JSONExtractInt(tupleElement(model_entry, 2), 'capacity'), 0), 'Nullable(Int32)') AS advertised_capacity,
    toUInt8(0) AS hardware_present,
    CAST(NULL, 'Nullable(String)') AS gpu_id,
    CAST(NULL, 'Nullable(String)') AS gpu_model_name,
    CAST(NULL, 'Nullable(UInt64)') AS gpu_memory_bytes_total
FROM (
    SELECT
        row_id,
        event_id,
        event_ts,
        org,
        orch_address,
        orch_uri,
        orch_uri_norm,
        tupleElement(percap_entry, 1) AS capability_key,
        arrayJoin(
            if(
                models_raw IN ('', 'null', '{}'),
                CAST([], 'Array(Tuple(String, String))'),
                JSONExtractKeysAndValuesRaw(models_raw)
            )
        ) AS model_entry
    FROM (
        SELECT
            row_id,
            event_id,
            event_ts,
            org,
            orch_address,
            orch_uri,
            orch_uri_norm,
            percap_entry,
            JSONExtractRaw(tupleElement(percap_entry, 2), 'models') AS models_raw
        FROM (
            SELECT
                row_id,
                event_id,
                event_ts,
                org,
                orch_address,
                orch_uri,
                orch_uri_norm,
                arrayJoin(
                    if(
                        percap_raw IN ('', 'null', '{}'),
                        CAST([], 'Array(Tuple(String, String))'),
                        JSONExtractKeysAndValuesRaw(percap_raw)
                    )
                ) AS percap_entry
            FROM (
                SELECT
                    row_id,
                    event_id,
                    event_ts,
                    org,
                    orch_address,
                    orch_uri,
                    orch_uri_norm,
                    JSONExtractRaw(raw_capabilities, 'capabilities', 'constraints', 'PerCapability') AS percap_raw
                FROM naap.normalized_network_capabilities
                WHERE length(JSONExtractArrayRaw(raw_capabilities, 'hardware')) = 0
            )
        )
    )
)
WHERE toUInt16OrZero(capability_key) > 0
  AND tupleElement(model_entry, 1) != '';

CREATE MATERIALIZED VIEW naap.mv_canonical_capability_offer_inventory_store_hardware
TO naap.canonical_capability_offer_inventory_store (
    `snapshot_row_id` String,
    `source_event_id` String,
    `snapshot_ts` DateTime64(3, 'UTC'),
    `org` LowCardinality(String),
    `orch_address` String,
    `orch_uri` String,
    `orch_uri_norm` String,
    `capability_id` Nullable(UInt16),
    `offered_name` Nullable(String),
    `model_id` Nullable(String),
    `warm` Nullable(UInt8),
    `advertised_capacity` Nullable(Int32),
    `hardware_present` UInt8,
    `gpu_id` Nullable(String),
    `gpu_model_name` Nullable(String),
    `gpu_memory_bytes_total` Nullable(UInt64)
)
AS
SELECT
    row_id AS snapshot_row_id,
    event_id AS source_event_id,
    event_ts AS snapshot_ts,
    org,
    orch_address,
    orch_uri,
    orch_uri_norm,
    CAST(NULL, 'Nullable(UInt16)') AS capability_id,
    cast(nullIf(JSONExtractString(hardware_json, 'pipeline'), ''), 'Nullable(String)') AS offered_name,
    cast(nullIf(JSONExtractString(hardware_json, 'model_id'), ''), 'Nullable(String)') AS model_id,
    CAST(NULL, 'Nullable(UInt8)') AS warm,
    CAST(NULL, 'Nullable(Int32)') AS advertised_capacity,
    toUInt8(1) AS hardware_present,
    cast(nullIf(JSONExtractString(gpu_json, 'id'), ''), 'Nullable(String)') AS gpu_id,
    cast(nullIf(JSONExtractString(gpu_json, 'name'), ''), 'Nullable(String)') AS gpu_model_name,
    cast(nullIf(JSONExtractUInt(gpu_json, 'memory_total'), 0), 'Nullable(UInt64)') AS gpu_memory_bytes_total
FROM (
    SELECT
        row_id,
        event_id,
        event_ts,
        org,
        orch_address,
        orch_uri,
        orch_uri_norm,
        hardware_json,
        arrayJoin(
            if(
                gpu_info_raw IN ('', 'null', '{}', '[]'),
                CAST(['{}'], 'Array(String)'),
                if(startsWith(gpu_info_raw, '['), JSONExtractArrayRaw(gpu_info_raw), tupleElement(JSONExtractKeysAndValuesRaw(gpu_info_raw), 2))
            )
        ) AS gpu_json
    FROM (
        SELECT
            row_id,
            event_id,
            event_ts,
            org,
            orch_address,
            orch_uri,
            orch_uri_norm,
            hardware_json,
            JSONExtractRaw(hardware_json, 'gpu_info') AS gpu_info_raw
        FROM (
            SELECT
                row_id,
                event_id,
                event_ts,
                org,
                orch_address,
                orch_uri,
                orch_uri_norm,
                arrayJoin(JSONExtractArrayRaw(raw_capabilities, 'hardware')) AS hardware_json
            FROM naap.normalized_network_capabilities
            WHERE length(JSONExtractArrayRaw(raw_capabilities, 'hardware')) > 0
        )
    )
)
WHERE offered_name IS NOT NULL;

CREATE MATERIALIZED VIEW naap.mv_canonical_capability_pricing_inventory_store_capability
TO naap.canonical_capability_pricing_inventory_store (
    `snapshot_row_id` String,
    `source_event_id` String,
    `snapshot_ts` DateTime64(3, 'UTC'),
    `org` LowCardinality(String),
    `orch_address` String,
    `orch_uri` String,
    `orch_uri_norm` String,
    `capability_id` Nullable(UInt16),
    `constraint_value` Nullable(String),
    `price_per_unit` Int64,
    `pixels_per_unit` Int64
)
AS
SELECT
    row_id AS snapshot_row_id,
    event_id AS source_event_id,
    event_ts AS snapshot_ts,
    org,
    orch_address,
    orch_uri,
    orch_uri_norm,
    cast(toUInt16(JSONExtractUInt(price_json, 'capability')), 'Nullable(UInt16)') AS capability_id,
    cast(nullIf(JSONExtractString(price_json, 'constraint'), ''), 'Nullable(String)') AS constraint_value,
    toInt64(JSONExtractInt(price_json, 'pricePerUnit')) AS price_per_unit,
    toInt64(JSONExtractInt(price_json, 'pixelsPerUnit')) AS pixels_per_unit
FROM naap.normalized_network_capabilities
ARRAY JOIN JSONExtractArrayRaw(raw_capabilities, 'capabilities_prices') AS price_json
WHERE JSONExtractUInt(price_json, 'capability') > 0
  AND JSONExtractInt(price_json, 'pricePerUnit') > 0
  AND JSONExtractInt(price_json, 'pixelsPerUnit') > 0;

CREATE MATERIALIZED VIEW naap.mv_canonical_capability_pricing_inventory_store_global
TO naap.canonical_capability_pricing_inventory_store (
    `snapshot_row_id` String,
    `source_event_id` String,
    `snapshot_ts` DateTime64(3, 'UTC'),
    `org` LowCardinality(String),
    `orch_address` String,
    `orch_uri` String,
    `orch_uri_norm` String,
    `capability_id` Nullable(UInt16),
    `constraint_value` Nullable(String),
    `price_per_unit` Int64,
    `pixels_per_unit` Int64
)
AS
SELECT
    row_id AS snapshot_row_id,
    event_id AS source_event_id,
    event_ts AS snapshot_ts,
    org,
    orch_address,
    orch_uri,
    orch_uri_norm,
    CAST(NULL, 'Nullable(UInt16)') AS capability_id,
    CAST(NULL, 'Nullable(String)') AS constraint_value,
    toInt64(JSONExtractInt(raw_capabilities, 'price_info', 'pricePerUnit')) AS price_per_unit,
    toInt64(JSONExtractInt(raw_capabilities, 'price_info', 'pixelsPerUnit')) AS pixels_per_unit
FROM naap.normalized_network_capabilities
WHERE JSONExtractInt(raw_capabilities, 'price_info', 'pricePerUnit') > 0
  AND JSONExtractInt(raw_capabilities, 'price_info', 'pixelsPerUnit') > 0;
