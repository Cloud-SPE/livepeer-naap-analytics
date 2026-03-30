-- Migration 018: explicit tier namespaces plus canonical refresh runtime state
--
-- The physical storage engines remain the existing events/typed/agg tables for
-- now, but the stable database contract moves to raw_*, normalized_*,
-- operational_*, canonical_*, and api_* names. Canonical/api relations are
-- published by dbt; this migration adds the raw/normalized/operational aliases
-- plus the runtime tables used by the canonical refresh worker.

CREATE VIEW IF NOT EXISTS naap.raw_events AS
SELECT * FROM naap.events;

CREATE VIEW IF NOT EXISTS naap.normalized_stream_trace AS
SELECT * FROM naap.typed_stream_trace;

CREATE VIEW IF NOT EXISTS naap.normalized_ai_stream_status AS
SELECT * FROM naap.typed_ai_stream_status;

CREATE VIEW IF NOT EXISTS naap.normalized_ai_stream_events AS
SELECT * FROM naap.typed_ai_stream_events;

CREATE VIEW IF NOT EXISTS naap.normalized_discovery_results AS
SELECT * FROM naap.typed_discovery_results;

CREATE VIEW IF NOT EXISTS naap.normalized_stream_ingest_metrics AS
SELECT * FROM naap.typed_stream_ingest_metrics;

CREATE VIEW IF NOT EXISTS naap.normalized_network_capabilities AS
SELECT * FROM naap.typed_network_capabilities;

CREATE VIEW IF NOT EXISTS naap.normalized_payments AS
SELECT * FROM naap.typed_payments;

CREATE VIEW IF NOT EXISTS naap.operational_orchestrator_state AS
SELECT * FROM naap.agg_orch_state;

CREATE VIEW IF NOT EXISTS naap.operational_stream_hourly AS
SELECT * FROM naap.agg_stream_hourly;

CREATE VIEW IF NOT EXISTS naap.operational_stream_status_samples AS
SELECT * FROM naap.agg_stream_status_samples;

CREATE VIEW IF NOT EXISTS naap.operational_payment_hourly AS
SELECT * FROM naap.agg_payment_hourly;

CREATE VIEW IF NOT EXISTS naap.operational_gpu_inventory AS
SELECT * FROM naap.agg_gpu_inventory;

CREATE TABLE IF NOT EXISTS naap.canonical_refresh_lease
(
    lease_name      LowCardinality(String),
    owner_id        String,
    fencing_token   String,
    status          LowCardinality(String) DEFAULT 'active',
    acquired_at     DateTime64(3, 'UTC') DEFAULT now64(),
    heartbeat_at    DateTime64(3, 'UTC') DEFAULT now64(),
    expires_at      DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(heartbeat_at)
ORDER BY lease_name;

CREATE TABLE IF NOT EXISTS naap.canonical_refresh_watermark
(
    pipeline_name     LowCardinality(String),
    watermark_ts      DateTime64(3, 'UTC'),
    artifact_checksum String DEFAULT '',
    updated_at        DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY pipeline_name;

CREATE TABLE IF NOT EXISTS naap.canonical_refresh_runs
(
    run_id                    String,
    lease_name                LowCardinality(String),
    owner_id                  String,
    fencing_token             String,
    artifact_checksum         String DEFAULT '',
    status                    LowCardinality(String),
    started_at                DateTime64(3, 'UTC') DEFAULT now64(),
    finished_at               Nullable(DateTime64(3, 'UTC')),
    watermark_before          Nullable(DateTime64(3, 'UTC')),
    watermark_after           Nullable(DateTime64(3, 'UTC')),
    affected_session_keys     UInt64 DEFAULT 0,
    affected_capability_rows  UInt64 DEFAULT 0,
    refreshed_rows            UInt64 DEFAULT 0,
    error_summary             String DEFAULT ''
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(started_at)
ORDER BY (started_at, run_id);
