-- Migration 030: narrow attribution latest/audit tables
--
-- Canonical attribution is the mutable part of session truth. Keep the hot join
-- surface compact and append decision history only when attribution changes.

CREATE TABLE IF NOT EXISTS naap.canonical_session_attribution_latest_store
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
    refresh_run_id                String,
    artifact_checksum             String DEFAULT '',
    decided_at                    DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = MergeTree()
PARTITION BY org
ORDER BY (canonical_session_key, decided_at)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS naap.canonical_session_attribution_audit
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
    refresh_run_id                String,
    artifact_checksum             String DEFAULT '',
    decided_at                    DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = MergeTree()
PARTITION BY (org, toYYYYMM(decided_at))
ORDER BY (canonical_session_key, decided_at)
SETTINGS index_granularity = 8192;
