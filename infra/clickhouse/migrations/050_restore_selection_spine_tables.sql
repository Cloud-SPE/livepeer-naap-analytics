-- Migration 050: restore resolver-owned selection spine tables.
--
-- dbt must not shadow resolver-owned physical tables with views of the same
-- name. Recreate the physical tables so the resolver remains the source of
-- truth for selection events and attribution current state.

DROP TABLE IF EXISTS naap.canonical_selection_events;

CREATE TABLE IF NOT EXISTS naap.canonical_selection_events
(
    selection_event_id         String,
    org                        LowCardinality(String),
    canonical_session_key      String,
    selection_seq              UInt32,
    selection_ts               DateTime64(3, 'UTC'),
    selection_trigger          LowCardinality(String),
    observed_orch_raw_address  Nullable(String),
    observed_orch_url          Nullable(String),
    observed_model_hint        Nullable(String),
    observed_pipeline_hint     Nullable(String),
    anchor_event_id            String,
    anchor_event_type          LowCardinality(String),
    anchor_event_ts            DateTime64(3, 'UTC'),
    source_topic               LowCardinality(String) DEFAULT '',
    source_partition           Int32 DEFAULT 0,
    source_offset              Int64 DEFAULT 0,
    selection_input_hash       String DEFAULT '',
    resolver_version           String DEFAULT '',
    resolver_run_id            String DEFAULT '',
    rebuilt_at                 DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = ReplacingMergeTree(rebuilt_at)
PARTITION BY (org, toYYYYMM(selection_ts))
ORDER BY (org, canonical_session_key, selection_event_id)
SETTINGS index_granularity = 8192;

DROP TABLE IF EXISTS naap.canonical_selection_attribution_current;

CREATE TABLE IF NOT EXISTS naap.canonical_selection_attribution_current
(
    selection_event_id             String,
    org                            LowCardinality(String),
    canonical_session_key          String,
    selection_ts                   DateTime64(3, 'UTC'),
    attribution_status             LowCardinality(String),
    attribution_reason             LowCardinality(String),
    attribution_method             LowCardinality(String),
    selection_confidence           LowCardinality(String),
    selected_capability_version_id Nullable(String),
    selected_snapshot_event_id     Nullable(String),
    selected_snapshot_ts           Nullable(DateTime64(3, 'UTC')),
    attributed_orch_address        Nullable(String),
    attributed_orch_uri            Nullable(String),
    canonical_pipeline             Nullable(String),
    canonical_model                Nullable(String),
    gpu_id                         Nullable(String),
    decision_input_hash            String DEFAULT '',
    resolver_version               String DEFAULT '',
    resolver_run_id                String DEFAULT '',
    decided_at                     DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = ReplacingMergeTree(decided_at)
PARTITION BY org
ORDER BY (org, selection_event_id)
SETTINGS index_granularity = 8192;
