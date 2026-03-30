-- Migration 036: selection-centered resolver spine.
--
-- This migration introduces the new canonical selection-centered tables without
-- destructively removing the existing refresh-era tables. The resolver runtime
-- writes to these tables directly and does not depend on dbt-compiled runtime
-- SQL.

ALTER TABLE naap.events
    ADD COLUMN IF NOT EXISTS source_topic LowCardinality(String) DEFAULT '';

ALTER TABLE naap.events
    ADD COLUMN IF NOT EXISTS source_partition Int32 DEFAULT 0;

ALTER TABLE naap.events
    ADD COLUMN IF NOT EXISTS source_offset Int64 DEFAULT 0;

ALTER TABLE naap.events
    ADD COLUMN IF NOT EXISTS payload_hash String DEFAULT hex(MD5(data));

ALTER TABLE naap.events
    ADD COLUMN IF NOT EXISTS schema_version Nullable(String) DEFAULT cast(null as Nullable(String));

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

CREATE TABLE IF NOT EXISTS naap.canonical_orch_capability_versions
(
    capability_version_id      String,
    org                        LowCardinality(String),
    orch_address               String,
    orch_uri                   Nullable(String),
    orch_uri_norm              String,
    local_address              Nullable(String),
    snapshot_event_id          String,
    snapshot_ts                DateTime64(3, 'UTC'),
    capability_payload_hash    String,
    raw_capabilities           String,
    is_noop                    UInt8,
    version_rank               UInt32,
    resolver_version           String DEFAULT '',
    resolver_run_id            String DEFAULT '',
    built_at                   DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = ReplacingMergeTree(built_at)
PARTITION BY (org, toYYYYMM(snapshot_ts))
ORDER BY (org, orch_address, orch_uri_norm, snapshot_ts, capability_version_id)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS naap.canonical_orch_capability_intervals
(
    capability_version_id      String,
    org                        LowCardinality(String),
    orch_address               String,
    orch_uri                   Nullable(String),
    orch_uri_norm              String,
    valid_from_ts              DateTime64(3, 'UTC'),
    valid_to_ts                Nullable(DateTime64(3, 'UTC')),
    canonical_pipeline         Nullable(String),
    canonical_model            Nullable(String),
    gpu_id                     Nullable(String),
    gpu_model_name             Nullable(String),
    gpu_memory_bytes_total     Nullable(UInt64),
    hardware_present           UInt8,
    interval_hash              String,
    resolver_version           String DEFAULT '',
    resolver_run_id            String DEFAULT '',
    built_at                   DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = ReplacingMergeTree(built_at)
PARTITION BY (org, toYYYYMM(valid_from_ts))
ORDER BY (org, orch_address, valid_from_ts, capability_version_id, ifNull(canonical_pipeline, ''), ifNull(canonical_model, ''), ifNull(gpu_id, ''))
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS naap.canonical_selection_attribution_decisions
(
    decision_id                    String,
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
ENGINE = MergeTree()
PARTITION BY (org, toYYYYMM(selection_ts))
ORDER BY (org, selection_event_id, decided_at, decision_id)
SETTINGS index_granularity = 8192;

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

CREATE TABLE IF NOT EXISTS naap.canonical_session_current_store
(
    canonical_session_key         String,
    org                           LowCardinality(String),
    stream_id                     String,
    request_id                    String,
    current_selection_event_id    Nullable(String),
    current_selection_ts          Nullable(DateTime64(3, 'UTC')),
    canonical_pipeline            String,
    canonical_model               Nullable(String),
    gpu_id                        Nullable(String),
    started_at                    Nullable(DateTime64(3, 'UTC')),
    last_seen                     DateTime64(3, 'UTC'),
    started                       UInt8,
    playable_seen                 UInt8,
    no_orch                       UInt8,
    completed                     UInt8,
    swap_count                    UInt64,
    restart_seen                  UInt8,
    error_seen                    UInt8,
    degraded_input_seen           UInt8,
    degraded_inference_seen       UInt8,
    status_sample_count           UInt64,
    status_error_sample_count     UInt64,
    loading_only_session          UInt8,
    zero_output_fps_session       UInt8,
    health_signal_count           UInt64,
    health_expected_signal_count  UInt64,
    health_signal_coverage_ratio  Float64,
    startup_outcome               LowCardinality(String),
    has_ambiguous_identity        UInt8,
    has_snapshot_match            UInt8,
    is_hardware_less              UInt8,
    is_stale                      UInt8,
    attribution_reason            LowCardinality(String),
    attribution_status            LowCardinality(String),
    attributed_orch_address       Nullable(String),
    attributed_orch_uri           Nullable(String),
    attribution_snapshot_ts       Nullable(DateTime64(3, 'UTC')),
    resolver_version              String DEFAULT '',
    resolver_run_id               String DEFAULT '',
    materialized_at               DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = ReplacingMergeTree(materialized_at)
PARTITION BY (org, toYYYYMM(last_seen))
ORDER BY (org, canonical_session_key)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS naap.selection_attribution_changes
(
    run_id                        String,
    selection_event_id            String,
    org                           LowCardinality(String),
    canonical_session_key         String,
    selection_ts                  DateTime64(3, 'UTC'),
    change_reason                 LowCardinality(String),
    previous_decision_hash        Nullable(String),
    current_decision_hash         String,
    created_at                    DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(created_at)
ORDER BY (run_id, selection_event_id)
TTL toDateTime(created_at) + INTERVAL 7 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS naap.session_current_changes
(
    run_id                        String,
    canonical_session_key         String,
    org                           LowCardinality(String),
    last_seen                     DateTime64(3, 'UTC'),
    change_reason                 LowCardinality(String),
    current_row_hash              String,
    created_at                    DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(created_at)
ORDER BY (run_id, canonical_session_key)
TTL toDateTime(created_at) + INTERVAL 7 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS naap.status_hour_changes
(
    run_id                        String,
    canonical_session_key         String,
    org                           LowCardinality(String),
    hour                          DateTime('UTC'),
    change_reason                 LowCardinality(String),
    current_row_hash              String,
    created_at                    DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(created_at)
ORDER BY (run_id, canonical_session_key, hour)
TTL toDateTime(created_at) + INTERVAL 7 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS naap.resolver_runs
(
    run_id                        String,
    mode                          LowCardinality(String),
    status                        LowCardinality(String),
    owner_id                      String,
    org                           Nullable(String),
    window_start                  Nullable(DateTime64(3, 'UTC')),
    window_end                    Nullable(DateTime64(3, 'UTC')),
    cutoff_ts                     Nullable(DateTime64(3, 'UTC')),
    lateness_window_seconds       UInt64 DEFAULT 0,
    rows_processed                UInt64 DEFAULT 0,
    mismatch_count                UInt64 DEFAULT 0,
    error_summary                 Nullable(String),
    resolver_version              String DEFAULT '',
    started_at                    DateTime64(3, 'UTC'),
    finished_at                   Nullable(DateTime64(3, 'UTC')),
    created_at                    DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(started_at)
ORDER BY (started_at, run_id)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS naap.resolver_backfill_runs
(
    run_id                        String,
    owner_id                      String,
    org                           LowCardinality(String),
    event_date                    Date,
    hash_range_start              Nullable(UInt32),
    hash_range_end                Nullable(UInt32),
    status                        LowCardinality(String),
    cutoff_ts                     Nullable(DateTime64(3, 'UTC')),
    rows_processed                UInt64 DEFAULT 0,
    mismatch_count                UInt64 DEFAULT 0,
    error_summary                 Nullable(String),
    started_at                    DateTime64(3, 'UTC'),
    finished_at                   Nullable(DateTime64(3, 'UTC')),
    created_at                    DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (org, event_date, ifNull(hash_range_start, 0), run_id)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS naap.resolver_dead_letters
(
    dead_letter_id                String,
    item_type                     LowCardinality(String),
    item_id                       String,
    org                           Nullable(String),
    earliest_affected_ts          Nullable(DateTime64(3, 'UTC')),
    attempt_count                 UInt32 DEFAULT 0,
    next_retry_at                 Nullable(DateTime64(3, 'UTC')),
    last_error_summary            String,
    resolver_version              String DEFAULT '',
    escalated                     UInt8 DEFAULT 0,
    created_at                    DateTime64(3, 'UTC') DEFAULT now64(),
    updated_at                    DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(created_at)
ORDER BY (item_type, item_id, dead_letter_id)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS naap.resolver_dirty_sessions
(
    work_item_id                  String,
    canonical_session_key         String,
    org                           LowCardinality(String),
    reason                        LowCardinality(String),
    earliest_affected_ts          Nullable(DateTime64(3, 'UTC')),
    claim_owner                   Nullable(String),
    lease_acquired_at             Nullable(DateTime64(3, 'UTC')),
    lease_expires_at              Nullable(DateTime64(3, 'UTC')),
    attempt_count                 UInt32 DEFAULT 0,
    next_retry_at                 Nullable(DateTime64(3, 'UTC')),
    last_error_summary            Nullable(String),
    resolver_version              String DEFAULT '',
    created_at                    DateTime64(3, 'UTC') DEFAULT now64(),
    updated_at                    DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(created_at)
ORDER BY (org, canonical_session_key, work_item_id)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS naap.resolver_dirty_orchestrators
(
    work_item_id                  String,
    orch_identity                 String,
    org                           LowCardinality(String),
    reason                        LowCardinality(String),
    earliest_affected_ts          Nullable(DateTime64(3, 'UTC')),
    claim_owner                   Nullable(String),
    lease_acquired_at             Nullable(DateTime64(3, 'UTC')),
    lease_expires_at              Nullable(DateTime64(3, 'UTC')),
    attempt_count                 UInt32 DEFAULT 0,
    next_retry_at                 Nullable(DateTime64(3, 'UTC')),
    last_error_summary            Nullable(String),
    resolver_version              String DEFAULT '',
    created_at                    DateTime64(3, 'UTC') DEFAULT now64(),
    updated_at                    DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(created_at)
ORDER BY (org, orch_identity, work_item_id)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS naap.resolver_dirty_selection_events
(
    work_item_id                  String,
    selection_event_id            String,
    org                           LowCardinality(String),
    canonical_session_key         String,
    reason                        LowCardinality(String),
    earliest_affected_ts          Nullable(DateTime64(3, 'UTC')),
    claim_owner                   Nullable(String),
    lease_acquired_at             Nullable(DateTime64(3, 'UTC')),
    lease_expires_at              Nullable(DateTime64(3, 'UTC')),
    attempt_count                 UInt32 DEFAULT 0,
    next_retry_at                 Nullable(DateTime64(3, 'UTC')),
    last_error_summary            Nullable(String),
    resolver_version              String DEFAULT '',
    created_at                    DateTime64(3, 'UTC') DEFAULT now64(),
    updated_at                    DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(created_at)
ORDER BY (org, selection_event_id, work_item_id)
SETTINGS index_granularity = 8192;
