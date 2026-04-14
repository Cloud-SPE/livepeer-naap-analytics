-- Rebuild legacy canonical_session_attribution_* compatibility tables from the
-- current selection-centered attribution state produced by the resolver.
--
-- This keeps older semantic views coherent after a retained-raw rebuild even
-- though the maintained runtime writes canonical_selection_attribution_*.

TRUNCATE TABLE naap.canonical_session_attribution_audit;
TRUNCATE TABLE naap.canonical_session_attribution_current;
TRUNCATE TABLE naap.canonical_session_attribution_latest_store;

INSERT INTO naap.canonical_session_attribution_audit
SELECT
    canonical_session_key,
    org,
    selection_ts AS decision_anchor_ts,
    canonical_model,
    toUInt8(attribution_status = 'ambiguous') AS has_ambiguous_identity,
    toUInt8(selected_snapshot_event_id IS NOT NULL) AS has_snapshot_match,
    toUInt8(attribution_status = 'hardware_less') AS is_hardware_less,
    toUInt8(attribution_status = 'stale') AS is_stale,
    attribution_reason,
    attribution_status,
    attributed_orch_address,
    attributed_orch_uri,
    selected_snapshot_event_id AS attribution_snapshot_row_id,
    selected_snapshot_ts AS attribution_snapshot_ts,
    attribution_method,
    selection_confidence,
    resolver_run_id AS refresh_run_id,
    '' AS artifact_checksum,
    decided_at
FROM naap.canonical_selection_attribution_decisions;

INSERT INTO naap.canonical_session_attribution_current
SELECT
    canonical_session_key,
    org,
    selection_ts AS decision_anchor_ts,
    canonical_model,
    toUInt8(attribution_status = 'ambiguous') AS has_ambiguous_identity,
    toUInt8(selected_snapshot_event_id IS NOT NULL) AS has_snapshot_match,
    toUInt8(attribution_status = 'hardware_less') AS is_hardware_less,
    toUInt8(attribution_status = 'stale') AS is_stale,
    attribution_reason,
    attribution_status,
    attributed_orch_address,
    attributed_orch_uri,
    selected_snapshot_event_id AS attribution_snapshot_row_id,
    selected_snapshot_ts AS attribution_snapshot_ts,
    attribution_method,
    selection_confidence,
    decision_input_hash AS attribution_input_hash,
    resolver_run_id AS refresh_run_id,
    '' AS artifact_checksum,
    decided_at
FROM (
    SELECT
        canonical_session_key,
        org,
        selection_ts,
        canonical_model,
        attribution_reason,
        attribution_status,
        attributed_orch_address,
        attributed_orch_uri,
        selected_snapshot_event_id,
        selected_snapshot_ts,
        attribution_method,
        selection_confidence,
        decision_input_hash,
        resolver_run_id,
        decided_at,
        row_number() OVER (
            PARTITION BY org, canonical_session_key
            ORDER BY selection_ts DESC, decided_at DESC, selection_event_id DESC
        ) AS rn
    FROM naap.canonical_selection_attribution_current FINAL
)
WHERE rn = 1;

INSERT INTO naap.canonical_session_attribution_latest_store
SELECT
    canonical_session_key,
    org,
    decision_anchor_ts,
    canonical_model,
    has_ambiguous_identity,
    has_snapshot_match,
    is_hardware_less,
    is_stale,
    attribution_reason,
    attribution_status,
    attributed_orch_address,
    attributed_orch_uri,
    attribution_snapshot_row_id,
    attribution_snapshot_ts,
    attribution_method,
    selection_confidence,
    refresh_run_id,
    artifact_checksum,
    decided_at
FROM naap.canonical_session_attribution_current FINAL;
