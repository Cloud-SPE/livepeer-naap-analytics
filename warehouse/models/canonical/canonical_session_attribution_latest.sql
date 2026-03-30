select
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
    selection_confidence
from (
    select
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
        selection_confidence
    from naap.canonical_session_attribution_current final
)
limit 1 by canonical_session_key
