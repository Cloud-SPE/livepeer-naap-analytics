with ranked as (
    select
        canonical_session_key,
        org,
        selection_ts as decision_anchor_ts,
        canonical_model,
        cast(attribution_status = 'ambiguous', 'UInt8') as has_ambiguous_identity,
        cast(selected_capability_version_id IS NOT NULL, 'UInt8') as has_snapshot_match,
        cast(attribution_status = 'hardware_less', 'UInt8') as is_hardware_less,
        cast(attribution_status = 'stale', 'UInt8') as is_stale,
        attribution_reason,
        attribution_status,
        attributed_orch_address,
        attributed_orch_uri,
        selected_capability_version_id as attribution_snapshot_row_id,
        selected_snapshot_ts as attribution_snapshot_ts,
        attribution_method,
        selection_confidence,
        row_number() over (
            partition by org, canonical_session_key
            order by selection_ts desc, decided_at desc, selection_event_id desc
        ) as rn
    from naap.canonical_selection_attribution_current final
)
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
from ranked
where rn = 1
