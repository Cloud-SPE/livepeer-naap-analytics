-- Perf-pass: thin latest-slice reader over api_orchestrator_identity_store.
-- The prior view expanded canonical_capability_orchestrator_identity_latest
-- on every read (multi-CTE argMaxIfMerge over canonical_capability_snapshot_latest
-- + ENS metadata — 315 GiB of reads per 2h of dashboard traffic in profiling).
-- The resolver now pre-materializes the identity chain into a single row per
-- orch_address; this view just picks the latest refresh slice.

{{ config(materialized='view') }}

with latest_slices as (
    select orch_address, argMax(refresh_run_id, refreshed_at) as refresh_run_id
    from naap.api_orchestrator_identity_store
    group by orch_address
)
select
    s.capability_version_id,
    s.snapshot_event_id,
    s.orch_address,
    s.name,
    s.orch_name,
    s.orch_label,
    s.orchestrator_uri,
    s.orch_uri_norm,
    s.version,
    s.org,
    s.last_seen
from naap.api_orchestrator_identity_store as s
inner join latest_slices as l
    on  s.orch_address = l.orch_address
    and s.refresh_run_id = l.refresh_run_id
