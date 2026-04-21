-- Phase 3 guard: orchestrator_uri is denormalized onto recent resolver-written
-- API SLA and active-stream store rows.
-- For rows with a concrete orchestrator_address whose identity is known
-- in canonical_capability_orchestrator_identity_latest, the resolver MUST
-- have stamped the URI at write time — an empty orchestrator_uri here
-- means the LEFT JOIN in the writer failed to find a match (likely a
-- case-sensitivity or DDL drift bug) and the API layer will silently
-- render rows without a ServiceURI.
--
-- Rows with no known identity row are ignored: the orchestrator legitimately
-- has no capability snapshot yet, so URI really is unknown.

with known_orchs as (
    select orch_address
    from naap.canonical_capability_orchestrator_identity_latest
    where ifNull(orchestrator_uri, '') != ''
),
recent_sla_cutoff as (
    select max(refreshed_at) - interval 24 hour as cutoff
    from naap.api_hourly_streaming_sla_store
),
recent_active_cutoff as (
    select max(refreshed_at) - interval 24 hour as cutoff
    from naap.canonical_active_stream_state_latest_store
),
violations_sla as (
    select
        'api_hourly_streaming_sla_store' as table_name,
        s.orchestrator_address as orch_address,
        toString(s.window_start) as row_key
    from naap.api_hourly_streaming_sla_store s
    inner join known_orchs k
        on k.orch_address = s.orchestrator_address
    where s.refreshed_at >= (select cutoff from recent_sla_cutoff)
      and ifNull(s.orchestrator_uri, '') = ''
),
violations_active as (
    select
        'canonical_active_stream_state_latest_store' as table_name,
        ifNull(a.orch_address, '') as orch_address,
        a.canonical_session_key as row_key
    from naap.canonical_active_stream_state_latest_store a
    inner join known_orchs k
        on k.orch_address = a.orch_address
    where a.refreshed_at >= (select cutoff from recent_active_cutoff)
      and ifNull(a.orchestrator_uri, '') = ''
)
select * from violations_sla
union all
select * from violations_active
