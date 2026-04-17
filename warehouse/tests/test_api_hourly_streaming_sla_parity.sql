-- Parity test: the new api_hourly_streaming_sla view (reading from
-- api_hourly_streaming_sla_store) must return row-equivalent output to
-- the legacy api_base_sla_compliance_scored_by_org view on the same
-- state. Both paths share the same scoring math — the new one runs it
-- at resolver-write time and stores the result, the old one recomputes
-- it on every read. Their outputs must match, otherwise the repoint
-- introduced a semantic drift.
--
-- This test protects the Phase 1 invariant as long as api_base_* still
-- exists. Phase 5 retires api_base_* and this test goes with it; by
-- then the semantic contract has been stable for multiple phases.

with
    new_path as (
        select
            window_start,
            ifNull(org, '') as org_key,
            orchestrator_address,
            pipeline_id,
            ifNull(model_id, '') as model_id_key,
            ifNull(gpu_id, '') as gpu_id_key,
            toUInt64(requested_sessions) as requested_sessions,
            toUInt64(startup_success_sessions) as startup_success_sessions,
            toUInt64(total_swapped_sessions) as total_swapped_sessions,
            toUInt64(status_samples) as status_samples,
            round(ifNull(sla_score, -1.0), 4) as sla_score_rounded
        from {{ ref('api_hourly_streaming_sla') }}
    ),
    old_path as (
        select
            window_start,
            ifNull(org, '') as org_key,
            orchestrator_address,
            pipeline_id,
            ifNull(model_id, '') as model_id_key,
            ifNull(gpu_id, '') as gpu_id_key,
            toUInt64(requested_sessions) as requested_sessions,
            toUInt64(startup_success_sessions) as startup_success_sessions,
            toUInt64(total_swapped_sessions) as total_swapped_sessions,
            toUInt64(status_samples) as status_samples,
            round(ifNull(sla_score, -1.0), 4) as sla_score_rounded
        from {{ ref('api_base_sla_compliance_scored_by_org') }}
    ),
    in_new_not_old as (
        select 'missing_in_old' as side, * from new_path
        except
        select 'missing_in_old' as side, * from old_path
    ),
    in_old_not_new as (
        select 'missing_in_new' as side, * from old_path
        except
        select 'missing_in_new' as side, * from new_path
    )
select * from in_new_not_old
union all
select * from in_old_not_new
