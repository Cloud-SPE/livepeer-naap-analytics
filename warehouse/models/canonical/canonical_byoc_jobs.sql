-- One row per completed BYOC job, attributed by the Go resolver.
--
-- Attribution (orchestrator identity, GPU hardware) is owned end-to-end by the
-- resolver and written to canonical_byoc_job_store. Model identity is resolved
-- from worker_lifecycle snapshots by the resolver (primary source) with the
-- capability interval canonical_model as fallback.
--
-- The event_id is the stable job key (BYOC request_id is always empty). It is
-- surfaced here as request_id to preserve the downstream API contract.
--
-- Capability strings are stored verbatim (e.g. "openai-chat-completions") and
-- are never normalized against the canonical pipeline allow-list.
--
-- Attribution status classes match the live V2V and AI batch resolver:
--   resolved       — full orchestrator + hardware identity confirmed
--   hardware_less  — orchestrator identity confirmed, no GPU data available
--   stale          — only a stale capability snapshot was available
--   ambiguous      — multiple incompatible candidates
--   unresolved     — no matching candidate found

with {{ latest_rows_cte(
    'latest',
    'naap.canonical_byoc_job_store',
    ['org', 'event_id'],
    [
        'materialized_at desc',
        'completed_at desc',
        'resolver_run_id desc',
        "ifNull(capability_version_id, '') desc",
        "ifNull(attributed_orch_uri, '') desc"
    ]
) }}

select
    latest.event_id              as request_id,
    latest.org                   as org,
    latest.gateway               as gateway,
    latest.capability            as capability,
    cast(null as Nullable(DateTime64(3, 'UTC'))) as submitted_at,
    latest.completed_at          as completed_at,
    latest.success               as success,
    latest.duration_ms           as duration_ms,
    latest.http_status           as http_status,
    latest.orch_address          as orch_address,
    latest.orch_url              as orch_url,
    latest.orch_url_norm         as orch_url_norm,
    latest.selection_outcome     as selection_outcome,
    latest.worker_url            as worker_url,
    latest.charged_compute       as charged_compute,
    latest.error                 as error,
    latest.model                 as model,
    latest.price_per_unit        as price_per_unit,
    latest.gpu_id                as gpu_id,
    latest.gpu_model_name        as gpu_model_name,
    latest.gpu_memory_bytes_total as gpu_memory_bytes_total,
    latest.attribution_status    as attribution_status,
    latest.attribution_reason    as attribution_reason,
    latest.attribution_method    as attribution_method,
    latest.attribution_confidence as attribution_confidence
from latest
