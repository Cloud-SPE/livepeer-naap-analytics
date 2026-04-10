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

with {{ latest_value_cte('latest', 'naap.canonical_byoc_job_store', ['org', 'event_id'], 'materialized_at', 'materialized_at') }}

select
    -- Explicit aliases required: ClickHouse qualifies ambiguous column names with
    -- the table alias (e.g. s.org) when the same column exists in multiple joined
    -- tables (here: canonical_byoc_job_store and the latest CTE which also has
    -- org and event_id). Without aliases the view schema exposes "s.org" instead
    -- of "org", which breaks any downstream view referencing these columns by plain name.
    s.event_id              as request_id,
    s.org                   as org,
    s.gateway               as gateway,
    s.capability            as capability,
    cast(null as Nullable(DateTime64(3, 'UTC'))) as submitted_at,
    s.completed_at          as completed_at,
    s.success               as success,
    s.duration_ms           as duration_ms,
    s.http_status           as http_status,
    s.orch_address          as orch_address,
    s.orch_url              as orch_url,
    s.orch_url_norm         as orch_url_norm,
    s.worker_url            as worker_url,
    s.charged_compute       as charged_compute,
    s.error                 as error,
    s.model                 as model,
    s.price_per_unit        as price_per_unit,
    s.gpu_id                as gpu_id,
    s.gpu_model_name        as gpu_model_name,
    s.gpu_memory_bytes_total as gpu_memory_bytes_total,
    s.attribution_status    as attribution_status,
    s.attribution_reason    as attribution_reason,
    s.attribution_method    as attribution_method,
    s.attribution_confidence as attribution_confidence
from naap.canonical_byoc_job_store s
inner join latest
    on latest.org = s.org
   and latest.event_id = s.event_id
   and latest.materialized_at = s.materialized_at
