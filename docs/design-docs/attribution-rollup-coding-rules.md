# Attribution & Rollup Coding Rules

These rules are the implementation contract for resolver-owned attribution,
canonical job facts, and downstream rollups. Treat them as merge-blocking
requirements, not suggestions.

## Canonical fact rules

- Canonical job views must expose one row per logical job key.
- AI-batch job key: `(org, request_id)`.
- BYOC job key: `(org, event_id)` even when the public view aliases it as
  `request_id`.
- LLM enrichment must be one row per `(org, request_id)` before joining into
  canonical AI-batch jobs.
- Direct one-to-many joins into canonical fact views are forbidden.

## Attribution rules

- The resolver is the attribution authority. dbt may read resolver-owned stores
  but must not re-infer attribution from raw history on public-serving paths.
- `verify` and `dry-run` must execute full compute paths and skip only writes.
- BYOC worker lifecycle is the primary model source.
- When a model hint is available, compatibility selection must use it before the
  final row is materialized.
- BYOC worker identity must prefer `(org, capability, orch_address, worker_url)`
  over address-only fallback.

## Rollup and dashboard rules

- Dedupe before enrichment.
- Publish additive support fields whenever higher-grain consumers need to
  recompute derived metrics safely.
- Dashboards and APIs must use weighted/additive math over aggregated views.
- Never average averages or ratios when additive support exists.
- Inflation-sensitive rollups must have explicit tests comparing published
  counts to canonical unique-job counts.

## Naming rules

- Address fields must contain addresses.
- URI fields must contain URIs.
- Do not reuse a convenient old field name for a different identity type.
- If a surface is non-streaming and URL-identified, prefer `orchestrator_uri`.

## Required tests

- uniqueness on canonical job and enrichment keys
- accepted-values coverage for attribution statuses
- inflation guards on downstream rollups
- regression tests for verify/dry-run behavior on new compute phases
- regression tests for BYOC worker-url/model-hint precedence

## Performance and rollout rules

- Resolver-owned stores require an explicit bootstrap/backfill plan on deploy.
- A schema migration that only creates empty stores is not a complete rollout.
- When a change adds or rewires attribution stores, update the runbooks and
  dashboard integrity checks in the same change.
