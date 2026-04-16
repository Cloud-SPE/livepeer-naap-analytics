# Warehouse

dbt owns the semantic analytics model downstream of the ClickHouse
`normalized_*` tables.

See [`../docs/design-docs/system-visuals.md`](../docs/design-docs/system-visuals.md) for the current system diagrams and [`../docs/operations/run-modes-and-recovery.md`](../docs/operations/run-modes-and-recovery.md) for when to run `dbt` in the supported deployment.

Ownership boundary:

- ClickHouse: physical schema and `raw_* -> normalized_*`
- dbt: canonical semantic SQL and `normalized_* -> canonical_* -> api_base_* -> api_*`
- Go API: reads `api_*` only
- resolver: owns selection extraction, attribution, current-store materialization,
  and bounded serving publication; dbt does not own runtime orchestration

Model split:

- `warehouse/models/staging/` exposes thin views over normalized physical tables
- `warehouse/models/canonical/` exposes current canonical contracts over
  physical `canonical_*_store` tables
- `warehouse/models/api_base/` exposes internal helper views used to build
  published API/dashboard models safely from additive or merge-safe inputs
- `warehouse/models/api/` exposes serving contracts over bounded canonical and
  resolver-fed stores
- `api_current_active_stream_state` remains store-backed so request paths do not
  reconstruct hot state from wide history joins
- `api_current_active_stream_state` is deterministic; recency filtering belongs
  in request or dashboard queries, not in the semantic view itself

Tier contract:

- `raw_*` — accepted raw envelopes
- `normalized_*` — normalized event-family records
- `canonical_*` — authoritative corrected derivation source
- `operational_*` — live ops tables only
- `api_base_*` — internal semantic helper views for published API models
- `api_*` — serving/read models only

This contract is semantic, not a promise that every physical ClickHouse object
shares one of those prefixes. The generated bootstrap also includes
infrastructure/runtime tables such as `accepted_raw_events`, `ignored_raw_events`,
`kafka_*`, `resolver_*`, `agg_*`, metadata tables, and materialized views.
Those physical names are part of the supported v1 schema and are not being
renamed just to force prefix uniformity.

Temporal suffixes add semantics on top of the tier prefix:

- `*_current` / `*_latest` — true latest-state entities or helpers only
- `*_inventory` — observed historical/windowed inventory
- `*_store` — physically owned tables backing a semantic relation; `store` is not a truth tier

The naming rule is therefore:

- prefix defines the semantic tier
- suffix defines latest-state vs observed inventory vs physical storage role

Important rule:

- downstream derivations must use `canonical_*`
- `api_base_*` and `api_*` are presentation-oriented and must not be treated as truth

The project intentionally stays small. New canonical or serving layers are
added only when they close a named defect or validation gap.

Fail-safe attribution is an explicit invariant:

- unresolved, ambiguous, stale, and hardware-less states stay visible in fact
  and serving outputs
- those rows must not be dropped from demand, SLA, or reliability views
- only `resolved` rows count as fully attributed identity
- session tail filtering remains canonical and is not an `api_*`-only rule

Important attribution nuance:

- unresolved URI/local-alias gap reasons such as
  `missing_uri_snapshot_local_alias_present` remain unresolved fact rows
- `hardware_less` is only for sessions with successful canonical attribution
  but missing hardware/GPU metadata
- unresolved URI/local-alias rows are therefore visible in fact surfaces, but
  they must not be treated as hardware-less GPU-attributed sessions
