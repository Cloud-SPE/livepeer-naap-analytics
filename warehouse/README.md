# Warehouse

dbt owns the semantic analytics model downstream of the ClickHouse
`normalized_*` tables.

Ownership boundary:

- ClickHouse: physical schema and `raw_* -> normalized_*`
- dbt: canonical semantic SQL and `normalized_* -> canonical_* -> api_*`
- Go API: reads `api_*` only
- resolver: owns selection extraction, attribution, current-store materialization,
  and bounded serving publication; dbt does not own runtime orchestration

Model split:

- `warehouse/models/staging/` exposes thin views over normalized physical tables
- `warehouse/models/canonical/` exposes latest/current canonical contracts over
  physical `canonical_*_store` tables
- `warehouse/models/api/` exposes serving contracts over bounded resolver-fed
  store tables
- `api_status_samples` and `api_active_stream_state` remain store-backed so
  request paths do not reconstruct hot state from wide history joins

Tier contract:

- `raw_*` — accepted raw envelopes
- `normalized_*` — normalized event-family records
- `canonical_*` — authoritative corrected derivation source
- `operational_*` — live ops tables only
- `api_*` — serving/read models only

Important rule:

- downstream derivations must use `canonical_*`
- `api_*` is presentation-oriented and must not be treated as truth

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
