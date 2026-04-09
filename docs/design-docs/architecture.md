# Architecture

Detailed per-component architecture, layer rules, and enforcement model for the
current Go, ClickHouse, resolver, and dbt stack.

## Layered Domain Model

Within the Go service, dependencies flow forward only:

```text
Types -> Config -> Repo -> Service -> Runtime
```

## Layer Responsibilities

| Layer | Package | Responsibility |
|---|---|---|
| `Types` | `api/internal/types` | Domain types, query params, and response payloads |
| `Config` | `api/internal/config` | Environment and runtime configuration |
| `Repo` | `api/internal/repo` | ClickHouse access and query wiring |
| `Service` | `api/internal/service` | Business logic over repo contracts |
| `Runtime` | `api/internal/runtime` | HTTP handlers, middleware, metrics, and server wiring |
| `Providers` | `api/internal/providers` | Logger, telemetry, and other cross-cutting runtime providers |

Providers are injected by the runtime layer. They are not a free pass to skip
the dependency direction rules.

## Enforcement

Current repo enforcement is mechanical, not aspirational:

- static and contract checks in `api/internal/validation`
- semantic-tier validation for warehouse models
- docs and route validation for the active documentation surface
- package structure kept aligned with the layer chain above

Concretely, this means:

- route and spec drift should fail validation rather than being noticed later in review
- semantic-tier violations should be caught where published models are defined
- rollup-safety rules belong in tests and serving contracts, not just prose
- providers should be wired at runtime boundaries rather than imported as convenience globals

## Analytics Tier Contract

The semantic storage contract is:

- `raw_*` style accepted-event surfaces
- `normalized_*` event-family records
- `canonical_*` authoritative corrected facts
- `operational_*` live-ops helper surfaces
- `api_base_*` internal semantic helper views
- `api_*` published API and dashboard read models

Not every physical ClickHouse object uses those prefixes. The active bootstrap
also includes supported runtime tables such as `accepted_raw_events`,
`ignored_raw_events`, `resolver_*`, `agg_*`, metadata tables, and change tables.

Allowed semantic flow:

```text
accepted_raw_events / raw-like inputs
  -> normalized_*
  -> canonical_*
  -> api_base_*
  -> api_*
```

Forbidden semantic flow:

- `api_*` back into `canonical_*`
- `api_*` back into `api_base_*`
- downstream truth derived from `api_base_*` or `api_*`
- operational/live helper surfaces treated as long-term truth

## Warehouse And Runtime Ownership

- ClickHouse owns physical ingest and storage.
- The resolver owns corrected current-state and serving-store publication.
- dbt owns semantic read models over resolver- and ingest-owned tables.
- The Go API reads published serving contracts; it does not synthesize truth from raw history on the hot path.

## Wiring Pattern

Runtime code owns construction and injection:

- providers are created at runtime boundaries
- repo implementations depend on config and typed contracts
- services depend on repo interfaces and types
- handlers depend on service interfaces and runtime providers

That keeps cross-cutting concerns explicit and prevents repo or service code from
silently acquiring runtime-only dependencies.

## Rollup Safety

Rollup algebra is a correctness rule:

- sum additive counters directly
- recompute ratios from additive numerators and denominators
- recompute means from sums and counts
- keep percentile or distribution state merge-safe
- never average averages or ratios and never recompute percentiles from scalar percentiles

If a higher-grain consumer cannot recompute a derived metric safely from the
published support fields, the metric contract is incomplete.
