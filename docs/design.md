# Design

Top-level architecture map for Livepeer NaaP Analytics.

## System overview

```
  Livepeer Network
  ─────────────────────────────────────────────────────────────────────────────
  network_events    ──Kafka──► ClickHouse Kafka Engine ──MV──► naap.accepted_raw_events
  streaming_events  ──Engine──► tables                    │         │
                                                           │         ├──► normalized_*
                                                           │         ├──► canonical_*
                                                           │         ├──► api_base_*
                                                           │         ├──► api_*
                                                           │         └──► ignored_raw_events
                                                           └──────────────────► HTTP :8000
                                                                                (Go API)

  Livepeer Public API                                                     Grafana :3000
  ─────────────────────────────────────────────────────────────────────────────
  /api/orchestrator  ──HTTP (5m)──► enrichment worker ──INSERT──► naap.orch_metadata
  /api/gateways      ──poll──►      (Go goroutine)     ──INSERT──► naap.gateway_metadata
  accepted_raw_events ──MV/dbt──► canonical capability inventory ──VIEW──► naap.api_observed_capability_hardware

  Observability
  ─────────────────────────────────────────────────────────────────────────────
  Go API :8000/metrics           ──scrape──► Prometheus :9090 ──► Grafana :3000
  Resolver :9102/metrics          ──scrape──► Prometheus               │
  ClickHouse :9363               ──scrape──► Prometheus               │
  Kafka exporter :9308           ──scrape──► Prometheus               │
  naap.* tables         ◄──query───────────────────────────────
```

Grafana dashboards and alert rules are provisioned from the repository under
`infra/grafana/provisioning/` and `infra/grafana/dashboards/`. Alert routing is
managed in Grafana rather than Prometheus Alertmanager, and Grafana startup now
renders and validates alerting receivers from explicit env flags so enabled
channels fail fast when misconfigured.

**Ingest path:** Two Kafka topics are consumed directly by ClickHouse via the Kafka Engine.
No application-layer consumer sits between Kafka and ClickHouse.

**Enrichment path:** A background Go goroutine polls the Livepeer public API every 5 minutes
and upserts orchestrator and gateway metadata (ENS names, stake, service URIs, deposits) into
dedicated ClickHouse tables. GPU inventory is not part of enrichment anymore; it is derived
natively inside ClickHouse from retained capability events and exposed through observed-window capability views.
All enrichment tables can be JOINed from any aggregate query.

**Table population strategies:** Two distinct strategies are used for aggregate tables:
- **MV-populated** (event-driven): accepted Kafka events are routed into `naap.accepted_raw_events`, then normalized/core materialized views populate the downstream event-driven tables.
- **Worker-populated** (polled): `orch_metadata`, `gateway_metadata` — written by the enrichment worker on a 5-minute interval.

**Serving path:** The Go API reads published `api_*` relations only. The
semantic serving layer may use internal `api_base_*` helper views to compute
contracted read models, but those helpers are not public contracts.
Downstream derivations must read `canonical_*`, never `api_base_*` or `api_*`.
For SLA specifically, the resolver now publishes additive org-hour inputs and
then materializes final SLA serving rows into physical `api_*_store` tables so
the API does not score benchmark cohorts on the request hot path.

**Resolver runtime path:** The long-lived resolver service is now intended to
run in `auto` mode. One service instance:

- bootstraps visible closed historical backlog
- repairs closed historical `(org, event_date)` partitions dirtied later by
  newly accepted raw arrivals
- keeps the live lateness window current in `tail`

This avoids separate backlog and steady-state deployments while preserving
exact write ownership and bounded padded reads.

**Tier contract:** The analytics storage contract is:

- `raw_*` — accepted raw envelopes
- `normalized_*` — normalized event-family records
- `canonical_*` — authoritative corrected facts/latest-state tables
- `operational_*` — low-latency live ops tables
- `api_base_*` — internal semantic helper views for API/dashboard rollups
- `api_*` — service/dashboard read models only

This tier contract is documentation for semantic derivation flow. The physical
bootstrap also contains infrastructure/runtime namespaces such as `resolver_*`,
`agg_*`, `kafka_*`, metadata tables, and materialized views. Those objects are
supported as-is; we are not planning another broad schema rename just to force
every table into the semantic prefixes.

Temporal suffixes add semantics on top of the tier prefix:

- `*_current` / `*_latest` — true latest-state entities or helpers only; do not use for observed inventory-over-window semantics
- `*_inventory` — observed historical/windowed inventory semantics
- `*_store` — physically owned tables written by a runtime component or MV; `store` says how data is persisted, not whether it is the semantic source of truth

Naming is therefore two-part:

- tier prefix describes where a relation sits in the semantic flow
- suffix describes whether it is latest-state, observed inventory, or a physical store

Runtime/infrastructure families such as `resolver_*`, `agg_*`, `mv_*`, `kafka_*`,
and metadata tables are intentionally exempt from prefix uniformity. They are
supported physical schema objects, not semantic tier names.

Medallion mapping is documentation-only:

- bronze = `raw_*`
- silver = `normalized_*`
- gold = `canonical_*`
- `operational_*` remains a live-ops side branch

## Rollup safety

Rollup algebra is a repository-wide correctness rule, not an implementation
detail.

- Do not compute aggregates from already-aggregated values unless the aggregate is mathematically merge-safe.
- Safe rollups sum additive counters directly.
- Safe rollups recompute ratios from additive numerators and denominators.
- Safe rollups recompute means from additive sums and counts.
- Safe rollups merge explicit aggregate state for percentiles and other
  distribution metrics.
- Unsafe rollups include averaging averages, averaging ratios, recomputing
  percentiles from scalar percentile values, and summing overlapping
  classifications that are not additive.

Any new gold/read-model metric must define its additive support fields or
merge-safe state at the same time as the derived metric. If a higher-grain
consumer cannot recompute the metric safely from those fields, the metric
contract is incomplete.

## Layered domain architecture

Each component follows a strict layered model.
Code may only depend **forward** through the layer chain:

```
Types → Config → Repo → Service → Runtime
```

Cross-cutting concerns (telemetry, logger, Kafka client) enter through
**Providers only** and are injected into the layers that need them.

Violations are caught by repo validation and contract checks. See [`design-docs/architecture.md`](design-docs/architecture.md).

## Components

| Component | Language | Role |
|-----------|----------|------|
| `api/` | Go | REST API + enrichment worker |
| `infra/clickhouse/` | SQL / Docker | Schema, migrations, Kafka Engine config |
| `infra/prometheus/` | YAML | Prometheus scrape configuration |
| `infra/grafana/` | YAML / JSON | Grafana provisioning and dashboards |

## Key design decisions

- **ClickHouse Kafka Engine for ingest**: no application-layer consumer; ClickHouse reads Kafka directly and routes each record into `accepted_raw_events` or `ignored_raw_events`.
- **Single-service resolver scheduling**: the resolver owns backlog catch-up, historical late-arrival repair, same-day closed-hour auto-healing, queued bounded repair requests, and tail updates in one `auto` scheduler, with manual `backfill` / `repair-window` retained for operator intervention.
- **Canonical-first serving**: physical ingest tables stay in ClickHouse; dbt owns semantic SQL; the API reads `api_*` while downstream derivations and parity logic must use `canonical_*`.
- **Enrichment as a sidecar**: ENS name resolution and stake data come from the Livepeer public API via a background goroutine; kept separate from the ingest path so a slow or down enrichment API does not affect event processing.
- **Prometheus-native observability**: `/metrics` endpoint on the Go API; ClickHouse built-in endpoint on port 9363; Kafka Exporter as a sidecar. No custom instrumentation library.
- **Validate at boundaries**: every Kafka message is validated against a typed schema on ingestion. See `docs/design-docs/data-validation-rules.md` for the full behavioral contract.
- **Boring technology preferred**: composable, stable APIs are easier to reason about.

## Further reading

- [`design-docs/architecture.md`](design-docs/architecture.md) — layer rules and enforcement
- [`design-docs/core-beliefs.md`](design-docs/core-beliefs.md) — operating principles
- [`design-docs/adr-004-quality-aware-sla.md`](design-docs/adr-004-quality-aware-sla.md) — current `sla_score` rationale
- [`design-docs/data-validation-rules.md`](design-docs/data-validation-rules.md) — validation behavior contract
- [`product-specs/index.md`](product-specs/index.md) — active product specifications
