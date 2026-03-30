# Design

Top-level architecture map for Livepeer NAAP Analytics.

## System overview

```
  Livepeer Network
  ─────────────────────────────────────────────────────────────────────────────
  network_events    ──Kafka──► ClickHouse Kafka Engine ──MV──► naap.accepted_raw_events
  streaming_events  ──Engine──► tables                    │         │
                                                           │         ├──► normalized_*
                                                           │         ├──► canonical_*
                                                           │         ├──► api_*
                                                           │         └──► ignored_raw_events
                                                           └──────────────────► HTTP :8000
                                                                                (Go API)

  Livepeer Public API                                                     Grafana :3000
  ─────────────────────────────────────────────────────────────────────────────
  /api/orchestrator  ──HTTP (5m)──► enrichment worker ──INSERT──► naap.orch_metadata
  /api/gateways      ──poll──►      (Go goroutine)     ──INSERT──► naap.gateway_metadata
  agg_orch_state     ──read──►      (same worker)      ──INSERT──► naap.agg_gpu_inventory

  Observability
  ─────────────────────────────────────────────────────────────────────────────
  Go API :8000/metrics           ──scrape──► Prometheus :9090 ──► Grafana :3000
  Resolver :9101/metrics          ──scrape──► Prometheus               │
  ClickHouse :9363               ──scrape──► Prometheus               │
  Kafka exporter :9308           ──scrape──► Prometheus               │
  naap.* tables         ◄──query───────────────────────────────
```

**Ingest path:** Two Kafka topics are consumed directly by ClickHouse via the Kafka Engine.
No application-layer consumer sits between Kafka and ClickHouse.

**Enrichment path:** A background Go goroutine polls the Livepeer public API every 5 minutes
and upserts orchestrator and gateway metadata (ENS names, stake, service URIs, deposits) into
dedicated ClickHouse tables. It also reads the current `agg_orch_state` snapshot to build a
structured GPU inventory in `agg_gpu_inventory`. All enrichment tables can be JOINed from any
aggregate query.

**Table population strategies:** Two distinct strategies are used for aggregate tables:
- **MV-populated** (event-driven): accepted Kafka events are routed into `naap.accepted_raw_events`, then normalized/core materialized views populate the downstream event-driven tables.
- **Worker-populated** (polled): `orch_metadata`, `gateway_metadata`, `agg_gpu_inventory` — written by the enrichment worker on a 5-minute interval. GPU inventory uses this strategy because `gpu_info` is a JSON map with dynamic integer keys that are trivial to iterate in Go but awkward in ClickHouse SQL.

**Serving path:** The Go API reads `api_*` relations only. `api_*` is a
presentation/read-model layer, not a source-of-truth layer. Downstream
derivations must read `canonical_*`, never `api_*`.

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
- `api_*` — service/dashboard read models only

This tier contract is documentation for semantic derivation flow. The physical
bootstrap also contains infrastructure/runtime namespaces such as `resolver_*`,
`agg_*`, `kafka_*`, metadata tables, and change/audit tables. Those objects are
supported as-is; we are not planning another broad schema rename just to force
every table into the semantic prefixes.

Medallion mapping is documentation-only:

- bronze = `raw_*`
- silver = `normalized_*`
- gold = `canonical_*`
- `operational_*` remains a live-ops side branch

## Layered domain architecture

Each component follows a strict layered model.
Code may only depend **forward** through the layer chain:

```
Types → Config → Repo → Service → Runtime
```

Cross-cutting concerns (telemetry, logger, Kafka client) enter through
**Providers only** and are injected into the layers that need them.

Violations are caught by structural linters. See `docs/design-docs/architecture.md`.

## Components

| Component | Language | Role |
|-----------|----------|------|
| `api/` | Go | REST API + enrichment worker |
| `infra/clickhouse/` | SQL / Docker | Schema, migrations, Kafka Engine config |
| `infra/prometheus/` | YAML | Prometheus scrape configuration |
| `infra/grafana/` | YAML / JSON | Grafana provisioning and dashboards |

## Key design decisions

- **ClickHouse Kafka Engine for ingest**: no application-layer consumer; ClickHouse reads Kafka directly and routes each record into `accepted_raw_events` or `ignored_raw_events`.
- **Single-service resolver scheduling**: the resolver owns backlog catch-up, historical late-arrival repair, and tail updates in one `auto` scheduler, with manual `backfill` / `repair-window` retained for operator intervention.
- **Canonical-first serving**: physical ingest tables stay in ClickHouse; dbt owns semantic SQL; the API reads `api_*` while downstream derivations and parity logic must use `canonical_*`.
- **Enrichment as a sidecar**: ENS name resolution and stake data come from the Livepeer public API via a background goroutine; kept separate from the ingest path so a slow or down enrichment API does not affect event processing.
- **Prometheus-native observability**: `/metrics` endpoint on the Go API; ClickHouse built-in endpoint on port 9363; Kafka Exporter as a sidecar. No custom instrumentation library.
- **Validate at boundaries**: every Kafka message is validated against a typed schema on ingestion. See `docs/design-docs/data-validation-rules.md` for the full behavioral contract.
- **Boring technology preferred**: composable, stable APIs are easier to reason about.

## Further reading

- `docs/design-docs/architecture.md` — layer rules, enforcement, dependency graph
- `docs/design-docs/core-beliefs.md` — operating principles
- `docs/design-docs/data-validation-rules.md` — data validation behavioral contract (17 rules, 31 tests)
- `docs/product-specs/index.md` — feature specifications (R1–R6)
