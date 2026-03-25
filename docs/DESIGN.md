# Design

Top-level architecture map for Livepeer NAAP Analytics.

## System overview

```
  Livepeer Network
  ─────────────────────────────────────────────────────────────────────────────
  network_events    ──Kafka──► ClickHouse Kafka Engine ──MV──► naap.events
  streaming_events  ──Engine──► tables                    │         │
                                                           │         └──► Aggregate MVs
                                                           │               agg_orch_state
                                                           │               agg_stream_*
                                                           │               agg_payment_*
                                                           │               agg_*_hourly
                                                           └──────────────────► HTTP :8000
                                                                                (Go API)

  Livepeer Public API
  ─────────────────────────────────────────────────────────────────────────────
  /api/orchestrator  ──HTTP (5m)──► enrichment worker ──INSERT──► naap.orch_metadata
  /api/gateways      ──poll──►      (Go goroutine)     ──INSERT──► naap.gateway_metadata

  Observability
  ─────────────────────────────────────────────────────────────────────────────
  Go API :8000/metrics  ──scrape──► Prometheus :9090 ──► Grafana :3000
  ClickHouse :9363      ──scrape──► Prometheus
  Kafka exporter :9308  ──scrape──► Prometheus
```

**Ingest path:** Two Kafka topics are consumed directly by ClickHouse via the Kafka Engine.
No application-layer consumer sits between Kafka and ClickHouse.

**Enrichment path:** A background Go goroutine polls the Livepeer public API every 5 minutes
and upserts orchestrator and gateway metadata (ENS names, stake, service URIs, deposits) into
dedicated ClickHouse tables. These tables can be JOINed from any aggregate query.

**Serving path:** The Go API queries only pre-aggregated ClickHouse tables. No raw-event
queries at request time; all heavy lifting happens at ingest via Materialized Views.

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

- **ClickHouse Kafka Engine for ingest**: no application-layer consumer; ClickHouse reads Kafka directly.
- **Pre-aggregated serving**: all aggregate tables are populated by Materialized Views at ingest time; the API never fans out raw events at query time.
- **Enrichment as a sidecar**: ENS name resolution and stake data come from the Livepeer public API via a background goroutine; kept separate from the ingest path so a slow or down enrichment API does not affect event processing.
- **Prometheus-native observability**: `/metrics` endpoint on the Go API; ClickHouse built-in endpoint on port 9363; Kafka Exporter as a sidecar. No custom instrumentation library.
- **Validate at boundaries**: every Kafka message is validated against a typed schema on ingestion. See `docs/design-docs/data-validation-rules.md` for the full behavioral contract.
- **Boring technology preferred**: composable, stable APIs are easier to reason about.

## Further reading

- `docs/design-docs/architecture.md` — layer rules, enforcement, dependency graph
- `docs/design-docs/core-beliefs.md` — operating principles
- `docs/design-docs/data-validation-rules.md` — data validation behavioral contract (17 rules, 31 tests)
- `docs/product-specs/index.md` — feature specifications (R1–R6)
