# Design

Top-level architecture map for Livepeer NAAP Analytics.

## System overview

```
                    ┌─────────────────────────────────────────┐
  Livepeer Network  │              Kafka Cluster               │
  ─────────────────>│  naap.events.raw                        │
  (events/metrics)  │  naap.analytics.aggregated              │
                    │  naap.alerts                            │
                    └──────────────┬──────────────────────────┘
                                   │
                    ┌──────────────▼──────────────┐
                    │     Python Pipeline          │
                    │  (consume → process → emit)  │
                    └──────────────┬──────────────┘
                                   │ writes
                    ┌──────────────▼──────────────┐
                    │       Storage Layer          │
                    │  (TBD: Postgres / ClickHouse)│
                    └──────────────┬──────────────┘
                                   │ reads
                    ┌──────────────▼──────────────┐
                    │         Go API               │
                    │   (REST / query interface)   │
                    └─────────────────────────────┘
```

## Layered domain architecture

Each component (api, pipeline) follows a strict layered model.
Code may only depend **forward** through the layer chain:

```
Types → Config → Repo → Service → Runtime
```

Cross-cutting concerns (auth, telemetry, feature flags, Kafka client) enter through
**Providers only** and are injected into the layers that need them.

Violations are caught by structural linters. See `docs/design-docs/architecture.md`.

## Components

| Component | Language | Role |
|-----------|----------|------|
| `api/` | Go | REST API, query interface for analytics data |
| `pipeline/` | Python | Kafka consumer, event processing, aggregation |
| `infra/` | Docker/YAML | Kafka cluster, compose orchestration |

## Key design decisions

- **Kafka as the event bus**: all inter-component communication is async via Kafka topics.
- **Validate at boundaries**: every Kafka message is validated against a typed schema on ingestion.
- **No shared database between components**: each component owns its read model.
- **Boring technology preferred**: composable, stable APIs are easier for agents to reason about.

## Further reading

- `docs/design-docs/architecture.md` — layer rules, enforcement, dependency graph
- `docs/design-docs/core-beliefs.md` — operating principles
- `docs/product-specs/index.md` — feature specifications
- `docs/generated/schema.md` — Kafka topic schemas (auto-generated)
