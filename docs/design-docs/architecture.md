# Architecture

Detailed per-component architecture, layer rules, and enforcement model.

## Layered domain model

Each component follows a strict layered model. Within a component, code may only
import from the **same or earlier** layer. Forward imports are forbidden.

```
Types → Config → Repo → Service → Runtime
```

### Layer responsibilities

| Layer | Go package | Python module | Responsibility |
|-------|-----------|---------------|---------------|
| **Types** | `internal/types` | `src/types` | Domain structs, enums, Kafka message schemas. No dependencies except stdlib. |
| **Config** | `internal/config` | `src/config` | Configuration loading from env/files. Depends on Types only. |
| **Repo** | `internal/repo` | `src/repo` | Data access interfaces and implementations. Depends on Types, Config. |
| **Service** | `internal/service` | `src/service` | Business logic. Depends on Types, Config, Repo. |
| **Runtime** | `internal/runtime` | `src/runtime` | Entry points: HTTP handlers (Go), Kafka consumers (Python). Wires everything together. |
| **Providers** | `internal/providers` | `src/providers` | Cross-cutting concerns only. Injected; not imported by layers. |

### Cross-cutting concerns (Providers)

Providers are the only exception to the forward-only rule.
They are injected at the Runtime layer and passed down via interfaces.

Current providers:
- **Telemetry** — OpenTelemetry tracer and meter
- **Kafka client** — shared producer/consumer factory
- **Logger** — structured logger (zap / structlog)

Future providers (when needed):
- Auth (JWT validation, API key lookup)
- Feature flags

## Enforcement

### Go (api/)

- `go vet` catches obvious issues
- Layer import checks: `scripts/check-layers-go.sh` (to be generated)
  - Runs `grep` for disallowed cross-layer imports per package
  - Exits non-zero if violations found; prints the offending import and remediation hint

### Python (pipeline/)

- `ruff` for linting and formatting
- `mypy` for type checking (strict mode)
- Layer import checks: `scripts/check-layers-python.sh` (to be generated)
- All Kafka message types must use Pydantic models — no `dict` at service boundaries

## Kafka topic naming

```
naap.<domain>.<event-type>
```

Examples:
- `naap.events.raw` — raw inbound events from the Livepeer network
- `naap.analytics.aggregated` — processed aggregation results
- `naap.alerts` — threshold breach alerts

Topic definitions: `infra/kafka/topics.yaml`
Schema reference: `docs/generated/schema.md`

## Analytics tier contract

The analytics storage/runtime contract uses explicit semantic tiers:

- `raw_*` — accepted raw envelopes
- `normalized_*` — normalized event-family records
- `canonical_*` — authoritative corrected derivation source
- `operational_*` — low-latency live ops tables only
- `api_*` — service-facing read models only

Those prefixes describe semantic tiers and allowed derivation flow. They do not
mean every physical ClickHouse table must use one of those prefixes. The
supported bootstrap intentionally also includes infrastructure/runtime tables
such as `accepted_raw_events`, `ignored_raw_events`, `kafka_*`, `resolver_*`,
`agg_*`, metadata tables, and change/audit tables.

Allowed data flow:

```text
raw_* -> normalized_* -> canonical_* -> api_*
raw_* / normalized_* -> operational_*
```

Forbidden data flow:

- `api_* -> canonical_*`
- `operational_* -> canonical_*`
- any downstream derivation sourcing truth from `api_*`

Future consumer surfaces may add new namespaces such as `dashboard_*`,
`export_*`, `partner_*`, or `feature_*`, but they must derive from
`canonical_*`, never from `api_*` or `operational_*`.

## Dependency injection pattern

### Go

Providers are constructed in `main.go` and passed to the Runtime.
The Runtime passes interfaces (not concrete types) to Service and Repo layers.

```go
// main.go
providers := providers.New(cfg)
runtime := runtime.New(cfg, providers, service.New(cfg, repo.New(cfg)))
runtime.Start()
```

### Python

Providers are constructed in the Runtime entry point and injected via constructor.

```python
# runtime/consumer.py
telemetry = TelemetryProvider(settings)
kafka = KafkaProvider(settings)
processor = EventProcessor(repo=EventRepo(settings), telemetry=telemetry)
consumer = KafkaConsumer(kafka=kafka, processor=processor)
consumer.run()
```

## File size and complexity limits

- No single Go file should exceed 400 lines
- No single Python file should exceed 300 lines
- No function should exceed 60 lines
- Violations are caught by linters with remediation messages
