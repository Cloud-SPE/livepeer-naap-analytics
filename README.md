# <img src="docs/naap_icon.png" alt="NaaP icon" width="80" valign="middle" /> NaaP Analytics

Livepeer NaaP Analytics is a ClickHouse-backed analytics platform for the
Livepeer AI Network. Kafka topics are ingested directly into ClickHouse through
the Kafka Engine, corrected and published by the resolver and dbt, and exposed
through a Go REST API and Grafana dashboards.

This repository is an analytics surface for the AI Network. It does not run
media transcoding workloads or cover the legacy broadcaster/transcoder stack;
its streaming coverage is limited to analytics for Livepeer AI
`live-video-to-video` sessions and Batch AI job traffic.

The supported serving spine is:

1. Kafka topics land in ClickHouse Kafka Engine tables.
2. Ingest materialized views route records into `accepted_raw_events` and `ignored_raw_events`.
3. `normalized_*` tables capture event-family facts.
4. The resolver publishes corrected latest-state and serving inputs into `canonical_*_store` and selected `api_*_store` tables.
5. dbt publishes `canonical_*`, internal `api_base_*`, and public `api_*` views.
6. The Go API and Grafana read those serving contracts.

## Quick Start

```bash
cp .env.example .env
make up
curl http://localhost:8000/healthz
```

`make up` starts the always-on local runtime and includes the one-shot
`warehouse-init` service so fresh volumes receive the published `canonical_*`,
`api_base_*`, and `api_*` relations without keeping a dbt container idling in
the default stack.

Local service entrypoints:

- API docs: `http://localhost:8000/docs`
- Grafana: `http://localhost:3000`
- Prometheus: `http://localhost:9090`
- Kafka UI: `http://localhost:8080`

Common local commands:

- `make down`: Stop the local Docker Compose stack and remove containers, volumes, and orphans.
- `make test`: Run the Go unit test suite with the race detector enabled.
- `make lint`: Run Go vet and staticcheck for the API code.
- `make test-validation-clean`: Run the full validation regression suite against a fresh isolated validation stack.
- `make warehouse-run`: Manually run dbt publication for the warehouse serving contracts.
- `make ch-query`: Open an interactive ClickHouse shell with the local admin user.
- `make resolver-logs`: Tail resolver logs from the local Compose stack.

## Read Next

- [`docs/start-here.md`](docs/start-here.md) for zero-context onboarding
- [`docs/index.md`](docs/index.md) for the full docs catalog
- [`docs/repository-guide.md`](docs/repository-guide.md) for the deeper repo map and common commands
- [`docs/design.md`](docs/design.md) for the architecture and tier contract
- [`docs/operations/run-modes-and-recovery.md`](docs/operations/run-modes-and-recovery.md) for runtime and rebuild procedures
- [`docs/generated/schema.md`](docs/generated/schema.md) for the generated bootstrap schema inventory

## Repository Layout

```text
api/        Go REST API, resolver runtime, validation tests
infra/      ClickHouse, Kafka, Grafana, Prometheus, Docker assets
scripts/    Developer and operator utilities
warehouse/  dbt semantic layer and serving contracts
docs/       Documentation system of record
deploy/     Production deployment material
```

## Runtime Surfaces

Local ports and interfaces most operators need first:

| Port | Surface |
|---|---|
| `8000` | Go API, Swagger UI, and Prometheus `/metrics` |
| `3000` | Grafana |
| `9090` | Prometheus |
| `8080` | Kafka UI |
| `8123` | ClickHouse HTTP |
| `9000` | ClickHouse native |
| `9102` | Resolver Prometheus metrics |

Primary API groups:

- `/v1/net/*` for orchestrators, models, and capacity
- `/v1/perf/*` for model performance
- `/v1/sla/*` for compliance metrics
- `/v1/network/*` for network demand
- `/v1/gpu/*` for GPU demand and metrics
- `/v1/ai-batch/*` for AI batch job analytics
- `/v1/byoc/*` for BYOC job analytics
- `/v1/jobs/*` for unified request/response job analytics
- `/v1/dashboard/*` for dashboard-facing read models
