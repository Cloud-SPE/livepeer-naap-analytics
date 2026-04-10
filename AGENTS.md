# AGENTS.md

This file is the repository table of contents. Keep it short.
Put detailed rules in `docs/` and link them here.

## What this project is

Livepeer NAAP Analytics is a ClickHouse-backed analytics API for the Livepeer AI
Network. Events flow from Kafka into ClickHouse through the Kafka Engine, are
published into semantic serving contracts by the resolver and dbt, and are
served through a Go REST API.

Start with [`docs/start-here.md`](docs/start-here.md).

## Repository map

```text
api/        Go REST API, resolver runtime, validation tests
infra/      ClickHouse, Kafka, Grafana, Prometheus, Docker assets
scripts/    Developer and operator utilities
warehouse/  dbt semantic layer and serving contracts
docs/       Human and agent documentation system of record
```

## Architecture

Layered dependencies flow forward only:

```text
Types -> Config -> Repo -> Service -> Runtime
```

Cross-cutting concerns enter through providers only.

See [`docs/design.md`](docs/design.md) for the top-level architecture map and
[`docs/design-docs/architecture.md`](docs/design-docs/architecture.md) for the
current layer rules and enforcement model.

## Documentation index

| File | Purpose |
|------|---------|
| [`docs/start-here.md`](docs/start-here.md) | Zero-context onboarding path |
| [`docs/index.md`](docs/index.md) | Full docs catalog |
| [`docs/design.md`](docs/design.md) | Architecture overview and tier contract |
| [`docs/product-sense.md`](docs/product-sense.md) | Product goals, non-goals, and success criteria |
| [`docs/plans.md`](docs/plans.md) | Active plans, completed plans, and technical debt |
| [`docs/backlog.md`](docs/backlog.md) | Current engineering backlog |
| [`docs/repository-guide.md`](docs/repository-guide.md) | Detailed repo map, common tasks, and deeper setup guidance |
| [`docs/design-docs/index.md`](docs/design-docs/index.md) | Design document index |
| [`docs/design-docs/attribution-rollup-coding-rules.md`](docs/design-docs/attribution-rollup-coding-rules.md) | Merge-blocking rules for attribution, inflation safety, and dashboard math |
| [`docs/operations/run-modes-and-recovery.md`](docs/operations/run-modes-and-recovery.md) | Resolver modes, rebuilds, and recovery |
| [`docs/operations/compose-services.md`](docs/operations/compose-services.md) | Local Compose runtime responsibilities |
| [`docs/operations/runtime-validation-and-performance.md`](docs/operations/runtime-validation-and-performance.md) | Runtime validation checklist |
| [`docs/references/inbound-kafka-contract.md`](docs/references/inbound-kafka-contract.md) | Active inbound Kafka contract for this repo |
| [`docs/generated/schema.md`](docs/generated/schema.md) | Generated bootstrap schema inventory |
| [`docs/product-specs/index.md`](docs/product-specs/index.md) | Product and API specifications |

## Working in this repo

- All important project knowledge should live in the repository.
- Use `docs/exec-plans/` for complex work.
- Prefer existing utilities in `api/internal/`, `warehouse/`, and `scripts/` over ad hoc helpers.
- Do not probe data shapes speculatively; rely on typed structs, OpenAPI, generated schema, and validation contracts.
- Keep docs and tests aligned with behavior in the same change.

## Quick start

```bash
make up
curl http://localhost:8000/healthz
make test
make lint
```
