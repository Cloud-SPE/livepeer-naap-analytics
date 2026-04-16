# Repository Guide

This document preserves the deeper repo map and common working commands that do
not belong on the root landing page.

## Repository Layout

```text
api/
  cmd/server/           API server entrypoint
  cmd/resolver/         Resolver service entrypoint
  internal/config/      Environment config
  internal/providers/   Cross-cutting providers
  internal/repo/        ClickHouse query layer
  internal/service/     Business logic
  internal/runtime/     HTTP handlers, middleware, metrics, server wiring
  internal/resolver/    Resolver logic
  internal/validation/  Scenario-based data-quality harness

warehouse/
  models/staging/       Thin views over normalized tables
  models/canonical/     Canonical semantic derivations
  models/api_base/      Internal semantic helper views
  models/api/           Published API and dashboard read models

infra/
  clickhouse/           Bootstrap, migrations, runtime config
  docker/               Dockerfiles
  grafana/              Dashboards and provisioning
  kafka/                Kafka provisioning references
  prometheus/           Scrape config

deploy/
  infra1/               Consumer-side production deployment material
  infra2/               Kafka-side production deployment material
```

## Common Local Commands

- `make up`: Start the local Docker Compose runtime and build images as needed.
- `make down`: Stop the local Docker Compose stack and remove containers, volumes, and orphans.
- `make test`: Run the Go unit test suite with the race detector enabled.
- `make lint`: Run Go vet and staticcheck for the API code.
- `make test-validation-clean`: Run the full validation regression suite against a fresh isolated validation stack.
- `make warehouse-run`: Manually run dbt publication for the warehouse serving contracts.
- `make ch-query`: Open an interactive ClickHouse shell with the local admin user.
- `make resolver-logs`: Tail resolver logs from the local Compose stack.

## Local Workflow

```bash
cp .env.example .env
make up
curl http://localhost:8000/healthz
make test
make test-validation-clean
```

Useful local endpoints:

- API docs: `http://localhost:8000/docs`
- Grafana: `http://localhost:3000`
- Prometheus: `http://localhost:9090`
- Kafka UI: `http://localhost:8080`

## Where To Go Deeper

- Architecture and tier contract: [`design.md`](design.md)
- Design details and ADRs: [`design-docs/index.md`](design-docs/index.md)
- Run modes and recovery: [`operations/run-modes-and-recovery.md`](operations/run-modes-and-recovery.md)
- Deployment and maintenance: [`operations/operations-runbook.md`](operations/operations-runbook.md)
- Warehouse ownership: [`../warehouse/README.md`](../warehouse/README.md)
- ClickHouse bootstrap and migrations: [`../infra/clickhouse/README.md`](../infra/clickhouse/README.md)
