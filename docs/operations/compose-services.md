# Compose Services

For the higher-level runtime modes, see [`run-modes-and-recovery.md`](run-modes-and-recovery.md).
For the deployment diagrams, see [`../design-docs/system-visuals.md`](../design-docs/system-visuals.md).

This document explains what each Docker Compose service is responsible for and
whether it is part of the default runtime, an init step, optional tooling, or
validation-only infrastructure.

## Default Runtime

These services are started by [`make up`](../../Makefile).

| Service | Type | Responsibility |
| --- | --- | --- |
| `zookeeper` | long-running | Kafka coordination for the local single-broker stack. |
| `kafka` | long-running | Local event source for `network_events` and `streaming_events`. |
| `clickhouse` | long-running | Physical ingest, storage, materialized views, resolver state, and serving-store tables. |
| `warehouse-init` | one-shot init | Publishes dbt semantic `canonical_*` and `api_*` views into ClickHouse after startup. This is the Compose equivalent of an init container. |
| `resolver` | long-running | Repairs dirty windows, publishes current state, and maintains serving stores. |
| `api` | long-running | Read-only REST API plus enrichment worker. Starts after `warehouse-init` succeeds so semantic views exist on fresh volumes. |
| `prometheus` | long-running | Scrapes API, resolver, ClickHouse, Kafka exporter, cAdvisor, and node-exporter metrics. |
| `grafana` | long-running | Dashboard UI over ClickHouse and Prometheus. |
| `kafka-ui` | long-running | Local Kafka inspection UI for topic and consumer debugging. |
| `kafka-exporter` | long-running | Prometheus exporter for Kafka broker, consumer-group, and lag metrics. |
| `node-exporter` | long-running | Host-level system metrics for Prometheus. |
| `cadvisor` | long-running | Container CPU, memory, and network metrics for Prometheus/Grafana. |

## Optional Tooling Profile

These services are not started by default. Use the `tooling` profile or the
Make targets that wrap it.

| Service | Type | Responsibility |
| --- | --- | --- |
| `warehouse` | optional long-running helper | Idle dbt toolbox container for manual `run`, `compile`, and `test` commands. Start it with `make up-tooling` only if you want a persistent shell target for dbt work. |

Useful commands:

```bash
make up-tooling
make warehouse-run
make warehouse-compile
make warehouse-test
```

If you previously started the tooling profile, `make down` tears it down too.

## Validation Profile

These services are isolated from the normal local runtime. They exist so tests
and bootstrap extraction do not depend on whatever state happens to be in the
main local ClickHouse volume.

| Service | Type | Responsibility |
| --- | --- | --- |
| `validation-clickhouse` | long-running, profile-scoped | Disposable ClickHouse instance for validation and baseline extraction. |
| `warehouse-validation` | one-shot init | Runs dbt publication against `validation-clickhouse` before validation tests execute. |
| `validation-go` | one-shot test runner | Runs `go test -tags=validation ./internal/validation/...` against the isolated validation database. |

Useful commands:

```bash
make test-validation-clean
docker compose --profile validation up -d validation-clickhouse
docker compose --profile validation run --rm warehouse-validation
make bootstrap-extract
```

## What Is Not Always-On

- `warehouse-init` is required on startup, but it is not a steady-state service.
- `warehouse` is not part of freshness or request serving. It is manual tooling only.
- Validation services are not part of the product runtime. They are regression and bootstrap safety infrastructure.

## Responsibility Boundaries

- Freshness belongs to `resolver`, not to `warehouse` or `warehouse-init`.
- Semantic view publication belongs to dbt (`warehouse-init` or manual `warehouse` commands).
- API request serving belongs to `api` and should only read published `api_*` contracts.
- Observability belongs to `prometheus`, `grafana`, `kafka-exporter`, `node-exporter`, and `cadvisor`.
