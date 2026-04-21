# Start Here

If you are new to this repository, use this path:

1. Read [`../README.md`](../README.md) for the project summary and local boot command.
2. Read [`design.md`](design.md) for the architecture and serving contract.
3. Read [`repository-guide.md`](repository-guide.md) for the repo map, common commands, and deeper setup guidance.
4. Use [`index.md`](index.md) to branch into operations, references, plans, and product specs.

## What This Stack Does

This repo ingests Livepeer AI Network Kafka events into ClickHouse, normalizes
and corrects them, publishes semantic serving models, and exposes those models
through a Go API and Grafana.

The main runtime owners are:

- ClickHouse for physical ingest and storage
- the resolver for corrected current state and serving-store publication
- dbt for semantic `canonical_*` and `api_*` views over published stores
- the Go API for public HTTP serving

## 5-Minute Local Start

```bash
cp .env.example .env
make up
curl http://localhost:8000/healthz
```

Then use:

- `http://localhost:8000/docs` for the API
- `http://localhost:3000` for Grafana
- `http://localhost:9090` for Prometheus
- `http://localhost:8080` for Kafka UI

## Choose Your Next Doc

- Build or debug locally: [`repository-guide.md`](repository-guide.md)
- Understand the system contract: [`design.md`](design.md)
- Understand layer rules and validation behavior: [`design-docs/index.md`](design-docs/index.md)
- Operate or recover the stack: [`operations/run-modes-and-recovery.md`](operations/run-modes-and-recovery.md)
- Understand inbound events: [`references/inbound-kafka-contract.md`](references/inbound-kafka-contract.md)
- Understand current open work: [`backlog.md`](backlog.md)
