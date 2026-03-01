# Livepeer Network Analytics

[![CI Nightly Full](https://github.com/Cloud-SPE/livepeer-naap-analytics/actions/workflows/ci-nightly-full.yml/badge.svg)](https://github.com/Cloud-SPE/livepeer-naap-analytics/actions/workflows/ci-nightly-full.yml)
[![CI PR Smoke](https://github.com/Cloud-SPE/livepeer-naap-analytics/actions/workflows/ci-pr-smoke.yml/badge.svg)](https://github.com/Cloud-SPE/livepeer-naap-analytics/actions/workflows/ci-pr-smoke.yml)

Operator-first analytics pipeline for Livepeer telemetry. Events flow through Kafka and Flink into ClickHouse, with contract-tested facts and serving views for performance, reliability, and capacity/demand monitoring.

## Quick Start (Local Non-Prod)

These commands create a local, non-production analytics stack for local testing and debugging.

```bash
docker volume create livepeer-analytics-flink-maven-cache
docker compose up -d
docker compose ps
cd flink-jobs && mvn test
cd ..
tests/integration/run_all.sh
```

## Data Analysis Quick Start

View the NaaP overview dashboard in Grafana:

- URL: [http://localhost:3000/d/naap-overview/naap-overview](http://localhost:3000/d/naap-overview/naap-overview)
- Default login: `admin` / `admin`

View the saved exec summary snapshot:

- [INTEGRATION_EXEC_SUMMARY.ipynb](tests/python/notebooks/INTEGRATION_EXEC_SUMMARY.ipynb)
- Saved output may be outdated; run the notebook locally for current results.

Run the analysis notebook with JupyterLab via `uv`:

```bash
uv run --project tests/python jupyter lab tests/python/notebooks/INTEGRATION_EXEC_SUMMARY.ipynb
```

## Local Endpoints

- Stream test UI: [http://localhost:8088/](http://localhost:8088/)
- Grafana: [http://localhost:3000/](http://localhost:3000/)
- ClickHouse: [http://localhost:8123/dashboard](http://localhost:8123/dashboard)
- Flink UI: [http://localhost:8081/](http://localhost:8081/)

## Why This Exists

- Preserve replay-safe, auditable raw telemetry.
- Keep correctness-critical lifecycle logic in Flink.
- Serve stable analytics contracts from ClickHouse facts and views.

## Top Links

- Architecture overview: [docs/architecture/SYSTEM_OVERVIEW.md](docs/architecture/SYSTEM_OVERVIEW.md)
- Schema and metric contracts: [docs/data/SCHEMA_AND_METRIC_CONTRACTS.md](docs/data/SCHEMA_AND_METRIC_CONTRACTS.md)
- Testing and validation contracts: [docs/quality/TESTING_AND_VALIDATION.md](docs/quality/TESTING_AND_VALIDATION.md)
- Operations and release runbooks: [docs/operations/RUNBOOKS_AND_RELEASE.md](docs/operations/RUNBOOKS_AND_RELEASE.md)
- Canonical docs index: [docs/README.md](docs/README.md)

## Full Reference

All detailed content that previously lived in this README (task map, local quickstart details, CI gates, and agent navigation) is now canonical in:

- [docs/README.md#repository-operations-reference](docs/README.md#repository-operations-reference)
- [docs/README.md#agent-navigation](docs/README.md#agent-navigation)
- [AGENTS.md](AGENTS.md)
