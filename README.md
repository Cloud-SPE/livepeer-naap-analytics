# Livepeer NaaP Analytics

[![CI Nightly Full](https://github.com/Cloud-SPE/livepeer-naap-analytics/actions/workflows/ci-nightly-full.yml/badge.svg)](https://github.com/Cloud-SPE/livepeer-naap-analytics/actions/workflows/ci-nightly-full.yml)
[![CI PR Smoke](https://github.com/Cloud-SPE/livepeer-naap-analytics/actions/workflows/ci-pr-smoke.yml/badge.svg)](https://github.com/Cloud-SPE/livepeer-naap-analytics/actions/workflows/ci-pr-smoke.yml)

Operator-first analytics pipeline for Livepeer NaaP telemetry. This project ingests gateway events, validates and transforms them with Flink, stores them in ClickHouse, and exposes contract-tested serving views so operators and contributors can reason about capability coverage, performance, and capacity/demand behavior.

## What This Project Offers

- A validated raw -> typed -> silver -> gold analytics pipeline.
- Contract assertions for lifecycle semantics, projection parity, and API readiness.
- Reproducible local stack for debugging and integration checks.
- Notebook and SQL tooling for quick status review and deep-dive tracing.

## Quick Start by Task

| Task | Run/Read This First | Then |
|---|---|---|
| Bring up local analytics stack | `docker compose up -d && docker compose ps` | `docs/operations/RUNBOOKS_AND_RELEASE.md` |
| Validate pipeline health quickly | `cd flink-jobs && mvn test` | `tests/integration/run_all.sh` |
| Review high-level PASS/FAIL | `uv run --project tests/python jupyter lab tests/python/notebooks/INTEGRATION_EXEC_SUMMARY.ipynb` | `tests/python/notebooks/FLINK_DATA_TRACE_AND_INTEGRATION_TESTS.ipynb` |
| Debug a specific failing contract | `docs/quality/TESTING_AND_VALIDATION.md` | `tests/integration/sql/assertions_*.sql` |
| Understand architecture/data model | `docs/architecture/SYSTEM_OVERVIEW.md` | `docs/data/SCHEMA_AND_METRIC_CONTRACTS.md` |

## Scope and Limits

- Coverage is limited to gateways/configurations sending data to this Kafka ingest path.
- This does not monitor all Livepeer network activity out of the box.
- Payment and onchain analytics are not complete yet and are not a current quality gate.
- Grafana exists locally, but dashboard refresh is planned to match the latest metrics/monitoring model.

## Quickstart (Local, 5-10 Minutes)

Prerequisites:

- Docker + Docker Compose on a modern laptop/CPU.
- Optional: Cloudflare Zero Trust tunnel if you want remote access to local web apps.

### 1) Start the stack

```bash
docker compose up -d
docker compose ps
```

### 2) Run smoke validation

```bash
cd flink-jobs && mvn test
cd ..
tests/integration/run_all.sh
```

Expected result:

- Java tests pass.
- Integration SQL packs report `0` failures for current fixture window/smoke scenario.

### 3) Inspect current activity

Quick trace pack:

```bash
uv run --project tests/python python tests/python/scripts/run_clickhouse_query_pack.py --lookback-hours 24
```

Executive PASS/FAIL notebook:

```bash
uv run --project tests/python jupyter lab tests/python/notebooks/INTEGRATION_EXEC_SUMMARY.ipynb
```

Deep-dive trace notebook:

```bash
uv run --project tests/python jupyter lab tests/python/notebooks/FLINK_DATA_TRACE_AND_INTEGRATION_TESTS.ipynb
```

## CI and Quality Gates

- PR gate (`CI PR Smoke`): Java tests + reduced docker integration smoke.
- Nightly (`CI Nightly Full`): full harness with richer diagnostics artifacts.
- Manual (`CI Manual Deep Verify`): on-demand full/smoke runs with window controls.

Primary quality contracts live in:

- `tests/integration/sql/assertions_pipeline.sql`
- `tests/integration/sql/assertions_api_readiness.sql`
- `tests/integration/sql/assertions_raw_typed.sql`
- `docs/quality/TESTING_AND_VALIDATION.md`

## Local Endpoints

- Stream test UI: `http://localhost:8088/`
- Grafana: `http://localhost:3000/`
- ClickHouse: `http://localhost:8123/dashboard`
- Flink UI: `http://localhost:8081/`

## Where To Go Next

| Task | Start Here | Then Use |
|---|---|---|
| Understand architecture/data flow | `docs/architecture/SYSTEM_OVERVIEW.md` | `docs/data/SCHEMA_AND_METRIC_CONTRACTS.md` |
| Run and interpret validation | `docs/quality/TESTING_AND_VALIDATION.md` | `tests/integration/sql/*.sql` |
| Deploy/replay operations | `docs/operations/RUNBOOKS_AND_RELEASE.md` | `docs/operations/REPLAY_RUNBOOK.md` |
| Contributor workflow | `docs/workflows/ENGINEERING_WORKFLOW.md` | `AGENTS.md` |

## Documentation

- Canonical docs index: `docs/README.md`
- Agent/operator quick map: `AGENTS.md`

## Agent Navigation

- Operator/developer entry map: `AGENTS.md`
- Canonical docs index: `docs/README.md`
- Specialized guides:
  - Docs updates: `docs/agents/docs-agent.md`
  - Test/validation changes: `docs/agents/test-agent.md`
  - API/serving checks: `docs/agents/api-agent.md`
  - Deployment/runbook tasks: `docs/agents/dev-deploy-agent.md`
  - Security and linting: `docs/agents/security-agent.md`, `docs/agents/lint-agent.md`
