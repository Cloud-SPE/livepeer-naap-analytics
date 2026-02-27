# Documentation System of Record

This repository uses a map-first documentation model for human and agent collaboration:

- `AGENTS.md` is the fast entry point.
- `docs/` is the canonical knowledge base.

## Canonical Doc Set

- Architecture: [docs/architecture/SYSTEM_OVERVIEW.md](architecture/SYSTEM_OVERVIEW.md)
- Data and metric contracts: [docs/data/SCHEMA_AND_METRIC_CONTRACTS.md](data/SCHEMA_AND_METRIC_CONTRACTS.md)
- Operations and release: [docs/operations/RUNBOOKS_AND_RELEASE.md](operations/RUNBOOKS_AND_RELEASE.md)
- Testing and validation: [docs/quality/TESTING_AND_VALIDATION.md](quality/TESTING_AND_VALIDATION.md)
- Engineering workflow: [docs/workflows/ENGINEERING_WORKFLOW.md](workflows/ENGINEERING_WORKFLOW.md)
- Code reuse and normalization guidelines: [docs/workflows/CODE_REUSE_AND_NORMALIZATION_GUIDELINES.md](workflows/CODE_REUSE_AND_NORMALIZATION_GUIDELINES.md)
- Flink lifecycle state machines: [docs/workflows/FLINK_LIFECYCLE_STATE_MACHINES.md](workflows/FLINK_LIFECYCLE_STATE_MACHINES.md)
- GPU attribution multi-candidate implementation plan: [docs/workflows/GPU_ATTRIBUTION_MULTI_CANDIDATE_PLAN.md](workflows/GPU_ATTRIBUTION_MULTI_CANDIDATE_PLAN.md)
- Capability enrichment ordering plan: [docs/workflows/CAPABILITY_ENRICHMENT_ORDERING_PLAN.md](workflows/CAPABILITY_ENRICHMENT_ORDERING_PLAN.md)
- Doc inventory and migration map: [docs/references/DOC_INVENTORY_AND_MIGRATION_MAP.md](references/DOC_INVENTORY_AND_MIGRATION_MAP.md)
- Consolidation review and next steps: [docs/references/DOC_CONSOLIDATION_PLAN.md](references/DOC_CONSOLIDATION_PLAN.md)
- Canonical issues backlog: [docs/references/ISSUES_BACKLOG.md](references/ISSUES_BACKLOG.md)
- Agent tools backlog: [docs/references/AGENT_TOOLS_BACKLOG.md](references/AGENT_TOOLS_BACKLOG.md)
- Canonical glossary: [docs/references/GLOSSARY.md](references/GLOSSARY.md)
- Docs automation helpers: [docs/automation/DOCS_TOOLING.md](automation/DOCS_TOOLING.md)
- Scripts reference: [docs/automation/SCRIPTS_REFERENCE.md](automation/SCRIPTS_REFERENCE.md)

## Quick Start by Goal

| Goal | Read first | Then use |
|---|---|---|
| Understand system flow end to end | [docs/architecture/SYSTEM_OVERVIEW.md](architecture/SYSTEM_OVERVIEW.md) | [docs/data/SCHEMA_AND_METRIC_CONTRACTS.md](data/SCHEMA_AND_METRIC_CONTRACTS.md) |
| Change schema/metrics safely | [docs/data/SCHEMA_AND_METRIC_CONTRACTS.md](data/SCHEMA_AND_METRIC_CONTRACTS.md) | [docs/quality/TESTING_AND_VALIDATION.md](quality/TESTING_AND_VALIDATION.md), [docs/workflows/ENGINEERING_WORKFLOW.md](workflows/ENGINEERING_WORKFLOW.md) |
| Understand lifecycle state transitions and attribution behavior | [docs/workflows/FLINK_LIFECYCLE_STATE_MACHINES.md](workflows/FLINK_LIFECYCLE_STATE_MACHINES.md) | [docs/data/SCHEMA_AND_METRIC_CONTRACTS.md](data/SCHEMA_AND_METRIC_CONTRACTS.md), lifecycle classes under `flink-jobs/src/main/java/com/livepeer/analytics/lifecycle/` |
| Deploy/redeploy/replay jobs | [docs/operations/RUNBOOKS_AND_RELEASE.md](operations/RUNBOOKS_AND_RELEASE.md) | [docs/operations/FLINK_DEPLOYMENT.md](operations/FLINK_DEPLOYMENT.md), [docs/operations/REPLAY_RUNBOOK.md](operations/REPLAY_RUNBOOK.md) |
| Investigate data quality issues | [docs/quality/TESTING_AND_VALIDATION.md](quality/TESTING_AND_VALIDATION.md) | [docs/quality/DATA_QUALITY.md](quality/DATA_QUALITY.md) |
| Find unresolved improvements | [docs/references/ISSUES_BACKLOG.md](references/ISSUES_BACKLOG.md) | [docs/references/METRICS_SCHEMA_DESIGN_SCRATCHPAD.md](references/METRICS_SCHEMA_DESIGN_SCRATCHPAD.md) (working context) |

## Working Rules

- Keep canonical docs concise and implementation-focused.
- Any schema or metric contract change must update both code and canonical docs in the same PR.

## Repository Operations Reference

This section is the canonical home for root-level onboarding and operations details.

### Project Scope and Limits

- Coverage is limited to gateways/configurations sending data to this Kafka ingest path.
- This does not monitor all Livepeer network activity out of the box.
- Payment and onchain analytics are not complete yet and are not a current quality gate.
- Grafana exists locally, but dashboard refresh is planned to match the latest metrics/monitoring model.

### Quick Start by Task

| Task | Run/Read This First | Then |
|---|---|---|
| Bring up local analytics stack | `docker compose up -d && docker compose ps` | [docs/operations/RUNBOOKS_AND_RELEASE.md](operations/RUNBOOKS_AND_RELEASE.md) |
| Validate pipeline health quickly | `cd flink-jobs && mvn test` | [tests/integration/run_all.sh](../tests/integration/run_all.sh) |
| Review high-level PASS/FAIL | `uv run --project tests/python jupyter lab tests/python/notebooks/INTEGRATION_EXEC_SUMMARY.ipynb` | [tests/python/notebooks/FLINK_DATA_TRACE_AND_INTEGRATION_TESTS.ipynb](../tests/python/notebooks/FLINK_DATA_TRACE_AND_INTEGRATION_TESTS.ipynb) |
| Debug a specific failing contract | [docs/quality/TESTING_AND_VALIDATION.md](quality/TESTING_AND_VALIDATION.md) | [tests/integration/sql/assertions_*.sql](../tests/integration/sql/) |
| Understand architecture/data model | [docs/architecture/SYSTEM_OVERVIEW.md](architecture/SYSTEM_OVERVIEW.md) | [docs/data/SCHEMA_AND_METRIC_CONTRACTS.md](data/SCHEMA_AND_METRIC_CONTRACTS.md) |

### Quickstart (Local, 5-10 Minutes)

This quickstart creates a local, non-production analytics stack for local testing and debugging.

Prerequisites:

- Docker + Docker Compose on a modern laptop/CPU.
- Optional: Cloudflare Zero Trust tunnel if you want remote access to local web apps.

1. Start the stack:

```bash
docker volume create livepeer-analytics-flink-maven-cache
docker compose up -d
docker compose ps
```

2. Run smoke validation:

```bash
cd flink-jobs && mvn test
cd ..
tests/integration/run_all.sh
```

Expected result:

- Java tests pass.
- Integration SQL packs report `0` failures for current fixture window/smoke scenario.

3. Inspect current activity:

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

### Data Analysis Quick Start

View the NaaP overview dashboard in Grafana:

- URL: [http://localhost:3000/d/naap-overview/naap-overview](http://localhost:3000/d/naap-overview/naap-overview)
- Default login: `admin` / `admin`

Run the analysis notebook with JupyterLab via `uv`:

```bash
uv run --project tests/python jupyter lab tests/python/notebooks/INTEGRATION_EXEC_SUMMARY.ipynb
```

### CI and Quality Gates

- PR gate (`CI PR Smoke`): Java tests + reduced docker integration smoke.
- Nightly (`CI Nightly Full`): full harness with richer diagnostics artifacts.
- Manual (`CI Manual Deep Verify`): on-demand full/smoke runs with window controls.

Primary quality contracts live in:

- [tests/integration/sql/assertions_pipeline.sql](../tests/integration/sql/assertions_pipeline.sql)
- [tests/integration/sql/assertions_api_readiness.sql](../tests/integration/sql/assertions_api_readiness.sql)
- [tests/integration/sql/assertions_raw_typed.sql](../tests/integration/sql/assertions_raw_typed.sql)
- [docs/quality/TESTING_AND_VALIDATION.md](quality/TESTING_AND_VALIDATION.md)

## Agent Navigation

- Operator/developer entry map: [AGENTS.md](../AGENTS.md)
- Canonical docs index: [docs/README.md](README.md)
- Specialized guides:
  - Docs updates: [docs/agents/docs-agent.md](agents/docs-agent.md)
  - Test/validation changes: [docs/agents/test-agent.md](agents/test-agent.md)
  - API/serving checks: [docs/agents/api-agent.md](agents/api-agent.md)
  - Deployment/runbook tasks: [docs/agents/dev-deploy-agent.md](agents/dev-deploy-agent.md)
  - Security and linting: [docs/agents/security-agent.md](agents/security-agent.md), [docs/agents/lint-agent.md](agents/lint-agent.md)
