# AGENTS.md

This file is a short operating map for AI agents and humans. Treat `docs/` as the system of record and use this file to find the right source quickly.

## Commands (Run Early)

- Start stack: `docker compose up -d`
- Check stack health: `docker compose ps`
- Build Flink job: `cd flink-jobs && mvn -q -DskipTests package`
- Run Flink unit/contract tests: `cd flink-jobs && mvn test`
- Run ClickHouse integration assertions: `tests/integration/run_all.sh`
- Re-run scenario assertions only (no replay/reset): `cd flink-jobs && mvn -Pscenario-it-assert-only validate`
- Run query trace pack: `uv run --project tests/python python tests/python/scripts/run_clickhouse_query_pack.py --lookback-hours 24`
- Run docs inventory: `scripts/docs_inventory.sh`
- Validate markdown links: `scripts/docs_link_check.sh`

## Read Order

1. `docs/README.md`
2. `docs/architecture/SYSTEM_OVERVIEW.md`
3. `docs/data/SCHEMA_AND_METRIC_CONTRACTS.md`
4. `docs/operations/RUNBOOKS_AND_RELEASE.md`
5. `docs/quality/TESTING_AND_VALIDATION.md`
6. `docs/references/DOC_INVENTORY_AND_MIGRATION_MAP.md`

## Fast Path by Task

| Task | Start here | Then use |
|---|---|---|
| Understand architecture and data flow | `docs/architecture/SYSTEM_OVERVIEW.md` | `docs/data/SCHEMA_AND_METRIC_CONTRACTS.md` |
| Change schema, parser, or mappings | `docs/data/SCHEMA_AND_METRIC_CONTRACTS.md` | `docs/quality/TESTING_AND_VALIDATION.md`, `docs/workflows/ENGINEERING_WORKFLOW.md` |
| Operate deployment and replay | `docs/operations/RUNBOOKS_AND_RELEASE.md` | `docs/operations/FLINK_DEPLOYMENT.md`, `docs/operations/REPLAY_RUNBOOK.md` |
| Debug quality issues (DLQ/quarantine/dedup) | `docs/quality/TESTING_AND_VALIDATION.md` | `docs/quality/DATA_QUALITY.md` |
| Review open feature work | `docs/references/ISSUES_BACKLOG.md` | `docs/references/METRICS_SCHEMA_DESIGN_SCRATCHPAD.md` |

## Project Snapshot

- Domain: Livepeer NaaP analytics for live AI video workflows.
- Runtime stack:
  - Kafka `apache/kafka:3.9.0`
  - Flink `1.20.3` (`flink:1.20.3-java11`)
  - ClickHouse `24.11`
  - Grafana `latest` with ClickHouse plugin
  - MinIO for cold/object storage
- Core code:
  - Flink pipeline: `flink-jobs/src/main/java/com/livepeer/analytics/pipeline/StreamingEventsToClickHouse.java`
  - ClickHouse schema: `configs/clickhouse-init/01-schema.sql`
  - Integration SQL: `tests/integration/sql/`

## Core Design Choices (Do Not Drift)

- Flink owns correctness-critical logic:
  - schema validation, dedup, session identity, lifecycle classification, stateful correlations.
- ClickHouse owns serving logic:
  - non-stateful projections, rollups, and API views.
- Stateful lifecycle facts are Flink-emitted:
  - `fact_workflow_sessions`
  - `fact_workflow_session_segments`
  - `fact_workflow_param_updates`
- Non-stateful facts are ClickHouse-MV derived:
  - `fact_stream_status_samples`
  - `fact_stream_trace_edges`
  - `fact_stream_ingest_samples`
- Contract stability:
  - metric semantics changes require versioning and parity validation windows.

## Source of Truth by Topic

- Architecture: `docs/architecture/SYSTEM_OVERVIEW.md`
- Schema + metric contracts: `docs/data/SCHEMA_AND_METRIC_CONTRACTS.md`
- Ops + release: `docs/operations/RUNBOOKS_AND_RELEASE.md`
- Tests + validation: `docs/quality/TESTING_AND_VALIDATION.md`
- Canonical glossary: `docs/references/GLOSSARY.md`

## Change Playbooks

### Event/schema change

1. Update `configs/clickhouse-init/01-schema.sql`.
2. Update parsers/models/mappers:
   - `flink-jobs/src/main/java/com/livepeer/analytics/parse/EventParsers.java`
   - `flink-jobs/src/main/java/com/livepeer/analytics/model/EventPayloads.java`
   - `flink-jobs/src/main/java/com/livepeer/analytics/sink/ClickHouseRowMappers.java`
3. Run tests:
   - `cd flink-jobs && mvn test`
4. Re-run integration assertions:
   - `tests/integration/run_all.sh`
5. Update docs:
   - canonical docs in `docs/`
   - migration/reference map if file ownership changed.

### Lifecycle semantics change

1. Update lifecycle contract: `docs/data/SCHEMA_AND_METRIC_CONTRACTS.md`.
2. Update SQL validation packs:
   - `docs/reports/METRICS_VALIDATION_QUERIES.sql`
   - `tests/integration/sql/assertions_api_readiness.sql`
3. Version semantics fields (for example `edge_semantics_version`) and run parity checks.

### Pipeline assertion SQL change

1. Update `tests/integration/sql/assertions_pipeline.sql`.
2. Co-update the requirement map in `docs/quality/TESTING_AND_VALIDATION.md`:
   - section: `Pipeline Assertions Requirement Map`.
   - rule: every `-- TEST:` block in SQL must have a corresponding row in the table.
3. Re-run pipeline assertions and notebook checks to confirm no contract regressions.

### General Contract-Drift Gate (Apply to Any Refactor)

Use this checklist for any change that can alter schema, semantics, serving views, or analysis output.

1. Update implementation + contracts together in the same change:
   - code (`flink-jobs` / SQL),
   - schema/contracts docs (`docs/data/*`),
   - validation docs (`docs/quality/*`).
2. Run deterministic checks in this order:
   - `cd flink-jobs && mvn test`
   - `tests/integration/run_all.sh`
   - `uv run --project tests/python python tests/python/scripts/run_clickhouse_query_pack.py --lookback-hours 24`
3. Keep assertion SQL and assertion-map docs synchronized:
   - `tests/integration/sql/assertions_pipeline.sql`
   - `docs/quality/TESTING_AND_VALIDATION.md` (Pipeline Assertions Requirement Map)
4. Keep notebook diagnostics aligned with contract semantics:
   - `tests/python/notebooks/FLINK_DATA_TRACE_AND_INTEGRATION_TESTS.ipynb`
5. If any check cannot be run, explicitly record what was skipped and residual risk in PR notes.

## Boundaries

- Always:
  - Prefer deterministic, testable changes over query-time heuristics.
- Ask first:
  - database schema drops or destructive migrations,
  - dependency upgrades,
  - CI/CD behavior changes.
- Never:
  - commit secrets/keys,
  - edit data under `data/` as a workaround,
  - silently change metric definitions without doc + validation updates.

## Documentation Governance (Prevent Sprawl)

- Canonical structure is fixed under `docs/`:
  - architecture: `docs/architecture/`
  - data contracts: `docs/data/`
  - operations: `docs/operations/`
  - quality/testing: `docs/quality/`
  - workflow/process: `docs/workflows/`
  - references and governance: `docs/references/`
  - historical evidence: `docs/reports/`
- New docs must go into an existing canonical bucket above.
- Do not create new top-level documentation folders without explicit approval.
- Do not create ad-hoc scratch docs at repo root.
- Working/scratch content belongs only in explicitly marked reference files (for example `docs/references/METRICS_SCHEMA_DESIGN_SCRATCHPAD.md`), not scattered new files.
- Any add/move/delete of docs must co-update:
  - `docs/README.md` (if canonical navigation changed)
- Prefer editing existing canonical docs over creating near-duplicate files.
- Before finishing any docs PR, run:
  - `scripts/docs_inventory.sh`
  - `scripts/docs_link_check.sh`

## Specialist Agents

Use focused agents in `docs/agents/`:

- `docs/agents/docs-agent.md`
- `docs/agents/test-agent.md`
- `docs/agents/lint-agent.md`
- `docs/agents/api-agent.md`
- `docs/agents/dev-deploy-agent.md`
- `docs/agents/security-agent.md`
