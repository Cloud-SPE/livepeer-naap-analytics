# Scripts Reference

Canonical reference for repository scripts under `scripts/`.

## Quick Usage Pattern

- Scripts are intended to be run from repo root.
- Shared Python environment for scripts and notebook is `tools/python/`.
- Preferred execution pattern for Python scripts:
  - `uv run --project tools/python python <script> [args...]`
- ClickHouse scripts read connection defaults from:
  - `CH_HOST` (default `localhost`)
  - `CH_PORT` (default `8123`)
  - `CH_DATABASE` (default `livepeer_analytics`)
  - `CH_USER` (default `analytics_user`)
  - `CH_PASSWORD` (default `analytics_password`)
  - `CH_SECURE` (`true/false` style)

## Script Catalog

| Script | Type | Purpose | Example |
|---|---|---|---|
| `scripts/docs_inventory.sh` | shell | Inventory markdown files with line counts and first heading. | `scripts/docs_inventory.sh` |
| `scripts/docs_link_check.sh` | shell | Validate local markdown links in repo docs. | `scripts/docs_link_check.sh` |
| `scripts/run_clickhouse_query_pack.py` | python | Run ordered SQL query packs marked with `-- QUERY:` blocks. | `uv run --project tools/python python scripts/run_clickhouse_query_pack.py --lookback-hours 24` |
| `scripts/run_clickhouse_data_tests.py` | python | Run SQL assertions marked with `-- TEST:` blocks and fail on nonzero `failed_rows`. | `uv run --project tools/python python scripts/run_clickhouse_data_tests.py --sql-file tests/integration/sql/assertions_pipeline.sql --lookback-hours 24` |
| `scripts/export_scenario_fixtures.py` | python | Export scenario-based ClickHouse fixtures from production/test data windows. | `uv run --project tools/python python scripts/export_scenario_fixtures.py --from-ts 2026-02-01T00:00:00Z --to-ts 2026-02-16T00:00:00Z` |
| `scripts/load_scenario_fixtures.py` | python | Load exported fixture JSONL data into ClickHouse from manifest metadata. | `uv run --project tools/python python scripts/load_scenario_fixtures.py --manifest tests/integration/fixtures/manifest.json --truncate` |
| `scripts/replay_scenario_events.py` | python | Replay fixture raw events to Kafka input topic for Flink end-to-end validation. | `python scripts/replay_scenario_events.py --manifest tests/integration/fixtures/manifest.json` |
| `scripts/run_scenario_test_harness.py` | python | Stage-based integration harness with report + per-stage logs. | `python scripts/run_scenario_test_harness.py --mode full` |
| `scripts/export_clickhouse_samples.py` | python | Export sampled rows from key ClickHouse event tables to JSONL. | `uv run --project tools/python python scripts/export_clickhouse_samples.py --output scripts/livepeer_samples.jsonl --lookback-hours 24` |
| `scripts/validate_metrics_jsonl.py` | python | Generate markdown feasibility report from JSONL samples. | `uv run --project tools/python python scripts/validate_metrics_jsonl.py --input scripts/livepeer_samples.jsonl --report docs/reports/METRICS_VALIDATION_REPORT.generated.md` |

## Details by Script

### `scripts/docs_inventory.sh`

- Inputs:
  - optional root path argument (defaults to `.`).
- Output:
  - markdown table to stdout (`Lines`, `Path`, `First Heading`).
- Common use:
  - `scripts/docs_inventory.sh`
  - `scripts/docs_inventory.sh docs`

### `scripts/docs_link_check.sh`

- Inputs:
  - optional root path argument (defaults to `.`).
- Output:
  - prints missing local markdown link targets; exits nonzero on failure.
- Common use:
  - `scripts/docs_link_check.sh`

### `scripts/run_clickhouse_query_pack.py`

- Required inputs:
  - SQL file with `-- QUERY:` blocks (default `tests/integration/sql/trace_pipeline_flow.sql`).
- Useful flags:
  - `--sql-file <path>`
  - `--lookback-hours <n>` or `--from-ts <UTC> --to-ts <UTC>`
  - `--max-rows <n>`
- Output:
  - ordered query result sets printed to stdout.

### `scripts/run_clickhouse_data_tests.py`

- Required inputs:
  - SQL file(s) with `-- TEST:` blocks returning a `failed_rows` column.
- Useful flags:
  - `--sql-file <path>` (repeatable)
  - `--lookback-hours <n>` or `--from-ts <UTC> --to-ts <UTC>`
  - `--json-out <path>`
- Output:
  - pass/fail summary; optional JSON report file.

### `scripts/export_scenario_fixtures.py`

- Required inputs:
  - `--from-ts <UTC>`
  - `--to-ts <UTC>`
- Optional flags:
  - `--scenario-sql` (default `tests/integration/sql/scenario_candidates.sql`)
  - `--output-dir` (default `tests/integration/fixtures`)
  - `--limit-per-scenario`
  - `--session-padding-minutes`
  - `--open-session-duration-minutes`
  - `--allow-missing-scenarios` / `--require-all-scenarios`
- Output:
  - scenario fixture JSONL files + manifest metadata.

### `scripts/load_scenario_fixtures.py`

- Required inputs:
  - `--manifest <path>`
- Optional flags:
  - `--truncate` (truncate touched tables before insert).
- Output:
  - inserts rows into ClickHouse fixture tables from manifest-listed files.

### `scripts/run_scenario_test_harness.py`

- Purpose:
  - Orchestrate integration-test stages with explicit boundaries and run artifacts.
  - Replay fixture events through Kafka -> Flink -> ClickHouse and validate with SQL contracts.
- Useful flags:
  - `--mode full|smoke`
  - `--stage <stage>` (repeatable; overrides mode stage list)
  - `--manifest <path>`
  - `--compose-files <csv>`
  - `--down-volumes` (include `docker compose down --volumes` on teardown)
  - `--keep-stack-on-fail` (leave stack running for manual investigation)
  - `--[no-]capture-logs-on-fail` (default on; writes compose diagnostics to artifacts)
  - `--docker-log-tail <n>` (default `2000`, `0` for full logs)
  - `--pipeline-ready-timeout-seconds <n>` / `--pipeline-ready-poll-seconds <n>`
  - `--required-connector <name>` (repeatable; defaults include raw ClickHouse + MinIO sinks)
  - `--scenario <name>` (repeatable; replay filter)
  - `--kafka-container`, `--kafka-bootstrap-server`, `--kafka-input-topic`
  - `--keep-stack-on-fail`
  - `--state-strategy preserve|reset|backup-reset`
  - `--allow-destructive-reset` (required for `reset`/`backup-reset`)
  - `--state-path <dir>` (repeatable; override default data paths)
  - `--backup-root <dir>` (base path for `backup-reset`)
  - `--lookback-hours <n>`
  - `--scenario-lookback-hours <n>`
- Outputs:
  - `artifacts/test-runs/<run_id>/harness.log`
  - `artifacts/test-runs/<run_id>/stages/<stage>.log`
  - `artifacts/test-runs/<run_id>/summary.json`
  - `artifacts/test-runs/<run_id>/report.md`
  - `artifacts/state-backups/<run_id>/...` (when `--state-strategy backup-reset`)
  - `artifacts/test-runs/<run_id>/diagnostics/docker_compose_ps.log` (on failure)
  - `artifacts/test-runs/<run_id>/diagnostics/docker_compose_logs.log` (on failure)

Smoke/full stage order now includes `pipeline_ready` before replay, which waits for:
- at least one RUNNING Flink job (`http://localhost:8081/jobs/overview`)
- required Kafka Connect connectors in RUNNING state (`http://localhost:8083/connectors/<name>/status`)

Maven `scenario-it` profiles default to:
- `--compose-files docker-compose.yml,docker-compose.scenario.yml`
- `--down-volumes`
This keeps scenario runs isolated from host `./data` bind mounts and avoids manual `chmod 777` workflows.

Compose file behavior:
- `docker-compose.yml` is the base stack.
- `docker-compose.scenario.yml` is an override used for scenario/integration runs.
- Compose merges files in order (`-f base -f override`), so scenario settings replace selected service fields from base and add scenario-only services/volumes.

Before first scenario run on a machine/runner:
- `docker volume create livepeer-analytics-flink-maven-cache`
- This volume is intentionally external so Maven cache survives scenario teardowns and avoids repeated dependency downloads.

Maven profiles:
- `cd flink-jobs && mvn -Pscenario-it-smoke verify`
  - CI-friendly smoke run; tears down scenario volumes at end.
- `cd flink-jobs && mvn -Pscenario-it-smoke-debug verify`
  - Local debug smoke run; does not run `stack_down`, so stack and scenario volumes remain up after both pass and fail for post-run notebook/SQL inspection.
  - Use `docker compose -f docker-compose.yml -f docker-compose.scenario.yml down --remove-orphans` when you are done debugging.

### `scripts/replay_scenario_events.py`

- Purpose:
  - Read fixture JSONL files from a manifest and publish raw event envelopes to Kafka.
  - Enables full-stack Flink path validation from captured production scenarios.
- Useful flags:
  - `--manifest <path>`
  - `--scenario <name>` (repeatable)
  - `--kafka-container <container>`
  - `--bootstrap-server <host:port>`
  - `--topic <topic>`
  - `--json-out <path>`
- Output:
  - stdout JSON summary (counts by scenario/table and replay totals)
  - optional JSON summary file via `--json-out`

### `scripts/export_clickhouse_samples.py`

- Required inputs:
  - `--output <path>`
- Optional flags:
  - `--tables <t1 t2 ...>`
  - `--per-sample <n>`
  - `--lookback-hours <n>`
  - `--older-hours <n>`
  - `--random-sample`
- Output:
  - JSONL sample file for downstream analysis.

### `scripts/validate_metrics_jsonl.py`

- Optional flags:
  - `--input` (default `scripts/livepeer_samples.jsonl`)
  - `--report` (default `documentation/reports/METRICS_VALIDATION_REPORT.md`)
- Output:
  - markdown metrics feasibility report generated from JSONL sample input.

## Notes

- Default output path for `scripts/validate_metrics_jsonl.py` is `docs/reports/METRICS_VALIDATION_REPORT.generated.md`.
- For contract-critical checks, prefer:
  - `tests/integration/run_all.sh`
  - `uv run --project tools/python python scripts/run_clickhouse_data_tests.py --sql-file tests/integration/sql/assertions_raw_typed.sql --lookback-hours 24`
  - `uv run --project tools/python python scripts/run_clickhouse_data_tests.py --sql-file tests/integration/sql/assertions_api_readiness.sql --lookback-hours 24`

## Notebook Alignment (Scenario Runs)

To run `docs/reports/notebook/FLINK_DATA_TRACE_AND_INTEGRATION_TESTS.ipynb` against local scenario output:

1. Execute a scenario run:
   - `cd flink-jobs && mvn -Pscenario-it-smoke verify`
2. Export local ClickHouse env:
   - `export CH_HOST=localhost CH_PORT=8123 CH_DATABASE=livepeer_analytics CH_USER=analytics_user CH_PASSWORD=analytics_password`
3. Reuse exact run window:
   - `cat artifacts/test-runs/<run_id>/stages/assert_raw_typed.json`
   - use `from_ts` / `to_ts` in notebook
4. Launch notebook:
   - `uv run --project tools/python jupyter lab`

This avoids comparing notebook output from production host windows against local harness assertions.
