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
  - `uv run --project tools/python python scripts/run_clickhouse_data_tests.py --sql-file tests/integration/sql/assertions_api_readiness.sql --lookback-hours 24`
