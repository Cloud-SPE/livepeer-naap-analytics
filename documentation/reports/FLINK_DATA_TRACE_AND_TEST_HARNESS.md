# Flink Data Trace + Test Harness

This package gives you a single path from ad-hoc inspection to repeatable integration tests.

## Artifacts

- Notebook: `documentation/reports/FLINK_DATA_TRACE_AND_INTEGRATION_TESTS.ipynb`
- Ordered trace queries: `tests/integration/sql/trace_pipeline_flow.sql`
- Core assertions: `tests/integration/sql/assertions_pipeline.sql`
- Scenario candidate discovery: `tests/integration/sql/scenario_candidates.sql`
- Scenario existence assertions: `tests/integration/sql/assertions_scenario_candidates.sql`
- Query pack runner: `scripts/run_clickhouse_query_pack.py`
- Assertion runner: `scripts/run_clickhouse_data_tests.py`
- Fixture exporter: `scripts/export_scenario_fixtures.py`
- Fixture loader: `scripts/load_scenario_fixtures.py`

## Prerequisites

```bash
pip install clickhouse-connect pandas
```

Preferred notebook environment setup with `uv`:

- `documentation/reports/NOTEBOOK_ENV_SETUP.md`

Connection env vars (optional defaults shown):

- `CH_HOST` (`localhost`)
- `CH_PORT` (`8123`)
- `CH_DATABASE` (`livepeer_analytics`)
- `CH_USER` (`analytics_user`)
- `CH_PASSWORD` (`analytics_password`)
- `CH_SECURE` (`false`)

## 1) Run Ordered Data Trace

```bash
python scripts/run_clickhouse_query_pack.py --lookback-hours 24
```

This runs raw -> typed -> silver -> stateful -> rollup -> API parity queries in order.

## 2) Run Integration Assertions

Core pipeline contract:

```bash
python scripts/run_clickhouse_data_tests.py \
  --sql-file tests/integration/sql/assertions_pipeline.sql \
  --lookback-hours 24
```

Scenario existence checks (longer window recommended):

```bash
python scripts/run_clickhouse_data_tests.py \
  --sql-file tests/integration/sql/assertions_scenario_candidates.sql \
  --lookback-hours 720
```

One-shot wrapper:

```bash
tests/integration/run_all.sh
```

## 3) Export Production Fixtures for Scenario Tests

```bash
python scripts/export_scenario_fixtures.py \
  --from-ts 2026-01-01T00:00:00Z \
  --to-ts 2026-02-16T00:00:00Z \
  --limit-per-scenario 3
```

Output:

- snapshot folder under `tests/integration/fixtures/prod_snapshot_<timestamp>/`
- one JSONL file per scenario/session
- `manifest.json` with selected sessions + table row counts

Load a snapshot into a ClickHouse test database:

```bash
python scripts/load_scenario_fixtures.py \
  --manifest tests/integration/fixtures/prod_snapshot_<timestamp>/manifest.json \
  --database livepeer_analytics_test \
  --truncate
```

Then run assertions against the test database:

```bash
python scripts/run_clickhouse_data_tests.py \
  --database livepeer_analytics_test \
  --sql-file tests/integration/sql/assertions_pipeline.sql \
  --from-ts 2026-01-01T00:00:00Z \
  --to-ts 2026-02-16T00:00:00Z
```

## Scenario Mapping

- `scenario_1_clean_success_no_swap_fps_gt_12`
- `scenario_2_no_orchestrator_then_closed`
- `scenario_3_success_with_swap`
- `scenario_4_success_with_param_updates`
