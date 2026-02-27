# Python Tooling Environment (uv)

This is the shared Python environment for:

- notebook workflows under `tests/python/notebooks/`
- Python scripts under `tests/python/scripts/` that require `clickhouse-connect`

## Install

From repo root:

```bash
uv sync --project tests/python
```

## Run JupyterLab

```bash
uv run --project tests/python jupyter lab tests/python/notebooks/FLINK_DATA_TRACE_AND_INTEGRATION_TESTS.ipynb
```

## Run repository Python scripts

```bash
uv run --project tests/python python tests/python/scripts/run_clickhouse_query_pack.py --lookback-hours 24
uv run --project tests/python python tests/python/scripts/run_clickhouse_data_tests.py --sql-file tests/integration/sql/assertions_pipeline.sql --lookback-hours 24
```

## Notes

- Connection settings are read from environment variables:
  - `CH_HOST`, `CH_PORT`, `CH_DATABASE`, `CH_USER`, `CH_PASSWORD`, `CH_SECURE`
- Run from repository root so repository-relative paths resolve consistently.
