# Python Tooling Environment (uv)

This notebook uses the shared Python environment in `tools/python/`.

Full environment guide: `tools/python/README.md`

## Install

From repo root:

```bash
uv sync --project tools/python
```

## Run JupyterLab

```bash
uv run --project tools/python jupyter lab docs/reports/notebook/FLINK_DATA_TRACE_AND_INTEGRATION_TESTS.ipynb
```

## Run ad-hoc Python with this env

```bash
uv run --project tools/python python -c "import clickhouse_connect, pandas, ipywidgets; print('ok')"
```

## Run repository Python scripts with this env

```bash
uv run --project tools/python python scripts/run_clickhouse_query_pack.py --lookback-hours 24
uv run --project tools/python python scripts/run_clickhouse_data_tests.py --sql-file tests/integration/sql/assertions_pipeline.sql --lookback-hours 24
```

## Notes

- Connection settings are read from environment variables used in the notebook (`CH_HOST`, `CH_PORT`, `CH_DATABASE`, `CH_USER`, `CH_PASSWORD`, `CH_SECURE`).
- Run from repository root so repository-relative paths in notebook cells and scripts resolve consistently.
