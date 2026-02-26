# Python Tooling Environment (uv)

This notebook uses the shared Python environment in `tests/python/`.

Full environment guide: `tests/python/README.md`

## Install

From repo root:

```bash
uv sync --project tests/python
```

## Run JupyterLab

```bash
uv run --project tests/python jupyter lab tests/python/notebooks/FLINK_DATA_TRACE_AND_INTEGRATION_TESTS.ipynb
```

## Notebooks

- Executive summary (fast PASS/FAIL review):
  - `tests/python/notebooks/INTEGRATION_EXEC_SUMMARY.ipynb`
- Deep dive (traceability, diagnostics, row-level debugging):
  - `tests/python/notebooks/FLINK_DATA_TRACE_AND_INTEGRATION_TESTS.ipynb`

## Run ad-hoc Python with this env

```bash
uv run --project tests/python python -c "import clickhouse_connect, pandas, ipywidgets; print('ok')"
```

## Run repository Python scripts with this env

```bash
uv run --project tests/python python tests/python/scripts/run_clickhouse_query_pack.py --lookback-hours 24
uv run --project tests/python python tests/python/scripts/run_clickhouse_data_tests.py --sql-file tests/integration/sql/assertions_pipeline.sql --lookback-hours 24
```

## Notes

- Connection settings are read from environment variables used in the notebook (`CH_HOST`, `CH_PORT`, `CH_DATABASE`, `CH_USER`, `CH_PASSWORD`, `CH_SECURE`).
- Notebooks share path/bootstrap helpers from `tests/python/notebooks/notebook_shared.py`.
- Repo root discovery is automatic from current working directory and parent paths.
- If Jupyter is launched outside the repo tree, set `REPO_ROOT=/abs/path/to/livepeer-naap-analytics` before opening notebooks.
