# Notebook Environment (uv)

This `uv` project is only for running the analytics notebook and related exploratory queries.

## Install

From repo root:

```bash
cd notebook-env
uv sync
```

## Run JupyterLab

```bash
uv run jupyter lab ../documentation/reports/FLINK_DATA_TRACE_AND_INTEGRATION_TESTS.ipynb
```

## Run ad-hoc Python with this env

```bash
uv run python -c "import clickhouse_connect, pandas, ipywidgets; print('ok')"
```

## Notes

- Connection settings are read from environment variables used in the notebook (`CH_HOST`, `CH_PORT`, `CH_DATABASE`, `CH_USER`, `CH_PASSWORD`, `CH_SECURE`).
- The notebook references SQL files via `../../tests/integration/sql/...` paths; run Jupyter with the repository as working context (command above does this).
