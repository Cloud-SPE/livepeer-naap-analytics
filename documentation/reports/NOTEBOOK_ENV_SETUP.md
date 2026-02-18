# Notebook Environment Setup (uv)

Use the dedicated `uv` project in `notebook-env/` to run the notebook and widgets.

## 1) Create environment

```bash
cd notebook-env
uv sync
```

This installs:

- `ipywidgets`
- `jupyterlab`
- `clickhouse-connect`
- `pandas`

## 2) Start notebook

```bash
uv run jupyter lab ../documentation/reports/FLINK_DATA_TRACE_AND_INTEGRATION_TESTS.ipynb
```

## 3) Configure ClickHouse connection (optional env vars)

```bash
export CH_HOST=clickhouse.livepeer.cloud
export CH_PORT=8123
export CH_DATABASE=livepeer_analytics
export CH_USER=analytics_user
export CH_PASSWORD=analytics_password
export CH_SECURE=false
```

## 4) Notebook workflow

In order, run:

1. `Ordered Pipeline Trace (Raw -> API)`
2. `Integration Assertions (CI-aligned)`
3. `Scenario Candidate Discovery`
4. `Raw -> Silver Correlation Checks`
5. `Session Timeline Drill-Down`
6. `Interactive Session Edge Explorer`

The interactive explorer lets a reader select a scenario candidate session and render all relevant raw edges/events timeline for that session.
