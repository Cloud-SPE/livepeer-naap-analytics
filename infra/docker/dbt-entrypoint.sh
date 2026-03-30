#!/bin/bash
set -euo pipefail

# Pass-through mode: if args are given, run dbt directly.
# Used by: docker compose run --rm warehouse run|test|...
if [ $# -gt 0 ]; then
    exec dbt "$@"
fi

# Tooling mode: publish dbt relations once on startup, then stay idle so the
# container remains available for manual run/test/compile commands. Freshness is
# owned by the canonical refresh worker, not by recurring dbt cron.
AUTO_RUN="${DBT_AUTO_RUN_ON_START:-true}"

if [ "${AUTO_RUN}" = "true" ]; then
    echo "[dbt-tooling] Running initial dbt run..."
    dbt run
    echo "[dbt-tooling] Running dbt compile for canonical refresh artifacts..."
    dbt compile
fi

echo "[dbt-tooling] Idle container started; use docker compose run --rm warehouse <dbt-command> for manual actions."
exec tail -f /dev/null
