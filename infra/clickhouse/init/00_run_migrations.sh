#!/bin/bash
set -euo pipefail

SCHEMA_MODE="${CLICKHOUSE_SCHEMA_MODE:-bootstrap}"

case "${SCHEMA_MODE}" in
    bootstrap)
        echo "[bootstrap] Starting ClickHouse schema bootstrap via ch-migrate"
        bash "$(dirname "$0")/ch-migrate.sh" bootstrap
        echo "[bootstrap] Applying any forward migrations after bootstrap"
        bash "$(dirname "$0")/ch-migrate.sh" up
        echo "[bootstrap] ClickHouse bootstrap applied successfully"
        ;;
    migrations)
        echo "[migrations] Starting ClickHouse schema migrations via ch-migrate"
        bash "$(dirname "$0")/ch-migrate.sh" up
        echo "[migrations] All migrations applied successfully"
        ;;
    *)
        echo "[schema] unknown CLICKHOUSE_SCHEMA_MODE=${SCHEMA_MODE}; expected bootstrap or migrations" >&2
        exit 1
        ;;
esac
