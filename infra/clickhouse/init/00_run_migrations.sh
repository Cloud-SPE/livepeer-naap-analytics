#!/bin/bash
# Applies all SQL migrations in /migrations in sorted order.
# Substitutes ${KAFKA_BROKER_LIST} in each file before execution.
# This script runs inside the ClickHouse container on first start
# (docker-entrypoint-initdb.d convention).

set -euo pipefail

MIGRATIONS_DIR="/migrations"
CH_USER="${CLICKHOUSE_USER:-naap_admin}"
CH_PASSWORD="${CLICKHOUSE_PASSWORD:-changeme}"
KAFKA_BROKER="${KAFKA_BROKER_LIST:-infra1.livepeer.cloud:9092}"

echo "[migrations] Starting ClickHouse schema migrations"
echo "[migrations] Kafka broker: ${KAFKA_BROKER}"

for f in $(ls "${MIGRATIONS_DIR}"/*.sql | sort); do
    echo "[migrations] Applying: $(basename "$f")"
    sed "s|\${KAFKA_BROKER_LIST}|${KAFKA_BROKER}|g" "$f" \
        | clickhouse-client \
            --host localhost \
            --user "${CH_USER}" \
            --password "${CH_PASSWORD}" \
            --multiquery
    echo "[migrations] Done:     $(basename "$f")"
done

echo "[migrations] All migrations applied successfully"
