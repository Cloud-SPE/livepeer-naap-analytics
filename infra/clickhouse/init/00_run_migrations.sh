#!/bin/bash
# Applies all SQL migrations in /migrations in sorted order.
# Substitutes ${KAFKA_BROKER_LIST} in each file before execution.
#
# Note: ClickHouse Kafka Engine tables do not support ALTER TABLE MODIFY SETTING,
# so the broker address must be baked in at CREATE TABLE time via substitution.
# The offset policy (auto_offset_reset) is set separately in
# infra/clickhouse/config/kafka.xml, which ClickHouse reads at startup.
#
# This script runs inside the ClickHouse container on first start
# (docker-entrypoint-initdb.d convention).

set -euo pipefail

MIGRATIONS_DIR="/migrations"
CH_USER="${CLICKHOUSE_USER:-naap_admin}"
CH_PASSWORD="${CLICKHOUSE_PASSWORD:-changeme}"
KAFKA_BROKER="${KAFKA_BROKER_LIST:-infra2.cloudspe.com:9092}"
KAFKA_NETWORK_GROUP="${KAFKA_NETWORK_GROUP:-clickhouse-naap-network}"
KAFKA_STREAMING_GROUP="${KAFKA_STREAMING_GROUP:-clickhouse-naap-streaming}"
CH_WRITER_PASSWORD="${CLICKHOUSE_WRITER_PASSWORD:?CLICKHOUSE_WRITER_PASSWORD is required}"
CH_READER_PASSWORD="${CLICKHOUSE_READER_PASSWORD:?CLICKHOUSE_READER_PASSWORD is required}"

echo "[migrations] Starting ClickHouse schema migrations"
echo "[migrations] Kafka broker:           ${KAFKA_BROKER}"
echo "[migrations] Network consumer group:  ${KAFKA_NETWORK_GROUP}"
echo "[migrations] Streaming consumer group: ${KAFKA_STREAMING_GROUP}"

for f in $(ls "${MIGRATIONS_DIR}"/*.sql | sort); do
    echo "[migrations] Applying: $(basename "$f")"
    sed \
        -e "s|\${KAFKA_BROKER_LIST}|${KAFKA_BROKER}|g" \
        -e "s|\${KAFKA_NETWORK_GROUP}|${KAFKA_NETWORK_GROUP}|g" \
        -e "s|\${KAFKA_STREAMING_GROUP}|${KAFKA_STREAMING_GROUP}|g" \
        -e "s|\${CLICKHOUSE_WRITER_PASSWORD}|${CH_WRITER_PASSWORD}|g" \
        -e "s|\${CLICKHOUSE_READER_PASSWORD}|${CH_READER_PASSWORD}|g" \
        "$f" \
        | clickhouse-client \
            --host localhost \
            --user "${CH_USER}" \
            --password "${CH_PASSWORD}" \
            --multiquery
    echo "[migrations] Done:     $(basename "$f")"
done

echo "[migrations] All migrations applied successfully"
