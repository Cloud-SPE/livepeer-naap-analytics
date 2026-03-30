#!/bin/bash
set -euo pipefail

MODE="${1:-up}"
MIGRATIONS_DIR="${MIGRATIONS_DIR:-/migrations}"
BOOTSTRAP_SQL="${BOOTSTRAP_SQL:-/bootstrap/v1.sql}"
CH_HOST="${CH_HOST:-localhost}"
CH_USER="${CLICKHOUSE_USER:-naap_admin}"
CH_PASSWORD="${CLICKHOUSE_PASSWORD:-changeme}"
KAFKA_BROKER="${KAFKA_BROKER_LIST:-infra2.cloudspe.com:9092}"
KAFKA_NETWORK_GROUP="${KAFKA_NETWORK_GROUP:-clickhouse-naap-network}"
KAFKA_STREAMING_GROUP="${KAFKA_STREAMING_GROUP:-clickhouse-naap-streaming}"
CH_WRITER_PASSWORD="${CLICKHOUSE_WRITER_PASSWORD:?CLICKHOUSE_WRITER_PASSWORD is required}"
CH_READER_PASSWORD="${CLICKHOUSE_READER_PASSWORD:?CLICKHOUSE_READER_PASSWORD is required}"

ch_query() {
    clickhouse-client \
        --host "${CH_HOST}" \
        --user "${CH_USER}" \
        --password "${CH_PASSWORD}" \
        --multiquery \
        --query "$1"
}

render_sql() {
    local file="$1"
    sed \
        -e "s|\${KAFKA_BROKER_LIST}|${KAFKA_BROKER}|g" \
        -e "s|\${KAFKA_NETWORK_GROUP}|${KAFKA_NETWORK_GROUP}|g" \
        -e "s|\${KAFKA_STREAMING_GROUP}|${KAFKA_STREAMING_GROUP}|g" \
        -e "s|\${CLICKHOUSE_WRITER_PASSWORD}|${CH_WRITER_PASSWORD}|g" \
        -e "s|\${CLICKHOUSE_READER_PASSWORD}|${CH_READER_PASSWORD}|g" \
        "$file"
}

ensure_history_table() {
    ch_query "
        CREATE TABLE IF NOT EXISTS naap.schema_migrations
        (
            file_name String,
            checksum String,
            applied_at DateTime64(3, 'UTC') DEFAULT now64(),
            runner_version String DEFAULT 'ch-migrate-v1'
        )
        ENGINE = ReplacingMergeTree(applied_at)
        ORDER BY file_name
    "
}

current_checksum() {
    local file_name="$1"
    clickhouse-client \
        --host "${CH_HOST}" \
        --user "${CH_USER}" \
        --password "${CH_PASSWORD}" \
        --query "SELECT ifNull(anyLast(checksum), '') FROM naap.schema_migrations FINAL WHERE file_name = '${file_name}'"
}

record_migration() {
    local file_name="$1"
    local checksum="$2"
    ch_query "
        INSERT INTO naap.schema_migrations (file_name, checksum)
        VALUES ('${file_name}', '${checksum}')
    "
}

run_up() {
    shopt -s nullglob
    local files=("${MIGRATIONS_DIR}"/*.sql)
    shopt -u nullglob
    if [ "${#files[@]}" -eq 0 ]; then
        echo "[migrations] no forward migrations to apply"
        return 0
    fi
    ensure_history_table
    for f in $(printf '%s\n' "${files[@]}" | sort); do
        local_name="$(basename "$f")"
        local_sum="$(sha256sum "$f" | awk '{print $1}')"
        applied_sum="$(current_checksum "${local_name}")"
        if [ "${applied_sum}" = "${local_sum}" ]; then
            echo "[migrations] Skipping ${local_name} (already applied)"
            continue
        fi
        if [ -n "${applied_sum}" ] && [ "${applied_sum}" != "${local_sum}" ]; then
            echo "[migrations] checksum mismatch for ${local_name}: applied=${applied_sum} current=${local_sum}" >&2
            exit 1
        fi

        echo "[migrations] Applying ${local_name}"
        render_sql "$f" | clickhouse-client \
            --host "${CH_HOST}" \
            --user "${CH_USER}" \
            --password "${CH_PASSWORD}" \
            --multiquery
        record_migration "${local_name}" "${local_sum}"
        echo "[migrations] Done ${local_name}"
    done
}

run_status() {
    shopt -s nullglob
    local files=("${MIGRATIONS_DIR}"/*.sql)
    shopt -u nullglob
    if [ "${#files[@]}" -eq 0 ]; then
        echo "[migrations] no forward migrations present"
        return 0
    fi
    ensure_history_table
    for f in $(printf '%s\n' "${files[@]}" | sort); do
        local_name="$(basename "$f")"
        local_sum="$(sha256sum "$f" | awk '{print $1}')"
        applied_sum="$(current_checksum "${local_name}")"
        status="pending"
        if [ "${applied_sum}" = "${local_sum}" ]; then
            status="applied"
        elif [ -n "${applied_sum}" ]; then
            status="checksum_mismatch"
        fi
        echo "${local_name} ${status}"
    done
}

run_validate() {
    shopt -s nullglob
    local files=("${MIGRATIONS_DIR}"/*.sql)
    shopt -u nullglob
    if [ "${#files[@]}" -eq 0 ]; then
        echo "[migrations] no forward migrations to validate"
        return 0
    fi
    ensure_history_table
    local failed=0
    for f in $(printf '%s\n' "${files[@]}" | sort); do
        local_name="$(basename "$f")"
        local_sum="$(sha256sum "$f" | awk '{print $1}')"
        applied_sum="$(current_checksum "${local_name}")"
        if [ -n "${applied_sum}" ] && [ "${applied_sum}" != "${local_sum}" ]; then
            echo "[migrations] invalid checksum for ${local_name}: applied=${applied_sum} current=${local_sum}" >&2
            failed=1
        fi
    done
    if [ "${failed}" -ne 0 ]; then
        exit 1
    fi
    echo "[migrations] validation ok"
}

run_dry_run() {
    shopt -s nullglob
    local files=("${MIGRATIONS_DIR}"/*.sql)
    shopt -u nullglob
    if [ "${#files[@]}" -eq 0 ]; then
        echo "[migrations] no forward migrations to apply"
        return 0
    fi
    for f in $(printf '%s\n' "${files[@]}" | sort); do
        local_name="$(basename "$f")"
        echo "[migrations] would apply ${local_name}"
    done
}

run_bootstrap() {
    if [ ! -f "${BOOTSTRAP_SQL}" ]; then
        echo "[bootstrap] bootstrap file not found: ${BOOTSTRAP_SQL}" >&2
        exit 1
    fi
    echo "[bootstrap] Applying ${BOOTSTRAP_SQL}"
    render_sql "${BOOTSTRAP_SQL}" | clickhouse-client \
        --host "${CH_HOST}" \
        --user "${CH_USER}" \
        --password "${CH_PASSWORD}" \
        --multiquery
    echo "[bootstrap] Bootstrap applied successfully"
}

# ClickHouse sources .sh files in /docker-entrypoint-initdb.d. This script must
# only run when invoked explicitly by 00_run_migrations.sh, not when sourced as
# a plain shell fragment during init discovery.
if [ "${BASH_SOURCE[0]}" != "$0" ]; then
    return 0
fi

case "${MODE}" in
    up) run_up ;;
    bootstrap) run_bootstrap ;;
    status) run_status ;;
    validate) run_validate ;;
    dry-run) run_dry_run ;;
    *)
        echo "usage: $0 {up|bootstrap|status|validate|dry-run}" >&2
        exit 1
        ;;
esac
