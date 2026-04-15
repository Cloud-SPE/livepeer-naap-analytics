#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

CLICKHOUSE_SERVICE="${CLICKHOUSE_SERVICE:-clickhouse}"
CH_USER="${CH_USER:-naap_admin}"
CH_PASSWORD="${CH_PASSWORD:-changeme}"
CH_DB="${CH_DB:-naap}"
TABLES="${TABLES:-accepted_raw_events ignored_raw_events}"
RAW_DIR="${1:-${RAW_DIR:-}}"
TRUNCATE_FIRST="${TRUNCATE_FIRST:-0}"

usage() {
  cat <<EOF2
Import retained raw ClickHouse tables exported by scripts/export_retained_raw.sh.

Usage:
  scripts/import_retained_raw.sh <export_dir>

Environment overrides:
  CLICKHOUSE_SERVICE=clickhouse
  CH_USER=naap_admin
  CH_PASSWORD=changeme
  CH_DB=naap
  TABLES="accepted_raw_events ignored_raw_events"
  TRUNCATE_FIRST=0

Notes:
  - The target database and tables must already exist.
  - Use TRUNCATE_FIRST=1 only when you want to replace existing raw contents.
  - After import, run the retained-raw replay steps to rebuild normalized, canonical,
    canonical store, and api serving tables.
EOF2
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ -z "$RAW_DIR" ]]; then
  usage
  exit 1
fi

if [[ ! -d "$RAW_DIR" ]]; then
  echo "raw export directory not found: $RAW_DIR" >&2
  exit 1
fi

for table in $TABLES; do
  file="$RAW_DIR/${table}.native"
  if [[ ! -f "$file" ]]; then
    echo "missing export file: $file" >&2
    exit 1
  fi

  if [[ "$TRUNCATE_FIRST" == "1" ]]; then
    echo "==> truncating ${CH_DB}.${table}"
    docker compose exec -T "$CLICKHOUSE_SERVICE" clickhouse-client \
      --user "$CH_USER" \
      --password "$CH_PASSWORD" \
      --database "$CH_DB" \
      --query "TRUNCATE TABLE IF EXISTS ${CH_DB}.${table}"
  fi

  echo "==> importing ${CH_DB}.${table}"
  docker compose exec -T "$CLICKHOUSE_SERVICE" clickhouse-client \
    --user "$CH_USER" \
    --password "$CH_PASSWORD" \
    --database "$CH_DB" \
    --query "INSERT INTO ${CH_DB}.${table} FORMAT Native" < "$file"
done

echo
if [[ -f "$RAW_DIR/manifest.tsv" ]]; then
  echo "Imported manifest: $RAW_DIR/manifest.tsv"
fi

echo "Retained raw import complete."
