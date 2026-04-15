#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

CLICKHOUSE_SERVICE="${CLICKHOUSE_SERVICE:-clickhouse}"
CH_USER="${CH_USER:-naap_admin}"
CH_PASSWORD="${CH_PASSWORD:-changeme}"
CH_DB="${CH_DB:-naap}"
TABLES="${TABLES:-accepted_raw_events ignored_raw_events}"
OUT_DIR="${1:-${OUT_DIR:-.local/raw-export/$(date -u +%Y%m%dT%H%M%SZ)}}"

usage() {
  cat <<EOF2
Export retained raw ClickHouse tables in Native format for scratch rebuilds.

Usage:
  scripts/export_retained_raw.sh [output_dir]

Environment overrides:
  CLICKHOUSE_SERVICE=clickhouse
  CH_USER=naap_admin
  CH_PASSWORD=changeme
  CH_DB=naap
  TABLES="accepted_raw_events ignored_raw_events"
  OUT_DIR=.local/raw-export/<timestamp>

Output layout:
  <output_dir>/manifest.tsv
  <output_dir>/<table>.native

Notes:
  - This exports retained raw only. It does not export Kafka offsets.
  - Use this with bootstrap + replay when you want to rebuild a fresh database.
EOF2
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

mkdir -p "$OUT_DIR"
MANIFEST="$OUT_DIR/manifest.tsv"
printf 'table\trows\tmin_event_ts\tmax_event_ts\tmin_ingested_at\tmax_ingested_at\n' > "$MANIFEST"

for table in $TABLES; do
  echo "==> exporting ${CH_DB}.${table}"
  docker compose exec -T "$CLICKHOUSE_SERVICE" clickhouse-client \
    --user "$CH_USER" \
    --password "$CH_PASSWORD" \
    --database "$CH_DB" \
    --query "SELECT * FROM ${CH_DB}.${table} FORMAT Native" > "$OUT_DIR/${table}.native"

  stats=$(docker compose exec -T "$CLICKHOUSE_SERVICE" clickhouse-client \
    --user "$CH_USER" \
    --password "$CH_PASSWORD" \
    --database "$CH_DB" \
    --query "SELECT '$table', count(), toString(min(event_ts)), toString(max(event_ts)), toString(min(ingested_at)), toString(max(ingested_at)) FROM ${CH_DB}.${table} FORMAT TSVRaw")
  printf '%s\n' "$stats" >> "$MANIFEST"
done

echo
cat <<EOF2
Raw export complete.
  Output directory: $OUT_DIR
  Manifest:         $MANIFEST
EOF2
