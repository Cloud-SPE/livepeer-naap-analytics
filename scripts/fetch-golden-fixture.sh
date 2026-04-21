#!/usr/bin/env bash
# Fetch a pinned raw-event fixture for the replay harness.
#
# Pulls a fixed window from a ClickHouse `raw_events` table and writes a
# deterministic NDJSON archive plus a manifest (row count, checksum, window
# bounds). The archive is git-ignored; the manifest is committed so CI can
# verify that a locally-fetched fixture matches the pinned snapshot.
#
# Defaults target the 8-day "golden" fixture used by the determinism
# gate. Override with flags for shorter dev-iteration windows (see
# scripts/fetch-daily-fixture.sh for the 24h preset).
#
# Usage:
#   scripts/fetch-golden-fixture.sh [--source staging|local] [--force]
#                                   [--window-start "YYYY-MM-DD HH:MM:SS"]
#                                   [--window-end   "YYYY-MM-DD HH:MM:SS"]
#                                   [--fixture-name NAME]
#
# Defaults: source=staging. Reads ClickHouse connection from env/.env.
set -euo pipefail

# Default pinned window — changing these values changes the semantic
# identity of the fixture and requires the manifest to be regenerated
# in the same commit.
FIXTURE_WINDOW_START="2026-04-08 18:00:00"
FIXTURE_WINDOW_END="2026-04-16 18:00:00"
FIXTURE_NAME="raw_events_golden"

readonly REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly FIXTURE_DIR="${REPO_ROOT}/tests/fixtures"

SOURCE="staging"
FORCE="0"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --source)       SOURCE="$2"; shift 2 ;;
    --force)        FORCE="1"; shift ;;
    --window-start) FIXTURE_WINDOW_START="$2"; shift 2 ;;
    --window-end)   FIXTURE_WINDOW_END="$2"; shift 2 ;;
    --fixture-name) FIXTURE_NAME="$2"; shift 2 ;;
    -h|--help)
      sed -n '2,18p' "$0"; exit 0 ;;
    *) echo "unknown flag: $1" >&2; exit 2 ;;
  esac
done

ARCHIVE_PATH="${FIXTURE_DIR}/${FIXTURE_NAME}.ndjson.zst"
MANIFEST_PATH="${FIXTURE_DIR}/${FIXTURE_NAME}.manifest.json"

# Load .env so local runs pick up connection details without export.
# Using a line-by-line parser instead of `source` because .env contains
# values like cron expressions (`*/2 * * * *`) that break shell parsing.
if [[ -f "${REPO_ROOT}/.env" ]]; then
  while IFS='=' read -r __key __value; do
    [[ "${__key}" =~ ^[A-Z_][A-Z0-9_]*$ ]] || continue
    # Strip surrounding quotes if present.
    __value="${__value%\"}"; __value="${__value#\"}"
    __value="${__value%\'}"; __value="${__value#\'}"
    export "${__key}=${__value}"
  done < <(grep -E '^[A-Z_][A-Z0-9_]*=' "${REPO_ROOT}/.env" || true)
fi

case "${SOURCE}" in
  staging)
    : "${CLICKHOUSE_STAGING_URL:?set CLICKHOUSE_STAGING_URL in env (e.g. https://ch.staging.example.com)}"
    : "${CLICKHOUSE_STAGING_USER:?set CLICKHOUSE_STAGING_USER}"
    : "${CLICKHOUSE_STAGING_PASSWORD:?set CLICKHOUSE_STAGING_PASSWORD}"
    CH_URL="${CLICKHOUSE_STAGING_URL}"
    CH_USER="${CLICKHOUSE_STAGING_USER}"
    CH_PASSWORD="${CLICKHOUSE_STAGING_PASSWORD}"
    ;;
  local)
    CH_URL="${CLICKHOUSE_HTTP_URL:-http://127.0.0.1:8123}"
    CH_USER="${CLICKHOUSE_ADMIN_USER:-naap_admin}"
    CH_PASSWORD="${CLICKHOUSE_ADMIN_PASSWORD:-changeme}"
    ;;
  s3)
    # CI path: pull a pre-uploaded archive from S3 keyed by the
    # manifest's archive_sha256. Skips ClickHouse entirely; the manifest
    # must already exist in the repo and contain the expected sha.
    : "${REPLAY_FIXTURE_S3_BUCKET:?set REPLAY_FIXTURE_S3_BUCKET (bucket name, no s3:// prefix)}"
    if ! command -v aws >/dev/null 2>&1; then
      echo "required tool missing: aws (install AWS CLI)" >&2; exit 1
    fi
    if [[ ! -f "${MANIFEST_PATH}" ]]; then
      echo "manifest not found: ${MANIFEST_PATH}" >&2
      echo "an S3 fetch validates the archive against the committed manifest;" >&2
      echo "commit a manifest first or use --source staging|local to generate one." >&2
      exit 1
    fi
    ARCHIVE_SHA="$(jq -r .archive_sha256 "${MANIFEST_PATH}")"
    if [[ -z "${ARCHIVE_SHA}" || "${ARCHIVE_SHA}" == "null" ]]; then
      echo "manifest ${MANIFEST_PATH} is missing archive_sha256" >&2; exit 1
    fi
    S3_KEY="replay/fixtures/${ARCHIVE_SHA}.ndjson.zst"
    echo "fetching s3://${REPLAY_FIXTURE_S3_BUCKET}/${S3_KEY}"
    mkdir -p "${FIXTURE_DIR}"
    aws s3 cp "s3://${REPLAY_FIXTURE_S3_BUCKET}/${S3_KEY}" "${ARCHIVE_PATH}"
    echo "${ARCHIVE_SHA}  ${ARCHIVE_PATH}" | sha256sum -c
    echo "fetched ${ARCHIVE_PATH} ($(stat -c %s "${ARCHIVE_PATH}") bytes)"
    exit 0
    ;;
  *)
    echo "unknown --source: ${SOURCE} (expected staging|local|s3)" >&2; exit 2 ;;
esac

CH_DATABASE="${CLICKHOUSE_DATABASE:-naap}"

mkdir -p "${FIXTURE_DIR}"

if [[ -f "${ARCHIVE_PATH}" && "${FORCE}" != "1" ]]; then
  echo "archive already exists: ${ARCHIVE_PATH}"
  echo "re-run with --force to regenerate"
  exit 0
fi

for tool in curl zstd sha256sum jq; do
  if ! command -v "${tool}" >/dev/null 2>&1; then
    echo "required tool missing: ${tool}" >&2; exit 1
  fi
done

# Deterministic extraction, chunked by day to keep each HTTP response
# bounded (~500k rows/day is comfortable for ClickHouse HTTP).
# - explicit column list (never SELECT *)
# - ORDER BY (event_ts, event_id) — stable on every re-run against the same source
# - FORMAT JSONEachRow for newline-delimited output
# - windows are [start, end) so adjacent chunks do not overlap
chunk_query() {
  local chunk_start="$1" chunk_end="$2"
  cat <<SQL
SELECT
  event_id,
  event_type,
  event_subtype,
  event_ts,
  org,
  gateway,
  data,
  source_topic,
  source_partition,
  source_offset,
  payload_hash,
  schema_version,
  ingested_at
FROM ${CH_DATABASE}.raw_events
WHERE event_ts >= toDateTime64('${chunk_start}', 3, 'UTC')
  AND event_ts <  toDateTime64('${chunk_end}',   3, 'UTC')
ORDER BY event_ts, event_id
FORMAT JSONEachRow
SQL
}

# Enumerate day boundaries between window_start and window_end.
day_iter() {
  local start="$1" end="$2"
  local cur_epoch
  cur_epoch="$(date -u -d "${start}" +%s)"
  local end_epoch
  end_epoch="$(date -u -d "${end}" +%s)"
  while (( cur_epoch < end_epoch )); do
    local next_epoch=$(( cur_epoch + 86400 ))
    (( next_epoch > end_epoch )) && next_epoch=${end_epoch}
    date -u -d "@${cur_epoch}"   +"%Y-%m-%d %H:%M:%S"
    date -u -d "@${next_epoch}"  +"%Y-%m-%d %H:%M:%S"
    cur_epoch=${next_epoch}
  done
}

echo "fetching fixture window [${FIXTURE_WINDOW_START} .. ${FIXTURE_WINDOW_END}) from ${SOURCE}"
echo "  url=${CH_URL}"
echo "  db=${CH_DATABASE}"

TMP_NDJSON="$(mktemp "${FIXTURE_DIR}/.fetch.XXXXXX.ndjson")"
trap 'rm -f "${TMP_NDJSON}"' EXIT

: > "${TMP_NDJSON}"
mapfile -t BOUNDARIES < <(day_iter "${FIXTURE_WINDOW_START}" "${FIXTURE_WINDOW_END}")
for ((i=0; i < ${#BOUNDARIES[@]}; i+=2)); do
  CHUNK_START="${BOUNDARIES[i]}"
  CHUNK_END="${BOUNDARIES[i+1]}"
  echo "  chunk [${CHUNK_START} .. ${CHUNK_END})"
  curl -fsS --retry 3 --retry-delay 2 \
    -u "${CH_USER}:${CH_PASSWORD}" \
    --data-binary "$(chunk_query "${CHUNK_START}" "${CHUNK_END}")" \
    "${CH_URL}/?database=${CH_DATABASE}" \
    >> "${TMP_NDJSON}"
done

ROW_COUNT="$(wc -l < "${TMP_NDJSON}" | tr -d ' ')"
NDJSON_SHA256="$(sha256sum "${TMP_NDJSON}" | awk '{print $1}')"

if [[ "${ROW_COUNT}" == "0" ]]; then
  echo "fetch returned 0 rows — check window and source" >&2
  exit 1
fi

# Compress with zstd at level 10 — good ratio (~4-6%) on JSON while staying
# fast enough that a regeneration run takes minutes, not tens of minutes.
# Use level 19 only when producing the S3 archive for long-term storage.
# Override via ZSTD_LEVEL env var.
readonly ZSTD_LEVEL="${ZSTD_LEVEL:-10}"
readonly ZSTD_CODEC="zstd-${ZSTD_LEVEL}"
zstd -q -f -T0 -${ZSTD_LEVEL} "${TMP_NDJSON}" -o "${ARCHIVE_PATH}"
ARCHIVE_SHA256="$(sha256sum "${ARCHIVE_PATH}" | awk '{print $1}')"
ARCHIVE_BYTES="$(stat -c %s "${ARCHIVE_PATH}")"

# Manifest is committed; archive is not. A reviewer comparing manifests
# detects any semantic drift even though the archive itself stays local/S3.
jq -n \
  --arg name "${FIXTURE_NAME}" \
  --arg window_start "${FIXTURE_WINDOW_START}" \
  --arg window_end "${FIXTURE_WINDOW_END}" \
  --arg source "${SOURCE}" \
  --arg database "${CH_DATABASE}" \
  --argjson row_count "${ROW_COUNT}" \
  --arg ndjson_sha256 "${NDJSON_SHA256}" \
  --arg archive_sha256 "${ARCHIVE_SHA256}" \
  --argjson archive_bytes "${ARCHIVE_BYTES}" \
  --arg archive_codec "${ZSTD_CODEC}" \
  '{
    name: $name,
    window_start: $window_start,
    window_end: $window_end,
    source: $source,
    database: $database,
    row_count: $row_count,
    ndjson_sha256: $ndjson_sha256,
    archive_sha256: $archive_sha256,
    archive_bytes: $archive_bytes,
    archive_codec: $archive_codec,
    schema_columns: [
      "event_id", "event_type", "event_subtype", "event_ts", "org",
      "gateway", "data", "source_topic", "source_partition",
      "source_offset", "payload_hash", "schema_version", "ingested_at"
    ]
  }' > "${MANIFEST_PATH}"

echo "wrote ${ARCHIVE_PATH}"
echo "  rows=${ROW_COUNT}"
echo "  ndjson_sha256=${NDJSON_SHA256}"
echo "  archive_sha256=${ARCHIVE_SHA256}"
echo "  archive_bytes=${ARCHIVE_BYTES}"
echo "wrote ${MANIFEST_PATH}"
