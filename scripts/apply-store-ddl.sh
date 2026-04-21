#!/usr/bin/env bash
# Apply every store-table DDL under warehouse/ddl/stores/ against the
# target ClickHouse. Each file contains a single `CREATE TABLE IF NOT
# EXISTS` statement, so re-applying is safe — existing tables with
# identical schema are left untouched.
#
# Use case 1 — fresh bootstrap: apply after infra/clickhouse/bootstrap
# has created the naap database and system tables.
# Use case 2 — drift enforcement: run before the resolver, drift
# detector will surface any tables whose live schema has diverged
# from the checked-in declaration.
#
# `CREATE TABLE IF NOT EXISTS` deliberately skips the body on a
# conflict rather than altering the live table, because an automated
# rewrite of a live schema is a bigger change than this script is
# entitled to. When a declaration drifts, run
# `make lint-store-ddl`, read the mismatch, and apply an explicit
# `ALTER TABLE` migration that covers it.
set -euo pipefail

readonly REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly DDL_DIR="${REPO_ROOT}/warehouse/ddl/stores"

# Load .env so operators running locally don't need to export everything.
if [[ -f "${REPO_ROOT}/.env" ]]; then
  while IFS='=' read -r __key __value; do
    [[ "${__key}" =~ ^[A-Z_][A-Z0-9_]*$ ]] || continue
    __value="${__value%\"}"; __value="${__value#\"}"
    __value="${__value%\'}"; __value="${__value#\'}"
    export "${__key}=${__value}"
  done < <(grep -E '^[A-Z_][A-Z0-9_]*=' "${REPO_ROOT}/.env" || true)
fi

CH_URL="${CLICKHOUSE_HTTP_URL:-http://127.0.0.1:8123}"
CH_USER="${CLICKHOUSE_ADMIN_USER:-naap_admin}"
CH_PASSWORD="${CLICKHOUSE_ADMIN_PASSWORD:-changeme}"
CH_DATABASE="${CLICKHOUSE_DATABASE:-naap}"

applied=0
failed=0
for f in "${DDL_DIR}"/*.sql; do
  [[ -f "${f}" ]] || continue
  name=$(basename "${f}" .sql)
  body=$(grep -v '^--' "${f}" | tr -d '\n' | sed 's/;.*$/;/')
  if curl -fsS -u "${CH_USER}:${CH_PASSWORD}" \
    --data-binary "${body}" \
    "${CH_URL}/?database=${CH_DATABASE}" >/dev/null; then
    echo "  applied ${name}"
    applied=$((applied + 1))
  else
    echo "  FAILED  ${name}" >&2
    failed=$((failed + 1))
  fi
done

echo ""
echo "store-ddl: ${applied} applied, ${failed} failed"
if (( failed > 0 )); then
  echo ""
  echo "Any CREATE TABLE IF NOT EXISTS that fails usually means the file has"
  echo "a syntax error. Drift is NOT reported here — use"
  echo "\`make lint-store-ddl\` for that."
  exit 1
fi
