#!/bin/bash
set -euo pipefail

echo "[migrations] Starting ClickHouse schema migrations via ch-migrate"
bash "$(dirname "$0")/ch-migrate.sh" up
echo "[migrations] All migrations applied successfully"
