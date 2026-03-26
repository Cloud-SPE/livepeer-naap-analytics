#!/bin/bash
set -euo pipefail

# Pass-through mode: if args are given, run dbt directly.
# Used by: docker compose run --rm warehouse run|test|...
if [ $# -gt 0 ]; then
    exec dbt "$@"
fi

# Daemon mode: tiered dbt execution on a cron schedule.
#
# Static models (views) are built once at container startup — recreating them
# on every cron tick just issues DDL against an already-live definition.
#
# Incremental models (physical tables) are rebuilt on the cron schedule.
# A lockfile prevents overlapping runs if a build exceeds the schedule interval.

SCHEDULE="${DBT_CRON_SCHEDULE:-*/5 * * * *}"
LOCKFILE=/tmp/dbt-run.lock

echo "[dbt-daemon] Schedule: ${SCHEDULE}"

# Persist Docker env vars so cron jobs can see them (cron doesn't inherit the container env).
printenv | sed "s/'/'\\\\''/g; s/\(.*\)=\(.*\)/export \1='\2'/" > /tmp/dbt-env.sh

# Write the lock-protected incremental run script.
# This is sourced by cron and also called directly on startup.
cat > /tmp/dbt-run-incremental.sh <<'RUNSCRIPT'
#!/bin/bash
set -euo pipefail
LOCKFILE=/tmp/dbt-run.lock

if [ -f "$LOCKFILE" ]; then
    echo "[dbt-daemon] $(date -u +%FT%TZ) Skipping: previous incremental run still active"
    exit 0
fi

touch "$LOCKFILE"
trap 'rm -f "$LOCKFILE"' EXIT

cd /app/warehouse
echo "[dbt-daemon] $(date -u +%FT%TZ) Incremental run started"
dbt run --select tag:incremental
echo "[dbt-daemon] $(date -u +%FT%TZ) Incremental run complete"
RUNSCRIPT
chmod +x /tmp/dbt-run-incremental.sh

# Install crontab — only schedules the incremental (table-backed) models.
cat > /etc/cron.d/dbt-run <<EOF
SHELL=/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

${SCHEDULE} root . /tmp/dbt-env.sh && bash /tmp/dbt-run-incremental.sh >> /proc/1/fd/1 2>&1
EOF
chmod 0644 /etc/cron.d/dbt-run

# Build static views once on startup — fast DDL only, no data scanned.
echo "[dbt-daemon] Building static views..."
cd /app/warehouse && dbt run --select tag:static

# Build incremental tables on startup for initial state.
echo "[dbt-daemon] Building incremental tables (initial)..."
bash /tmp/dbt-run-incremental.sh

echo "[dbt-daemon] Starting cron..."
exec cron -f
