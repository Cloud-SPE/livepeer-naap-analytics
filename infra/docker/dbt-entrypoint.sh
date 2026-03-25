#!/bin/bash
set -euo pipefail

# Pass-through mode: if args are given, run dbt directly.
# Used by: docker compose run --rm warehouse run|test|...
if [ $# -gt 0 ]; then
    exec dbt "$@"
fi

# Daemon mode: run dbt on a cron schedule.
SCHEDULE="${DBT_CRON_SCHEDULE:-*/5 * * * *}"

echo "[dbt-daemon] Schedule: ${SCHEDULE}"

# Persist Docker env vars so cron jobs can see them (cron doesn't inherit the container env).
printenv | sed "s/'/'\\\\''/g; s/\(.*\)=\(.*\)/export \1='\2'/" > /tmp/dbt-env.sh

# Install crontab entry.
cat > /etc/cron.d/dbt-run <<EOF
SHELL=/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

${SCHEDULE} root . /tmp/dbt-env.sh && cd /app/warehouse && dbt run >> /proc/1/fd/1 2>&1
EOF
chmod 0644 /etc/cron.d/dbt-run

# Run once immediately on startup.
echo "[dbt-daemon] Running initial dbt run..."
dbt run

echo "[dbt-daemon] Starting cron..."
exec cron -f
