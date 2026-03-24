# syntax=docker/dockerfile:1
#
# Custom ClickHouse image with config overrides, migrations, and init scripts
# baked in. No local file mounts are required at runtime.
#
# Build context must be the repo root:
#   docker build -f infra/docker/clickhouse.Dockerfile -t naap-clickhouse .

FROM clickhouse/clickhouse-server:24.3

# Server config overrides (kafka auto_offset_reset via from_env, listen addr, format settings)
COPY infra/clickhouse/config/ /etc/clickhouse-server/config.d/

# SQL migrations — applied in sort order on first container start by the init script
COPY infra/clickhouse/migrations/ /migrations/

# Init script — runs via docker-entrypoint-initdb.d on first start only
COPY infra/clickhouse/init/ /docker-entrypoint-initdb.d/
RUN chmod +x /docker-entrypoint-initdb.d/*.sh
