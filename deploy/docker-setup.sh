#!/bin/bash
set -euo pipefail

docker network create lpc-tester || true  # idempotent
docker network create ingress || true  # idempotent
docker volume create naap_clickhouse_data
docker volume create naap_clickhouse_db_config
docker volume create naap_clickhouse_config_d
docker volume create naap_clickhouse_user_config
docker volume create naap_gateway-lpData
docker volume create naap_grafana_config
docker volume create naap_grafana_data
docker volume create naap_kafka_data
docker volume create naap_mediamtx_config
docker volume create traefik_certs
docker volume create traefik_config
docker volume create traefik_logs
docker volume create leaderboard_postgres_data

# Grafana folders
mkdir -p /var/lib/docker/volumes/naap_grafana_config/_data/dashboards
mkdir -p /var/lib/docker/volumes/naap_grafana_config/_data/datasources

# Images for docker
docker pull livepeer/go-livepeer:latest
docker pull livepeerci/mediamtx
docker pull grafana/grafana:latest
docker pull postgres:16.4
docker pull traefik:v3.6.8
docker pull apache/kafka:3.9.1
docker pull ghcr.io/kafbat/kafka-ui:latest
docker pull clickhouse/clickhouse-server:24.11