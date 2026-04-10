#!/usr/bin/env bash
# server-prep.sh — one-time server preparation for infra2.cloudspe.com
#
# Run this script on infra2 from the repo root before deploying any stacks.
# It creates the directory structure for bind-mounted configs and the
# ClickHouse persistent volume.
#
# Usage:
#   git clone <repo> && cd livepeer-naap-analytics-simplified
#   bash deploy/infra2/server-prep.sh
#
# Safe to re-run: all operations are idempotent.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
NAAP_DIR="/opt/naap"

echo "[prep] Using repo root: ${REPO_ROOT}"
echo "[prep] Installing configs under: ${NAAP_DIR}"

# ── Directory structure ────────────────────────────────────────────────────────
echo "[prep] Creating directory tree..."
mkdir -p \
  "${NAAP_DIR}/kafka/config" \
  "${NAAP_DIR}/prometheus" \
  "${NAAP_DIR}/grafana/provisioning/alerting" \
  "${NAAP_DIR}/grafana/provisioning/datasources" \
  "${NAAP_DIR}/grafana/provisioning/dashboards" \
  "${NAAP_DIR}/grafana/scripts" \
  "${NAAP_DIR}/grafana/dashboards/infra"

# ── Kafka JAAS config ─────────────────────────────────────────────────────────
echo "[prep] Copying Kafka JAAS config..."
cp "${REPO_ROOT}/deploy/infra2/kafka/kafka_server_jaas.conf" \
   "${NAAP_DIR}/kafka/config/kafka_server_jaas.conf"

# ── Prometheus ─────────────────────────────────────────────────────────────────
echo "[prep] Copying Prometheus config..."
cp "${REPO_ROOT}/deploy/infra2/prometheus/prometheus.yml" \
   "${NAAP_DIR}/prometheus/prometheus.yml"

# ── Grafana provisioning ───────────────────────────────────────────────────────
echo "[prep] Copying Grafana datasource provisioning..."
cp "${REPO_ROOT}/infra/grafana/provisioning/datasources/prometheus.yml" \
   "${NAAP_DIR}/grafana/provisioning/datasources/prometheus.yml"
cp "${REPO_ROOT}/infra/grafana/provisioning/datasources/clickhouse.yml" \
   "${NAAP_DIR}/grafana/provisioning/datasources/clickhouse.yml"

echo "[prep] Copying Grafana dashboard provider config..."
cp "${REPO_ROOT}/infra/grafana/provisioning/dashboards/provider.yml" \
   "${NAAP_DIR}/grafana/provisioning/dashboards/provider.yml"

echo "[prep] Copying Grafana alerting provisioning..."
cp "${REPO_ROOT}/infra/grafana/provisioning/alerting/"*.yml \
   "${NAAP_DIR}/grafana/provisioning/alerting/"
cp "${REPO_ROOT}/infra/grafana/scripts/render-provisioning.sh" \
   "${NAAP_DIR}/grafana/scripts/render-provisioning.sh"

# ── Grafana dashboards ─────────────────────────────────────────────────────────
echo "[prep] Copying NAAP dashboards..."
cp "${REPO_ROOT}/infra/grafana/dashboards/"*.json \
   "${NAAP_DIR}/grafana/dashboards/"

echo "[prep] Copying infrastructure dashboards..."
cp "${REPO_ROOT}/infra/grafana/dashboards/infra/"*.json \
   "${NAAP_DIR}/grafana/dashboards/infra/"

# ── Docker volumes ─────────────────────────────────────────────────────────────
echo "[prep] Creating persistent volumes (skips each if already exists)..."

# ClickHouse — primary analytics data store
docker volume create naap_v3_clickhouse_data || true

# Kafka — topic data (losing this loses all buffered events)
docker volume create naap_v3_kafka_data || true

# ZooKeeper — cluster metadata, including all SCRAM user credentials
# Losing this requires recreating all Kafka SCRAM users before clients can reconnect
docker volume create naap_v3_zookeeper_data || true
docker volume create naap_v3_zookeeper_log || true

# ── Permissions ───────────────────────────────────────────────────────────────
# Grafana container runs as UID 472 by default
chown -R 472:472 "${NAAP_DIR}/grafana" || {
  echo "[prep] Warning: could not chown grafana dir to UID 472. If Grafana fails to start,"
  echo "[prep]          run: chown -R 472:472 ${NAAP_DIR}/grafana"
}

echo ""
echo "[prep] Done. Summary of what was created:"
echo ""
echo "  Directories:"
find "${NAAP_DIR}" -type d | sort | sed 's/^/    /'
echo ""
echo "  Docker volumes:"
echo "    naap_v3_clickhouse_data"
echo "    naap_v3_kafka_data"
echo "    naap_v3_zookeeper_data"
echo "    naap_v3_zookeeper_log"
echo ""
echo "[prep] Next steps:"
echo "  1. Deploy naap-kafka stack in Portainer"
echo "  2. Create SCRAM users (see deploy/infra2/kafka/stack.yml)"
echo "  3. Deploy remaining stacks in order:"
echo "       naap-clickhouse → naap-app → naap-prometheus → naap-grafana → naap-kafka-ui"
echo ""
echo "  See deploy/infra2/README.md for full deployment guide and env var reference."
