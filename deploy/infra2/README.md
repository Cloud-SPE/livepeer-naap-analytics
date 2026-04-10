# NAAP Analytics — infra2 Deployment Guide

Production deployment for `infra2.cloudspe.com`. Each service is a separate
Portainer stack for independent lifecycle management.

For the standard runtime and recovery model, see [`../../docs/operations/run-modes-and-recovery.md`](../../docs/operations/run-modes-and-recovery.md).

## Architecture

| Stack | Services | Domain |
|---|---|---|
| `naap-kafka` | Kafka (KRaft), Kafka Exporter | — (internal + port 9092) |
| `naap-clickhouse` | ClickHouse | — (internal, SSH tunnel for access) |
| `naap-app` | API | `naap-api.cloudspe.com` |
| `naap-prometheus` | Prometheus, Node Exporter, cAdvisor | — (SSH tunnel only) |
| `naap-grafana` | Grafana | `grafana.livepeer.cloud` |
| `naap-kafka-ui` | Kafka UI | `kafka-ui.cloudspe.com` |
| `naap-mirrormaker2` | MM2 (daydream→infra2) | — (internal) |

All stacks share the external `ingress` Docker network for inter-service
communication and Traefik routing.

## Security

**Kafka**: SASL/SCRAM-SHA-512. All clients authenticate with individual SCRAM users:

| SCRAM User | Used By | Permissions |
|---|---|---|
| `naap-consumer` | ClickHouse Kafka Engine | Consumer on network_events, streaming_events |
| `naap-monitor` | kafka-exporter | Describe topics, consumer groups |
| `kafka-ui` | Kafka UI | Read/describe all topics |
| `mm2-connector` | MirrorMaker2 | Producer on network_events |
| `cloudspe` | Cloud SPE gateways | Producer on streaming_events |

**Kafka UI**: Built-in login form auth (`AUTH_TYPE=LOGIN_FORM`) — separate from Kafka SCRAM auth.

**Grafana**: Built-in auth, anonymous access disabled.

**ClickHouse**: Not exposed externally. Access via `docker exec` or SSH tunnel.

**Prometheus**: Not exposed externally. Access via SSH tunnel.

## First-Time Setup

### Step 1 — Build and push images

On your local machine with Docker and `docker login tztcloud`:

```bash
make push IMAGE_TAG=latest
```

### Step 2 — Prepare the server

SSH into infra2 and run from the repo root:

```bash
bash deploy/infra2/server-prep.sh
```

This creates `/opt/naap/` config directories, copies Prometheus and Grafana
configs, and creates the `naap-analytics_clickhouse_data` Docker volume.

### Step 3 — Deploy naap-kafka

In Portainer: **Stacks → Add Stack → naap-kafka**
- Paste contents of `deploy/infra2/kafka/stack.yml`
- Set env vars (see below)
- Deploy and wait for all services to be healthy

### Step 4 — Create SCRAM users (one-time)

After Kafka is healthy, run on infra2:

```bash
# naap-consumer (ClickHouse Kafka Engine)
docker exec naap-kafka /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server naap-kafka:29092 --alter \
  --add-config 'SCRAM-SHA-512=[iterations=8192,password=NAAP_CONSUMER_PASSWORD_HERE]' \
  --entity-type users --entity-name naap-consumer

# naap-monitor (Kafka Exporter)
docker exec naap-kafka /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server naap-kafka:29092 --alter \
  --add-config 'SCRAM-SHA-512=[iterations=8192,password=KAFKA_MONITOR_PASSWORD_HERE]' \
  --entity-type users --entity-name naap-monitor

# kafka-ui (Kafka UI)
docker exec naap-kafka /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server naap-kafka:29092 --alter \
  --add-config 'SCRAM-SHA-512=[iterations=8192,password=KAFKA_UI_SASL_PASSWORD_HERE]' \
  --entity-type users --entity-name kafka-ui

# mm2-connector (MirrorMaker2 — Confluent Cloud → infra2)
docker exec naap-kafka /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server naap-kafka:29092 --alter \
  --add-config 'SCRAM-SHA-512=[iterations=8192,password=MM2_CONNECTOR_PASSWORD_HERE]' \
  --entity-type users --entity-name mm2-connector

# cloudspe (Cloud SPE gateways — direct streaming_events producers)
docker exec naap-kafka /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server naap-kafka:29092 --alter \
  --add-config 'SCRAM-SHA-512=[iterations=8192,password=CLOUDSPE_PRODUCER_PASSWORD_HERE]' \
  --entity-type users --entity-name cloudspe
```

Replace `*_HERE` with the actual passwords from your env vars.

### Step 5 — Deploy remaining stacks

Deploy in this order, waiting for each to become healthy before the next:

1. **naap-clickhouse** — `deploy/infra2/clickhouse/stack.yml`
2. **naap-app** — `deploy/infra2/app/stack.yml`
3. **naap-warehouse** — `deploy/infra2/warehouse/stack.yml` when you need scheduled or one-shot `dbt` publication
4. **naap-prometheus** — `deploy/infra2/prometheus/stack.yml`
5. **naap-grafana** — `deploy/infra2/grafana/stack.yml`
6. **naap-kafka-ui** — `deploy/infra2/kafka-ui/stack.yml`
7. **naap-mirrormaker2** — `deploy/infra2/mirrormaker2/stack.yml`

## Environment Variables

See `.env.example` for a complete reference. Passwords marked `change-me-*`
**must** be set to strong unique values before deploying.

### Important: password constraints

Passwords are substituted into ClickHouse SQL via `sed` using `|` as a
delimiter. Avoid `|` characters in ClickHouse user passwords
(`CLICKHOUSE_*_PASSWORD`, `NAAP_CONSUMER_PASSWORD`).

### First deployment: set KAFKA_AUTO_OFFSET_RESET=earliest

On first deploy of `naap-clickhouse`, set `KAFKA_AUTO_OFFSET_RESET=earliest`
to backfill the full topic history. Once the backfill is complete, update
to `latest` and redeploy the stack.

Monitor backfill progress:

```bash
docker exec naap-analytics-clickhouse clickhouse-client \
  --user naap_admin --password <pw> \
  --query "SELECT count(), min(event_ts), max(event_ts) FROM naap.accepted_raw_events"
```

## Updating

```bash
# 1. Build and push new images
make push IMAGE_TAG=latest

# 2. In Portainer: update naap-clickhouse and/or naap-app stacks (pull + redeploy)
```

ClickHouse data survives redeployment. Fresh volumes can use the extracted bootstrap baseline in [`../../infra/clickhouse/bootstrap/v1.sql`](../../infra/clickhouse/bootstrap/v1.sql); existing volumes still require deliberate schema management rather than assuming automatic replay on redeploy.

## Accessing Internal Services

**ClickHouse** (port 9000 / 8123):

```bash
docker exec -it naap-analytics-clickhouse clickhouse-client \
  --user naap_admin --password <pw>
```

Or SSH tunnel:

```bash
ssh -L 9000:localhost:9000 -L 8123:localhost:8123 user@infra2.cloudspe.com
```

**Prometheus** (port 9090):

```bash
ssh -L 9090:localhost:9090 user@infra2.cloudspe.com
# then open http://localhost:9090
```

## MirrorMaker2

Deploy as the `naap-mirrormaker2` stack after all other stacks are healthy.
The `mm2-connector` SCRAM user must be created first (Step 4 above).

One MM2 instance runs:

| Instance | Source | Topics Replicated |
|---|---|---|
| `naap-mm2-daydream` | Confluent Cloud / Daydream (SASL_SSL/PLAIN) | `network_events` |

`streaming_events` are **not** replicated via MM2. Cloud SPE Livepeer gateways
produce `streaming_events` directly into infra2's Kafka broker (SASL/SCRAM-SHA-512).

MM2 writes to infra2 using SASL/SCRAM-SHA-512 (`mm2-connector` user).

**Deploy**: In Portainer, create stack `naap-mirrormaker2` from
`deploy/infra2/mirrormaker2/stack.yml` with these env vars:

| Variable | Description |
|---|---|
| `MM2_CONNECTOR_PASSWORD` | SCRAM password for mm2-connector user |
| `DAYDREAM_SASL_USERNAME` | Confluent Cloud API key |
| `DAYDREAM_SASL_PASSWORD` | Confluent Cloud API secret |

**Restart** (safe — MM2 resumes from committed offsets):

```bash
# In Portainer: naap-mirrormaker2 → Restart
# Or on the host:
docker restart naap-mm2-daydream
```

## Dashboard Updates

If dashboard JSON files change, copy them to the server and Grafana will
pick them up within 30 seconds (no restart needed):

```bash
rsync -av infra/grafana/dashboards/ user@infra2.cloudspe.com:/opt/naap/grafana/dashboards/
```
