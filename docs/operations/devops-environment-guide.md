# DevOps & Environment Management Guide

| Field | Value |
|---|---|
| **Status** | Active |
| **Effective date** | 2026-04-02 |
| **Ticket** | TASK-24 / [#303](https://github.com/livepeer/livepeer-naap-analytics-deployment/issues/303) |
| **Last reviewed** | 2026-04-09 |

Related docs:
- [`run-modes-and-recovery.md`](run-modes-and-recovery.md) — resolver modes, dbt publication, failure recovery
- [`compose-services.md`](compose-services.md) — per-service Compose responsibilities
- [`runtime-validation-and-performance.md`](runtime-validation-and-performance.md) — standard performance and data-quality measurement checklist
- [`data-retention-policy.md`](data-retention-policy.md) — Kafka and ClickHouse retention windows
- [`infra-hardening-runbook.md`](infra-hardening-runbook.md) — security posture and open action items

---

## 1. Monitoring the System

### Grafana dashboards

Grafana runs on port 3000 (local) or via Traefik HTTPS (production). Dashboards are provisioned from `infra/grafana/dashboards/`.

**Application dashboards** — check these first when investigating a business-level issue:

| Dashboard | Purpose | When to use |
|---|---|---|
| `naap-overview` | System health at a glance: event rates, resolver lag, API latency | Daily health check, first stop in any incident |
| `naap-live-operations` | Real-time stream activity: active sessions, encoder counts | Active stream investigations |
| `naap-economics` | Payments and revenue metrics by org | Billing queries, economics anomalies |
| `naap-performance-drilldown` | FPS, latency, WebRTC metrics per orch | Quality degradation investigations |
| `naap-supply-inventory` | GPU supply and capacity by org | Capacity planning, supply gaps |
| `naap-jobs` | AI-batch / BYOC job volume, SLA, attribution, and integrity | Request-response job investigations, duplicate or coverage checks |

**Infrastructure dashboards** — check these when investigating a system-level issue:

| Dashboard | Purpose | When to use |
|---|---|---|
| `infra/naap-system-health` | Service availability overview | Any service degradation |
| `infra/clickhouse-overview` | ClickHouse query load, merge activity, insert rates | Slow queries, ClickHouse memory/CPU pressure |
| `infra/kafka-exporter-overview` | Topic lag, consumer group status, broker metrics | Consumer falling behind, Kafka errors |
| `infra/cadvisor-docker` | Container CPU, memory, and network I/O | Container resource exhaustion |
| `infra/node-exporter-full` | Host CPU, memory, disk, network | Host-level resource issues |
| `infra/prometheus-overview` | Prometheus internal health and scrape status | Missing metrics, scrape failures |

### Key metrics to watch

| Metric | Source | Healthy state | Warning sign |
|---|---|---|---|
| ClickHouse insert rate | `clickhouse-overview` | Consistent, matches Kafka produce rate | Drops to 0 — Kafka engine stalled |
| Consumer group lag | `kafka-exporter-overview` | Near 0 for `clickhouse-naap-*` groups | Growing lag — ClickHouse falling behind |
| Resolver dirty partitions | `naap-overview` | Drains to 0 over time | Stuck at non-zero — resolver blocked |
| API p99 latency | `naap-overview` | < 100ms | Spikes > 500ms |
| `accepted_raw_events` insert rate | `clickhouse-overview` | Matches raw Kafka produce rate | Zero — ingest pipeline stalled |

### Accessing Prometheus

Production Prometheus is not exposed externally. Access via SSH tunnel:

```bash
# Tunnel to infra1 Prometheus
ssh -L 9090:localhost:9090 user@infra1.cloudspe.com
# Then open http://localhost:9090
```

### Log access

```bash
# All services (local)
make logs

# Single service
docker compose logs -f resolver
docker compose logs -f clickhouse
docker compose logs -f api

# Production (Portainer)
# Portainer UI → Stacks → <stack> → Services → <service> → Logs
# Or via SSH:
docker logs -f --tail 200 naap-resolver
docker logs -f --tail 200 naap-analytics-clickhouse
```

### Quick health check queries

Run these after any deployment or recovery to confirm the pipeline is live:

```sql
-- Events arriving in last 5 minutes
SELECT count() FROM naap.accepted_raw_events
WHERE ingested_at > now() - INTERVAL 5 MINUTE;

-- Active sessions visible to the API
SELECT count() FROM naap.api_active_stream_state
WHERE last_seen > now() - INTERVAL 120 SECOND;

-- Consumer group offsets (run in ClickHouse)
SELECT * FROM system.kafka_consumers;

-- ClickHouse system health
SELECT event_time, query_duration_ms, query
FROM system.query_log
WHERE type = 'QueryFinish' AND event_time > now() - INTERVAL 5 MINUTE
ORDER BY query_duration_ms DESC LIMIT 10;
```

---

## 2. Local Environment Setup

### Prerequisites

| Tool | Version | Install |
|---|---|---|
| Docker + Docker Compose | v24+ / v2+ | https://docs.docker.com/get-docker/ |
| Go | 1.22+ | https://go.dev/dl/ (for `make build`, `make test`) |
| Make | any | `brew install make` / `apt install make` |
| Python + uv | 3.11+ / latest | https://docs.astral.sh/uv/ (for `make inspect-setup`) |

### First-run steps

```bash
# 1. Clone
git clone <repo-url> livepeer-naap-analytics
cd livepeer-naap-analytics

# 2. Create .env from template
cp .env.example .env

# 3. The defaults in .env.example work for local development.
#    The only values you may want to change:
#      CLICKHOUSE_ADMIN_PASSWORD (default: changeme)
#      CLICKHOUSE_WRITER_PASSWORD (default: naap_writer_changeme)
#      CLICKHOUSE_READER_PASSWORD (default: naap_reader_changeme)
#    Leave all Kafka SASL vars empty — local Kafka runs without auth.

# 4. Start the stack
make up

# 5. Wait for ClickHouse to bootstrap (~30s on first run)
docker compose logs -f clickhouse | grep -E "bootstrap|migration|ready"

# 6. Verify everything is healthy
make ch-smoke          # confirms events flowing Kafka → ClickHouse
make test-integration  # integration tests against running ClickHouse
curl -s http://localhost:8000/v1/jobs/sla?limit=5
```

### Service endpoints (local)

| Service | URL | Notes |
|---|---|---|
| API | http://localhost:8000 | REST API |
| Grafana | http://localhost:3000 | Anonymous admin enabled in local dev |
| Kafka UI | http://localhost:8080 | No auth in local dev |
| Prometheus | http://localhost:9090 | |
| ClickHouse HTTP | http://localhost:8123 | |
| Resolver metrics | http://localhost:9102/metrics | |

### Key env vars for local development

| Variable | Default | Notes |
|---|---|---|
| `CLICKHOUSE_SCHEMA_MODE` | `bootstrap` | `bootstrap` = apply v1.sql + migrations on first run; `migrations` = apply forward migrations only |
| `KAFKA_AUTO_OFFSET_RESET` | `earliest` | `earliest` = re-consume full topic history on connect; `latest` = only new messages |
| `RESOLVER_MODE` | `auto` | See [run-modes-and-recovery.md](run-modes-and-recovery.md) |
| `ENV` | `development` | |
| `LOG_LEVEL` | `debug` | |
| `ENRICHMENT_ENABLED` | `true` | Periodically enriches orch/gateway metadata from Livepeer API |
| `DBT_CRON_SCHEDULE` | `*/5 * * * *` | How often warehouse-init republishes semantic views |

### Common local dev tasks

```bash
make up              # Start all services
make down            # Stop all services
make logs            # Stream all logs
make up-tooling      # Start optional warehouse tooling container
make ch-query        # Open interactive ClickHouse shell
make warehouse-run   # Manually trigger dbt publication
make test            # Go unit tests
make test-validation-clean  # Full regression test (isolated ClickHouse)
make migrate-status  # Show schema migration state
make migrate-up      # Apply pending migrations
make lint            # go vet + staticcheck
```

### Verify end-to-end

After `make up`, confirm the full pipeline is live:

```bash
# 1. API is healthy
curl -s http://localhost:8000/healthz

# 2. ClickHouse has events
make ch-smoke

# 3. Consumer groups are active
docker compose exec clickhouse clickhouse-client \
  --user default --password changeme \
  --query "SELECT status, count() FROM system.kafka_consumers GROUP BY status"
# Expected: active   2

# 4. Recent events visible
docker compose exec clickhouse clickhouse-client \
  --user default --password changeme \
  --query "SELECT count(), min(event_ts), max(event_ts) FROM naap.accepted_raw_events"

# 5. Resolver-owned job stores populated
docker compose exec clickhouse clickhouse-client \
  --user default --password changeme \
  --query "SELECT 'ai_batch', count() FROM naap.canonical_ai_batch_jobs UNION ALL SELECT 'byoc', count() FROM naap.canonical_byoc_jobs"
```

When request/response job schemas or attribution logic change, treat the
resolver bootstrap/backfill as part of deploy validation. `dbt` can publish the
views immediately, but those views are not meaningful until the resolver-owned
job stores contain rows.

---

## 3. Production Environment Setup

### Topology

| Node | Services | Kafka |
|---|---|---|
| **infra2** | Kafka (KRaft), Traefik, Kafka UI, MirrorMaker2, kafka-exporter | Hosts the broker |
| **infra1** | ClickHouse, API, Resolver, Grafana, Prometheus | Connects to infra2 via external TLS (`kafka.cloudspe.com:9092`) |

### One-time server setup

```bash
# Run on each host (requires root/sudo)
bash deploy/ubuntu-setup.sh
bash deploy/set_ufw_rules.sh    # edit <PORTAINER_SERVER_IP> and <OPS_CIDR> first
bash deploy/server-prep.sh      # infra2 only — creates /opt/naap/ and Docker volumes
```

### Stack deployment order (infra2)

Deploy stacks in Portainer in this order (each depends on the previous):

1. `deploy/infra2/kafka/stack.yml` — broker + CONTROLLER
2. Create SCRAM users (one-time, after Kafka is up) — see `deploy/infra2/README.md`
3. `deploy/infra2/clickhouse/stack.yml`
4. `deploy/infra2/app/stack.yml` — API + resolver
5. `deploy/infra2/prometheus/stack.yml`
6. `deploy/infra2/grafana/stack.yml`
7. `deploy/infra2/kafka-ui/stack.yml`
8. `deploy/infra2/mirrormaker2/stack.yml`

For infra1, deploy the same stacks from `deploy/infra1/` (no Kafka stack needed).

### Environment variable reference (production)

All variables are set in Portainer's "Environment variables" panel for each stack. Source of truth: `deploy/infra2/.env.example`. Key differences from local dev:

| Variable | Local default | Production override |
|---|---|---|
| `KAFKA_BROKER_LIST` | `naap-kafka:9092` | `kafka.cloudspe.com:9092` (infra1 only) |
| `KAFKA_SECURITY_PROTOCOL` | _(empty)_ | `SASL_SSL` (infra1 ClickHouse) |
| `KAFKA_SASL_MECHANISM` | _(empty)_ | `SCRAM-SHA-512` |
| `KAFKA_AUTO_OFFSET_RESET` | `earliest` | `latest` (after initial bootstrap) |
| `LOG_LEVEL` | `debug` | `info` |
| `RATE_LIMIT_RPS` | `30` | `100` |

---

## 4. Kafka Offset Management

### Consumer groups

| Consumer group | Topic | Stack |
|---|---|---|
| `clickhouse-naap-network` | `network_events` | naap-clickhouse (infra1/infra2) |
| `clickhouse-naap-streaming` | `streaming_events` | naap-clickhouse (infra1/infra2) |

### When to reset offsets vs use resolver backfill

| Situation | Action |
|---|---|
| ClickHouse volume wiped / fresh start | Set `KAFKA_AUTO_OFFSET_RESET=earliest`, redeploy. Kafka replays the last 7 days automatically on first connect. |
| Events missing for < 7 days (within Kafka retention) | Reset consumer group offset (procedure below) |
| Events missing for 7–90 days (beyond Kafka retention) | Use resolver `backfill` mode — reads from `accepted_raw_events` in ClickHouse |
| Resolver state incorrect but raw events are present | Use `make resolver-repair-window` or `make resolver-backfill` |
| Events missing > 90 days | Not recoverable (TTL expired) |

### Resetting consumer group offsets

This procedure pauses ClickHouse Kafka consumption, resets the broker-side offset, then resumes. Use it when you need to re-consume events that are still within the 7-day Kafka retention window.

**Step 1 — Pause consumption by detaching the Kafka engine tables:**

```sql
-- Run in ClickHouse (docker exec or make ch-query)
DETACH TABLE naap.kafka_network_events;
DETACH TABLE naap.kafka_streaming_events;
```

**Step 2 — Reset the offset on the Kafka broker:**

```bash
# On infra2 (local: replace container name as needed)
# Reset to earliest (re-consume full 7-day window)
docker exec naap-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server naap-kafka:29092 \
  --group clickhouse-naap-network \
  --reset-offsets --to-earliest \
  --topic network_events --execute

docker exec naap-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server naap-kafka:29092 \
  --group clickhouse-naap-streaming \
  --reset-offsets --to-earliest \
  --topic streaming_events --execute

# Reset to a specific timestamp (ISO 8601 → epoch ms)
docker exec naap-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server naap-kafka:29092 \
  --group clickhouse-naap-network \
  --reset-offsets --to-datetime 2026-03-01T00:00:00.000 \
  --topic network_events --execute
```

**Step 3 — Resume consumption by re-attaching the tables:**

```sql
ATTACH TABLE naap.kafka_network_events;
ATTACH TABLE naap.kafka_streaming_events;
```

**Step 4 — Verify consumers are active:**

```sql
SELECT database, table, status, num_messages_processed
FROM system.kafka_consumers;
-- Expected: status = 'active' for both tables
```

> Note: The unauthenticated `PLAINTEXT` listener on port 29092 is used here because it is Docker-internal only and the consumer group reset must happen while ClickHouse is paused. Do not expose port 29092 externally. See [`infra-hardening-runbook.md`](infra-hardening-runbook.md).

---

## 5. ClickHouse Reload and Replay

### Restarting the ClickHouse container

A container restart is safe — ClickHouse data is stored in the named Docker volume (`naap-analytics_clickhouse_data` / `naap_v3_clickhouse_data`) and persists across restarts. The Kafka engine tables reconnect and resume from the last committed consumer group offset.

```bash
# Local
docker compose restart clickhouse

# Production (Portainer)
# Portainer → naap-clickhouse stack → Services → naap-analytics-clickhouse → Restart
# Or via SSH:
docker restart naap-analytics-clickhouse
```

After restart, verify consumers reconnect within ~30 seconds:

```sql
SELECT status FROM system.kafka_consumers;
```

### Changing Kafka broker or security settings

The ClickHouse Kafka Engine does not support `ALTER TABLE MODIFY SETTING` — the broker list and security protocol are baked into the table DDL at creation time. To change them:

1. Update the relevant env vars in Portainer (`KAFKA_BROKER_LIST`, `KAFKA_SECURITY_PROTOCOL`, etc.)
2. Redeploy the ClickHouse stack. The container entrypoint recreates the Kafka engine tables with the new settings from the environment.
3. Set `KAFKA_AUTO_OFFSET_RESET=earliest` before redeploying if you need to re-consume from the beginning of the Kafka retention window.

### Pausing and resuming ingest (without restart)

Use `DETACH`/`ATTACH` to temporarily pause Kafka consumption without restarting the container. This is useful during maintenance or schema changes:

```sql
-- Pause
DETACH TABLE naap.kafka_network_events;
DETACH TABLE naap.kafka_streaming_events;

-- ... perform maintenance ...

-- Resume
ATTACH TABLE naap.kafka_network_events;
ATTACH TABLE naap.kafka_streaming_events;
```

Consumption resumes from where it left off. No events are lost as long as they remain within the Kafka retention window (7 days).

### Full rebuild from a clean volume

See `run-modes-and-recovery.md` → **Full Rebuild** for the complete procedure. Summary:

1. Stop the ClickHouse stack and remove the data volume.
2. Redeploy with `CLICKHOUSE_SCHEMA_MODE=bootstrap` — applies `infra/clickhouse/bootstrap/v1.sql` and all forward migrations.
3. Run `make warehouse-run` to publish semantic views.
4. Set `KAFKA_AUTO_OFFSET_RESET=earliest` and redeploy — ClickHouse re-consumes the last 7 days from Kafka automatically.
5. For events older than 7 days (up to 90 days): run `make resolver-backfill FROM=<start> TO=<end> ORG=<org>` — this reads from `naap.accepted_raw_events` on a neighbouring ClickHouse node if available.
6. Verify with `make test-validation-clean` and `make parity-verify`.

### Modifying ClickHouse TTLs on a live system

```sql
-- Example: change raw event retention from 90 to 60 days
ALTER TABLE naap.accepted_raw_events MODIFY TTL toDateTime(event_ts) + toIntervalDay(60);

-- Monitor the background mutation
SELECT * FROM system.mutations WHERE is_done = 0;
```

See `infra/clickhouse/retention.sql` for all table TTL statements, and [`data-retention-policy.md`](data-retention-policy.md) for the policy rationale.

---

## 6. Schema Migrations

```bash
make migrate-status    # show applied/pending migrations
make migrate-validate  # verify checksums of applied migrations
make migrate-up        # apply pending migrations
```

Migrations live in `infra/clickhouse/migrations/`. Each file is tracked by SHA-256 checksum in `naap.schema_migrations`. Do not modify already-applied migration files — add a new file instead.

To refresh the generated schema inventory after bootstrap changes:

```bash
make bootstrap-extract
```

`make bootstrap-extract` rewrites `docs/generated/schema.md` from the
checked-in `infra/clickhouse/bootstrap/v1.sql`. Refreshing `v1.sql` itself from
a clean migrated ClickHouse instance remains a separate operator task.

---

## 7. Image Build and Deployment

### Build and push

```bash
make push IMAGE_TAG=<tag>         # push all images (api, clickhouse, dbt, resolver)
make push-api IMAGE_TAG=<tag>     # push individual image
make push-resolver IMAGE_TAG=<tag>
make push-clickhouse IMAGE_TAG=<tag>
make push-dbt IMAGE_TAG=<tag>
```

### Deploy to production (Portainer)

1. Push the new image(s) with a version tag.
2. Update `IMAGE_TAG` in the relevant stack's Portainer environment variables.
3. Portainer → Stack → **Update the stack** (pulls new image and redeploys).

Do not use "Pull and redeploy" unless you also intend to pull the latest base image — it bypasses the `IMAGE_TAG` pin.
