# Operations Runbook

| Field | Value |
|---|---|
| **Status** | Active |
| **Effective date** | 2026-04-02 |
| **Ticket** | TASK 7.6 / [#273](https://github.com/livepeer/livepeer-naap-analytics-deployment/issues/273) |
| **Last reviewed** | 2026-04-14 |

---

## Quick Reference

| Topic | Document |
|---|---|
| Monitoring, local setup, offset reset, ClickHouse reload | [`devops-environment-guide.md`](devops-environment-guide.md) |
| Runtime validation and performance checklist | [`runtime-validation-and-performance.md`](runtime-validation-and-performance.md) |
| Resolver modes, failure recovery, full rebuild, retained-raw rebuild | [`run-modes-and-recovery.md`](run-modes-and-recovery.md) |
| Security posture, credential rotation, UFW rules | [`infra-hardening-runbook.md`](infra-hardening-runbook.md) |
| Kafka and ClickHouse retention windows | [`data-retention-policy.md`](data-retention-policy.md) |
| Per-service Compose responsibilities | [`compose-services.md`](compose-services.md) |
| Incident response, severity levels, escalation contacts | [`incident-response.md`](incident-response.md) |

### Stack topology

| Node | Services |
|---|---|
| **infra2** | Kafka (KRaft), Traefik, Kafka UI, kafka-exporter, MirrorMaker2 |
| **infra1** | ClickHouse, API, Resolver, Grafana, Prometheus, Traefik |

### Key URLs (production)

| Service | URL | Access |
|---|---|---|
| API | `https://naap-api.livepeer.cloud` | Public |
| Grafana | `https://grafana.livepeer.cloud` | Auth required |
| Kafka UI | `https://kafka-ui.livepeer.cloud` | Auth required |
| Portainer | `https://portainer.livepeer.cloud` | Ops only |
| Prometheus | SSH tunnel from infra1: `ssh -L 9090:localhost:9090 user@infra1` | Ops only |

---

## 1. Deployment from Scratch

For environment variable reference and local dev setup, see [`devops-environment-guide.md`](devops-environment-guide.md) §2–3.

### infra2 (Kafka node)

```bash
# Step 1 — Server preparation (run once as root/sudo)
bash deploy/ubuntu-setup.sh
# Edit <PORTAINER_SERVER_IP> and <OPS_CIDR> placeholders first:
bash deploy/set_ufw_rules.sh
bash deploy/server-prep.sh
```

```bash
# Step 2 — Deploy Kafka stack
# In Portainer: create stack naap-kafka from deploy/infra2/kafka/stack.yml
# Wait until naap-kafka container is healthy (~30s)
docker logs naap-kafka 2>&1 | grep -i "started\|ready\|bound"
```

```bash
# Step 3 — Create SCRAM users (one-time, run on infra2 after Kafka is healthy)
docker exec naap-kafka /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server naap-kafka:29092 --alter \
  --add-config 'SCRAM-SHA-512=[iterations=8192,password=NAAP_CONSUMER_PASSWORD_HERE]' \
  --entity-type users --entity-name naap-consumer

docker exec naap-kafka /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server naap-kafka:29092 --alter \
  --add-config 'SCRAM-SHA-512=[iterations=8192,password=KAFKA_MONITOR_PASSWORD_HERE]' \
  --entity-type users --entity-name naap-monitor

docker exec naap-kafka /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server naap-kafka:29092 --alter \
  --add-config 'SCRAM-SHA-512=[iterations=8192,password=KAFKA_UI_SASL_PASSWORD_HERE]' \
  --entity-type users --entity-name kafka-ui

docker exec naap-kafka /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server naap-kafka:29092 --alter \
  --add-config 'SCRAM-SHA-512=[iterations=8192,password=MM2_CONNECTOR_PASSWORD_HERE]' \
  --entity-type users --entity-name mm2-connector

docker exec naap-kafka /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server naap-kafka:29092 --alter \
  --add-config 'SCRAM-SHA-512=[iterations=8192,password=CLOUDSPE_PRODUCER_PASSWORD_HERE]' \
  --entity-type users --entity-name cloudspe
```

```bash
# Step 4 — Deploy remaining infra2 stacks (in order)
# naap-clickhouse → naap-app → naap-prometheus → naap-grafana → naap-kafka-ui → naap-mirrormaker2
# Each from deploy/infra2/<service>/stack.yml via Portainer
```

### infra1 (consumer node)

```bash
# Step 1 — Server preparation
bash deploy/ubuntu-setup.sh
bash deploy/set_ufw_rules.sh   # edit placeholders first
# (no server-prep.sh needed for infra1)
```

```bash
# Step 2 — Deploy stacks (in order)
# naap-clickhouse → naap-app → naap-prometheus → naap-grafana
# Each from deploy/infra1/<service>/stack.yml via Portainer
# Set KAFKA_BROKER_LIST=kafka.cloudspe.com:9092 and KAFKA_SECURITY_PROTOCOL=SASL_SSL in Portainer
```

```bash
# Step 3 — Publish dbt semantic views (run locally, targeting infra1 ClickHouse via SSH tunnel)
make warehouse-run
```

### First-run verification checklist

```
[ ] docker logs naap-kafka — no ERROR lines
[ ] docker logs naap-analytics-clickhouse — "bootstrap complete" or "migrations applied"
[ ] SELECT table, count() FROM system.kafka_consumers GROUP BY table — active consumers exist for both Kafka engine tables
[ ] SELECT count() FROM naap.accepted_raw_events — growing over time
[ ] curl https://naap-api.livepeer.cloud/healthz — 200 OK
[ ] Grafana dashboards loading — `naap-overview` (NaaP: Live AI Video) and `infra/naap-system-health` show data
[ ] Grafana alert rules present — `infra` folder loads without provisioning errors
[ ] Grafana contact points — enabled receivers resolve without missing env vars
[ ] Kafka UI — naap cluster shows connected, consumer groups visible
[ ] MM2 logs — "Successfully joined group", no SASL errors
```

### Retained-raw recovery

If Kafka is unavailable but ClickHouse still retains the raw event history, use
the retained-raw rebuild workflow in
[`run-modes-and-recovery.md`](run-modes-and-recovery.md#rebuild-from-retained-raw).
It is dry-run by default and becomes destructive only with `APPLY=1`.

---

## 2. Monitoring

For the full dashboard inventory and key metrics, see [`devops-environment-guide.md`](devops-environment-guide.md) §1.

### Incident triage decision tree

Start at the top and work down until you find the broken layer:

```
1. Is the API responding?
   NO  → Check naap-app / naap-resolver containers (logs, health)
   YES ↓

2. Is data fresh? (`api_current_active_stream_state.last_seen` within 2 minutes)
   NO  → Check resolver: make resolver-logs | grep ERROR
         Check ClickHouse ingest rate (clickhouse-overview dashboard)
   YES ↓

3. Is ClickHouse ingesting? (accepted_raw_events insert rate > 0)
   NO  → Check Kafka consumer lag (kafka-exporter-overview dashboard)
         Check system.kafka_consumers — are tables ATTACHED?
   YES ↓

4. Is Kafka receiving events?
   NO  → Check naap-kafka logs
         Check MirrorMaker2 (network_events) and gateway producers (streaming_events)
   YES → Data flow is healthy; check specific org/feature
```

### Log access

```bash
# Production — via SSH on each node
docker logs -f --tail 200 naap-analytics-clickhouse
docker logs -f --tail 200 naap-resolver
docker logs -f --tail 200 naap-app
docker logs -f --tail 200 naap-kafka
docker logs -f --tail 200 naap-mm2-daydream

# Or via Portainer UI: Stacks → <stack> → Services → <service> → Logs
```

---

## 3. Alerting

Grafana-managed alerting is provisioned from `infra/grafana/provisioning/alerting/`.
Prometheus remains a datasource only; the repository does not use a separate
Alertmanager path.

### Provisioned alert families

| Surface | Alert family | Condition | Severity | Action |
|---|---|---|---|---|
| System | `NaapScrapeTargetDown` | Core scrape target `< 1` for 2m | `critical` | Check service/container health and Prometheus targets |
| System | `NaapHostDiskUsageHigh` | Root disk usage `> 85%` for 15m | `critical` | Check host pressure and clear space before ClickHouse degrades |
| System | `NaapApi5xxRatioHigh` | API 5xx ratio `> 5%` for 10m | `degraded` | Check API logs and ClickHouse latency |
| System | `NaapApiLatencyHigh` | API p99 latency `> 500ms` for 10m | `degraded` | See §4.2 |
| System | `NaapKafkaConsumerLagHigh` | Kafka lag `> 50,000` for 15m | `critical` | See §4.1 |
| System | `NaapResolverClaimConflictsHigh` | Resolver claim conflicts `> 3` over 15m | `warning` | Inspect resolver ownership churn |
| System | `NaapAcceptedRawFreshnessStale` | accepted raw freshness `> 300s` for 5m | `critical` | Check ingest path end to end |
| System | `NaapDirtyScanLagHigh` | resolver watermark lag `> 300s` for 10m | `critical` | See `run-modes-and-recovery.md` |
| System | `NaapResolverTailStale` | no successful tail run for `> 900s` for 10m | `critical` | Check resolver runtime, `resolver_runs`, and claim churn immediately |
| System | `NaapResolverSameDayRepairStalled` | eligible same-day repairs, using the resolver's recorded lateness and quiet-period config, remain pending for 15m | `critical` | Inspect `resolver_dirty_windows`, `resolver_runs`, and tail freshness |
| System | `NaapDirtyHistoricalQueueBacklog` | pending dirty partitions `> 25` for 15m | `degraded` | See `run-modes-and-recovery.md` |
| System | `NaapResolverHistoricalRepairAgeHigh` | oldest pending historical dirty partition age `> 6h` for 30m | `degraded` | Inspect `resolver_dirty_partitions` and repair throughput |
| Streaming | `NaapStreamingAttributionRateLow` | attribution rate `< 75%` after 500-sample guard | `warning` | Check attribution inputs and resolver freshness |
| Request / response | `NaapRequestResponseDuplicateRowsPresent` | duplicate canonical job rows `> 0` | `critical` | Check canonical job uniqueness and resolver publication |
| Request / response | `NaapRequestResponseCanonicalCoverageLow` | canonical coverage `< 98%` after 20-source-job guard | `critical` | Check normalized vs canonical job publication |
| Request / response | `NaapRequestResponseDensityCollapse` | previous full hour `< 20%` of trailing 24h median hourly job count | `degraded` | Check ingest, resolver, and source traffic |
| Streaming | `NaapStreamingHealthSignalCoverageLow` | previous published hour `< 60%` coverage after 50-session guard | `degraded` | Check health sample publication |
| Streaming | `NaapStreamingUnservedDemandHigh` | previous published hour `> 40%` effective failure rate after 50-session guard | `critical` | Check gateway + pipeline capacity and failures |
| Streaming | `NaapStreamingDemandDensityCollapse` | previous full hour `< 20%` of trailing 24h median requested-session count | `degraded` | Check source traffic and ingest freshness |

### Notification routing

| Severity | Delivery |
|---|---|
| `warning`, `degraded` | Default warning receiver for the enabled channels |
| `critical`, `page-now` | Enabled critical channels; Discord + Telegram only when both are enabled |

Grafana startup validates the explicit alerting config before it loads
provisioning:

- `GRAFANA_ALERTING_ENABLED=false` disables alerting intentionally.
- If alerting is enabled, at least one delivery channel must be enabled.
- If Discord is enabled, `GRAFANA_ALERT_DISCORD_WEBHOOK_URL` must be set.
- If Telegram is enabled, both `GRAFANA_ALERT_TELEGRAM_BOT_TOKEN` and `GRAFANA_ALERT_TELEGRAM_CHAT_ID` must be set.
- Set `GRAFANA_PUBLIC_URL=https://grafana.livepeer.cloud/` in deployed Grafana so alert links resolve to the public dashboard host instead of an internal container address.

### Notification content

The shared alert template is intentionally short:

- The title carries severity plus the alert summary.
- The body leads with the summary, then promotes a single `Target:` line before any lower-signal metadata.
- `Observed:` should name the concrete instance, pipeline, or consumer group that needs investigation.
- Multi-dimensional alerts are grouped by concrete source labels such as `instance`, `job`, `topic`, `consumergroup`, `pipeline_id`, and `gateway` so different failing targets do not collapse into one vague notification.
- `Grafana:` links come from Grafana's configured root URL, so wrong `GRAFANA_PUBLIC_URL` values will produce wrong alert links.
- Enabled-but-missing secrets are treated as startup errors, not silent channel skips.

Alert messages must include the Grafana labels that identify the failing
surface, including `component`, `surface`, `pipeline_type`, and where relevant
`job_type`, `pipeline_id`, `gateway`, `topic`, or `consumergroup`.

---

## 4. Troubleshooting

### 4.1 Kafka consumer lag

**Symptoms:** Consumer group lag growing in `kafka-exporter-overview` dashboard; `accepted_raw_events` insert rate falling behind event produce rate.

**Diagnose:**
```sql
-- Check consumer status
SELECT table, status, num_messages_processed, last_exception
FROM system.kafka_consumers;

-- Check if any table is detached (returns 0 rows if detached)
SELECT name FROM system.tables WHERE database = 'naap' AND name LIKE 'kafka_%';
```

```bash
# Check Kafka-side lag
docker exec naap-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server naap-kafka:29092 \
  --group clickhouse-naap-network --describe
```

**Resolution by cause:**

| Cause | Resolution |
|---|---|
| Kafka engine table detached | `ATTACH TABLE naap.kafka_network_events;` |
| ClickHouse under memory/CPU pressure | Check `system.processes` and `system.merges`; wait for merges to complete or restart container |
| Sustained high event volume | Monitor for natural recovery; consider adding Kafka partitions (§6.6) |
| Consumer group offset needs reset | See [`devops-environment-guide.md`](devops-environment-guide.md) §4 |

---

### 4.2 ClickHouse query performance

**Symptoms:** API p99 latency spike visible in `infra/naap-system-health`; `clickhouse-overview` shows high query duration.

**Diagnose:**
```sql
-- Slowest recent queries
SELECT query_duration_ms, read_rows, memory_usage, query
FROM system.query_log
WHERE type = 'QueryFinish'
  AND event_time > now() - INTERVAL 10 MINUTE
ORDER BY query_duration_ms DESC LIMIT 10;

-- Active merges (can block reads)
SELECT table, elapsed, progress, rows_read, rows_written
FROM system.merges ORDER BY elapsed DESC;

-- Memory pressure
SELECT metric, value FROM system.asynchronous_metrics
WHERE metric LIKE '%Memory%' ORDER BY metric;
```

**Resolution by cause:**

| Cause | Resolution |
|---|---|
| Heavy merge activity | Wait; merges are temporary. If blocking production, `SYSTEM STOP MERGES` briefly then restart |
| Query missing partition pruning | Ensure queries filter on `org` and `toYYYYMM(event_ts)` — the partition key |
| Memory pressure causing query spills | Reduce concurrent queries; consider increasing container memory limit |
| Large TTL mutation running | Check `system.mutations WHERE is_done = 0`; wait or cancel with `KILL MUTATION` |

---

### 4.3 Gateway connectivity issues

**Symptoms:** `streaming_events` produce rate drops to 0 in `kafka-exporter-overview`; Cloud SPE gateways log SASL authentication errors.

Cloud SPE gateways connect to `kafka.cloudspe.com:9092` using SASL/SCRAM-SHA-512 (`cloudspe` user) over TLS terminated by Traefik. See [`infra-hardening-runbook.md`](infra-hardening-runbook.md) §Kafka Listener Architecture for the full TLS path.

**Diagnose in order:**

```bash
# 1. Is Traefik routing traffic? Check Traefik logs for TLS or routing errors
docker logs naap-traefik 2>&1 | grep -iE "kafka|9092|error|certificate" | tail -20

# 2. Is Kafka healthy?
docker logs naap-kafka 2>&1 | tail -20

# 3. Does the cloudspe SCRAM user exist?
docker exec naap-kafka /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server naap-kafka:29092 \
  --describe --entity-type users --entity-name cloudspe
# Expected output: SCRAM-SHA-512 iterations=8192

# 4. Is the Cloudflare TLS certificate valid?
# Check Traefik acme.json on infra2: the cert for kafka.cloudspe.com should not be expired
docker exec naap-traefik cat /ssl-certs/acme-cloudflare.json | \
  python3 -c "import json,sys,base64; d=json.load(sys.stdin); print([c['certificate'] for c in d.get('cloudflare',{}).get('Certificates',[]) if 'kafka' in str(c.get('domain',''))][:1])"
```

**Resolution by cause:**

| Cause | Resolution |
|---|---|
| `cloudspe` SCRAM user missing | Re-run Step 3 creation command for `cloudspe` from §1 |
| `cloudspe` password rotated but gateways not updated | Coordinate gateway config update before provisioning new hash — see [`infra-hardening-runbook.md`](infra-hardening-runbook.md) |
| TLS certificate expired | Traefik auto-renews via Cloudflare DNS challenge; if stuck, restart Traefik stack |
| Traefik TCP router misconfigured | Verify labels in `deploy/infra2/kafka/stack.yml` — HostSNI rule and port 9094 target |
| Kafka broker unhealthy | Restart `naap-kafka` container; verify volume `naap_v3_kafka_data` is intact |

---

### 4.4 DLQ spike (`ignored_raw_events`)

**Note on terminology:** This stack has no Kafka-level DLQ topics. `naap.ignored_raw_events` is the functional equivalent — all events that fail validation during ingest are written here. A "DLQ spike" means this table's insert rate has jumped. See [`data-retention-policy.md`](data-retention-policy.md) for full context.

**Symptoms:** `ignored_raw_events` insert rate visible in `clickhouse-overview`; ratio of ignored:accepted events is unusually high.

**Diagnose:**
```sql
-- Break down by rejection reason in the last hour
SELECT ignore_reason, count(), min(event_ts), max(event_ts)
FROM naap.ignored_raw_events
WHERE ingested_at > now() - INTERVAL 1 HOUR
GROUP BY ignore_reason ORDER BY count() DESC;

-- Inspect a sample of the rejected events for context
SELECT ignore_reason, event_type, data
FROM naap.ignored_raw_event_diagnostics
WHERE ingested_at > now() - INTERVAL 1 HOUR
LIMIT 20;
```

**Resolution by cause:**

| `ignore_reason` | Cause | Resolution |
|---|---|---|
| `unsupported_event_family` | New event type added upstream that ingest doesn't recognise | Update ingestion materialized view filter; coordinate with gateway team |
| `malformed_or_invalid_payload` | Upstream schema change breaking JSON parse | Check gateway team for recent changes; fix payload format or update parser |
| `unsupported_stream_trace_type` | New stream_trace subtype | Add to allowed subtypes in ingest MV |
| `ignored_stream_trace_*` | Noise filter working as intended | Verify the ratio is expected; no action if proportion is stable |

---

### 4.5 MirrorMaker2 replication lag (`network_events`)

**Symptoms:** `network_events` consumer group lag growing; `naap-mm2-daydream` container logging errors.

```bash
docker logs --tail 100 naap-mm2-daydream
```

**Resolution by cause:**

| Cause | Resolution |
|---|---|
| Confluent Cloud API key expired | Rotate key in Confluent console; update `DAYDREAM_SASL_USERNAME` / `DAYDREAM_SASL_PASSWORD` in Portainer and redeploy naap-mirrormaker2 |
| `mm2-connector` SCRAM password rotated | Update `MM2_CONNECTOR_PASSWORD` in Portainer and redeploy |
| MM2 container OOM | Increase memory limit in `deploy/infra2/mirrormaker2/stack.yml`; restart |
| Network connectivity loss | Check infra2 → Confluent Cloud connectivity; verify DNS resolves `pkc-921jm.us-east-2.aws.confluent.cloud` |

MM2 resumes from committed offsets on restart — no data is lost as long as Confluent Cloud retains the source events.

---

### 4.6 Resolver stuck / dirty partitions not draining

See [`run-modes-and-recovery.md`](run-modes-and-recovery.md) §Failure Recovery for full diagnostic and resolution procedures (stalled runs, orphaned window claims, dirty partitions, warehouse publication drift, full rebuild).

---

### 4.7 Note on Flink

The ticket that spawned this runbook referenced "Flink job failures" as a troubleshooting scenario. **Flink is not deployed in the current stack.** The pipeline is entirely ClickHouse-based (Kafka Engine → materialized views → resolver → dbt). Flink was used in an earlier architecture iteration and has been removed. No Flink troubleshooting is required.

---

## 5. Maintenance

### 5.1 Upgrading service images (API, resolver, ClickHouse init, dbt)

```bash
# Build and push with a version tag
make push IMAGE_TAG=<new-version>

# In Portainer: update IMAGE_TAG env var for the relevant stack, then "Update the stack"
# Do NOT use Grafana latest tag in production — pin to a specific version
```

### 5.2 Upgrading Kafka

Kafka SCRAM credentials are stored in the KRaft metadata log (volume `naap_v3_kafka_data`). Losing this volume requires recreating all SCRAM users.

```bash
# 1. Back up Kafka metadata volume
docker run --rm \
  -v naap_v3_kafka_data:/data:ro -v /backup/naap:/backup \
  alpine tar czf /backup/kafka-data-pre-upgrade-$(date +%Y%m%d).tar.gz /data

# 2. Stop consumer services (ClickHouse, Kafka UI, MM2)
# In Portainer: scale to 0 or stop stacks

# 3. Update kafka image version in deploy/infra2/kafka/stack.yml
# 4. Redeploy Kafka stack via Portainer

# 5. Verify SCRAM users still present
docker exec naap-kafka /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server naap-kafka:29092 \
  --describe --entity-type users --entity-name naap-consumer

# 6. Restart consumer services; verify consumer groups reconnect
SELECT status FROM system.kafka_consumers;  -- should return 'active'
```

### 5.3 Upgrading ClickHouse

```bash
# 1. Check for pending migrations
make migrate-status

# 2. Back up data volume (see §6.2)

# 3. Update clickhouse-server image version in deploy/infra1/clickhouse/stack.yml
# 4. Redeploy via Portainer (data volume persists)

# 5. Verify schema is intact
make migrate-status   # all migrations should still show applied
make test-validation-clean
```

### 5.4 Updating Grafana dashboards and alerting (hot-reload, no restart)

```bash
# Edit repo-managed dashboard JSON or alert provisioning in:
#   infra/grafana/dashboards/
#   infra/grafana/provisioning/alerting/
# Then copy to server — Grafana reloads provisioned files every 30 seconds
rsync -av infra/grafana/dashboards/ user@infra1:/opt/naap/grafana/dashboards/
rsync -av infra/grafana/provisioning/alerting/ user@infra1:/opt/naap/grafana/provisioning/alerting/
```

### 5.5 Adding or rotating SCRAM users

See [`infra-hardening-runbook.md`](infra-hardening-runbook.md) §Open Action Items for credential rotation procedures and the `cloudspe` external gateway coordination note.

### 5.6 Scaling Kafka partitions

```bash
# Increase partition count (safe — decreasing is not supported)
docker exec naap-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server naap-kafka:29092 \
  --alter --topic network_events --partitions <N>

# Update infra/kafka/topics.yaml to reflect the new count
```

---

## 6. Backups

### What to back up

| Volume | Contents | Priority | Recommended RPO |
|---|---|---|---|
| `naap_v3_clickhouse_data` | All analytics data (raw events, aggregates, serving tables) | **Critical** | Daily |
| `naap_v3_kafka_data` | KRaft metadata + SCRAM user credentials + buffered events | **Critical** | Before any Kafka upgrade or credential change |
| `naap_v3_zookeeper_data` | Legacy ZooKeeper volume (if still present from earlier deployment) | High | After any credential change |
| `naap_grafana_data` | Grafana annotations and custom settings | Low | Dashboard JSON is already in git |
| `naap_v3_kafka_data` (event data) | 7-day event buffer | Low | Ephemeral — events beyond Kafka retention are in ClickHouse |

ClickHouse table DDL (`infra/clickhouse/bootstrap/v1.sql`) and Grafana dashboards (`infra/grafana/dashboards/`) are version-controlled in git and do not need volume backups.
Grafana alert rules, contact points, templates, and notification policies are
also version-controlled in `infra/grafana/provisioning/alerting/`.

### ClickHouse data backup

```bash
# Pause merges for a consistent snapshot (optional but recommended)
docker exec naap-analytics-clickhouse clickhouse-client \
  --user naap_admin --password <CLICKHOUSE_ADMIN_PASSWORD> \
  --query "SYSTEM STOP MERGES"

# Back up the data volume
docker run --rm \
  -v naap_v3_clickhouse_data:/data:ro \
  -v /backup/naap:/backup \
  alpine tar czf /backup/clickhouse-$(date +%Y%m%d-%H%M).tar.gz /data

# Resume merges
docker exec naap-analytics-clickhouse clickhouse-client \
  --user naap_admin --password <CLICKHOUSE_ADMIN_PASSWORD> \
  --query "SYSTEM START MERGES"
```

Retain at least 7 daily backups locally. Copy offsite (S3, rsync to another host) for disaster recovery.

### Kafka metadata backup

```bash
docker run --rm \
  -v naap_v3_kafka_data:/data:ro \
  -v /backup/naap:/backup \
  alpine tar czf /backup/kafka-meta-$(date +%Y%m%d).tar.gz /data
```

Run this after every credential rotation and before any Kafka upgrade.

### Restore procedures

**ClickHouse restore from backup:**
```bash
# 1. Stop the ClickHouse stack in Portainer
# 2. Delete the existing data volume
docker volume rm naap_v3_clickhouse_data
# 3. Recreate the volume
docker volume create naap_v3_clickhouse_data
# 4. Restore from backup
docker run --rm \
  -v naap_v3_clickhouse_data:/data \
  -v /backup/naap:/backup \
  alpine tar xzf /backup/clickhouse-<YYYYMMDD-HHMM>.tar.gz -C /
# 5. Redeploy with CLICKHOUSE_SCHEMA_MODE=migrations (schema already present in restored data)
# 6. Verify: SELECT count() FROM naap.accepted_raw_events
```

**SCRAM credential restore (if Kafka volume is lost):**

Recreate all 5 SCRAM users using the commands in §1 (Step 3). Distribute the `cloudspe` credential to Cloud SPE gateway nodes out-of-band.

### Backup schedule

No automated backup tooling is currently configured. Recommended cron on each host:

```bash
# /etc/cron.d/naap-backup
# Daily ClickHouse backup at 02:00
0 2 * * * root docker run --rm -v naap_v3_clickhouse_data:/data:ro -v /backup/naap:/backup alpine tar czf /backup/clickhouse-$(date +\%Y\%m\%d).tar.gz /data && find /backup/naap -name 'clickhouse-*.tar.gz' -mtime +7 -delete
```
