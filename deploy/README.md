# Deployment

Production stack files for Portainer. No local file mounts — everything is
baked into the Docker images.

For the supported runtime story, see [`../docs/operations/run-modes-and-recovery.md`](../docs/operations/run-modes-and-recovery.md).

## Images

| Image | Registry | What's in it |
|---|---|---|
| `tztcloud/naap-api` | Go API binary + embedded OpenAPI spec |
| `tztcloud/naap-clickhouse` | ClickHouse 24.3 + config + migrations + init script |

## Building and pushing images

```bash
# Requires: docker login tztcloud

make push                          # build + push both images (tag: latest)
make push IMAGE_TAG=v1.2.3         # build + push with a specific tag
make push-api                      # API image only
make push-clickhouse               # ClickHouse image only
```

The ClickHouse image bakes in:
- `infra/clickhouse/config/` — server config overrides (Kafka offset policy, listen addr)
- `infra/clickhouse/bootstrap/` — generated fresh-volume bootstrap baseline
- `infra/clickhouse/migrations/` — migration/reference path
- `infra/clickhouse/init/00_run_migrations.sh` — schema runner (bootstrap or migrations)

Any change to migrations or config requires a new image build and push.

## Deploying via Portainer

### Main stack (`stack.yml`)

1. Log in to Portainer
2. Go to **Stacks → Add Stack**
3. Name it `naap-analytics`
4. Paste the contents of `stack.yml` (or point to the git repo)
5. Set the required environment variables (see below)
6. Click **Deploy the stack**

### Required environment variables

| Variable | Description |
|---|---|
| `CLICKHOUSE_ADMIN_PASSWORD` | Admin password for the `naap_admin` ClickHouse user |
| `CLICKHOUSE_READER_PASSWORD` | Password for `naap_reader` (used by the API for read queries) |
| `CLICKHOUSE_WRITER_PASSWORD` | Password for `naap_writer` (used by the enrichment worker for inserts) |

### Optional environment variables

| Variable | Default | Description |
|---|---|---|
| `IMAGE_TAG` | `latest` | Image tag to deploy |
| `KAFKA_BROKER_LIST` | `infra2.cloudspe.com:9092` | Broker for ClickHouse Kafka Engine |
| `KAFKA_AUTO_OFFSET_RESET` | `latest` | `earliest` for full backfill, `latest` for new data only |
| `KAFKA_NETWORK_GROUP` | `clickhouse-naap-network` | Consumer group for `network_events` — **must be unique per node** |
| `KAFKA_STREAMING_GROUP` | `clickhouse-naap-streaming` | Consumer group for `streaming_events` — **must be unique per node** |
| `KAFKA_BROKERS` | `infra2.cloudspe.com:9092` | Broker(s) for the API service |
| `API_PORT` | `8000` | Host port (only used if Traefik labels are removed and `ports:` is uncommented) |
| `RATE_LIMIT_RPS` | `100` | Requests/sec per IP (0 = disabled) |
| `RATE_LIMIT_BURST` | `200` | Burst allowance |
| `OTLP_ENDPOINT` | *(empty)* | OTLP trace endpoint; empty disables telemetry |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |
| `ENRICHMENT_ENABLED` | `true` | Enable/disable the Livepeer API enrichment worker |
| `ENRICHMENT_INTERVAL` | `5m` | How often the enrichment worker polls the Livepeer API |
| `LIVEPEER_API_URL` | `https://livepeer-api.livepeer.cloud` | Livepeer public API base URL |

### Traefik routing

The API is routed via Traefik to `https://naap-api.cloudspe.com`. The stack attaches to the external `lpc-tester` network where Traefik listens. TLS is handled by the `cloudflare` cert resolver.

Direct port exposure (`ports:`) is disabled by default — uncomment it only for debugging.

### ClickHouse access

ClickHouse ports (8123, 9000) are **not exposed** in the production stack.
The container is named `naap-analytics-clickhouse` for stable DNS resolution.

To access the ClickHouse shell on the host:

```bash
docker exec -it naap-analytics-clickhouse clickhouse-client \
  --user naap_admin --password <password>
```

Or SSH tunnel from your local machine:

```bash
ssh -L 9000:localhost:9000 user@infra2.cloudspe.com
# then locally:
clickhouse-client --host localhost --user naap_admin --password <password>
```

### External volume

The stack uses an **external** named volume `naap-analytics_clickhouse_data`. Create it once before first deploy:

```bash
docker volume create naap-analytics_clickhouse_data
```

## Grafana dashboards

Ten dashboards are baked into the API image at `infra/grafana/dashboards/` and auto-provisioned
on startup.

### NAAP application dashboards (General folder)

| Dashboard | Description |
|-----------|-------------|
| `naap-overview` | High-level KPIs — active streams, GPUs, FPS, latency, demand |
| `naap-live-operations` | Real-time stream state, event throughput, gateway→orch paths |
| `naap-economics` | Payment volume (ETH), latest quoted prices by orchestrator |
| `naap-performance-drilldown` | E2E latency, FPS per orch, jitter, network FPS by pipeline |
| `naap-supply-inventory` | GPU count by model, full GPU inventory with VRAM |

### Infrastructure dashboards (infra folder)

Community dashboards from grafana.com, stored under `infra/grafana/dashboards/infra/`.
All query the **Prometheus** datasource (uid `prometheus`).

| Dashboard | grafana.com ID | Description |
|-----------|----------------|-------------|
| `node-exporter-full` | 1860 | Host CPU, memory, disk I/O, network — via `node-exporter` |
| `cadvisor-docker` | 893 | Per-container CPU, memory, network, disk I/O — via cAdvisor |
| `kafka-exporter-overview` | 7589 | Consumer group lag, topic throughput, broker health |
| `clickhouse-overview` | 14192 | ClickHouse queries/sec, merges, memory, insert rates |
| `prometheus-overview` | 3662 | Prometheus scrape health, TSDB size, target status |

### ClickHouse datasource

Dashboards query ClickHouse directly via the `grafana-clickhouse-datasource` plugin (uid
`clickhouse_prod`). This datasource is provisioned from
`infra/grafana/provisioning/datasources/clickhouse.yml` using:

| Setting | Value |
|---------|-------|
| Server | `naap-analytics-clickhouse:8123` |
| Protocol | HTTP |
| User | `naap_reader` |
| Default database | `naap` |

The datasource password is injected at runtime via the `CLICKHOUSE_READER_PASSWORD` environment
variable — the same variable required by the API service.

### Required environment variables (Grafana)

The Grafana service requires no additional variables beyond those already listed for the API.
`CLICKHOUSE_READER_PASSWORD` is shared.

### Grafana plugin install

The `grafana-clickhouse-datasource` plugin is installed at container startup via
`GF_INSTALL_PLUGINS=grafana-clickhouse-datasource`. First start may take 5–10 seconds for the
plugin download before dashboards become functional.

### Home dashboard

`GF_DASHBOARDS_DEFAULT_HOME_DASHBOARD_PATH` is set to `naap-overview.json` so Grafana opens the
overview dashboard on login.

## Kafka UI stack (`kafka-ui.stack.yml`)

Deploy as a separate Portainer stack for optional topic/consumer monitoring.

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BROKER` | `infra2.cloudspe.com:9092` | Broker to connect to |
| `KAFKA_UI_PORT` | `8080` | Host port to expose Kafka UI on |

Restrict access via firewall or reverse proxy — Kafka UI has no authentication by default.

## Multi-node deployments

To run independent stacks on multiple nodes against the same Kafka broker,
set unique consumer group names per node so ClickHouse instances don't
split partitions between them:

```
Node 1:  KAFKA_NETWORK_GROUP=clickhouse-naap-network-node1
         KAFKA_STREAMING_GROUP=clickhouse-naap-streaming-node1

Node 2:  KAFKA_NETWORK_GROUP=clickhouse-naap-network-node2
         KAFKA_STREAMING_GROUP=clickhouse-naap-streaming-node2
```

Each node will independently consume the full topic and maintain its own
complete copy of the data.

## Initial backfill

On first deployment, set `KAFKA_AUTO_OFFSET_RESET=earliest` to consume full
topic history. Monitor progress:

```bash
docker exec -it <stack>-clickhouse-1 clickhouse-client \
  --user naap_admin --password <password> \
  --query "SELECT count(), min(event_ts), max(event_ts) FROM naap.events"
```

Once backfill is complete:
1. Update `KAFKA_AUTO_OFFSET_RESET=latest` in Portainer
2. Redeploy the stack (ClickHouse will restart with the new setting for any future consumer group resets)

## Updating

```bash
# 1. Build and push new images
make push IMAGE_TAG=latest

# 2. In Portainer: Stacks → naap-analytics → Update the stack
#    (or pull and redeploy)
```

ClickHouse data is preserved in the `clickhouse_data` named volume across
redeployments. Fresh volumes can use the extracted bootstrap baseline; existing
volumes still need deliberate schema management rather than assuming automatic
replay on redeploy.
