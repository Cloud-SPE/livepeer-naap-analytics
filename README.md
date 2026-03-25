# NAAP Analytics

Analytics API for the Livepeer AI Network (NAAP). Events flow from Kafka into
ClickHouse via the Kafka Engine, and are surfaced through a Go REST API covering
network state, stream activity, performance, payments, reliability, and an
orchestrator leaderboard.

## How it works

```
Kafka topics                 ClickHouse                          Go API / Grafana
──────────────────────────────────────────────────────────────────────────────────
network_events   ──Kafka──►  Kafka Engine ──MV──►  naap.events
streaming_events ──Engine──►  tables          │      │
                                              │      └──► Aggregate MVs (MV-populated)
                                              │             agg_orch_state
                                              │             agg_stream_*
                                              │             agg_stream_status_samples  ← migration 013
                                              │             agg_payment_*
                                              │             agg_*_hourly
                                              └──────────────────────────► HTTP :8000
                                                                           Grafana :3000

Livepeer API (poll every 5m)   enrichment worker        ClickHouse
──────────────────────────────────────────────────────────────────────────────────
/api/orchestrator  ──HTTP──►  Go goroutine  ──INSERT──►  naap.orch_metadata
/api/gateways      ──poll──►  (in API proc) ──INSERT──►  naap.gateway_metadata
agg_orch_state     ──read──►  (same worker) ──INSERT──►  naap.agg_gpu_inventory    ← migration 014
```

Two Kafka topics are consumed directly by ClickHouse:

| Topic | Org | Volume | Content |
|---|---|---|---|
| `network_events` | `daydream` | ~4.6M events/day | Orch capabilities, discovery, network caps |
| `streaming_events` | `cloudspe` | ~35K events/day | Stream lifecycle, payments, AI status |

Events land in `naap.events` (raw, 365-day TTL) and fan out to pre-aggregated
tables via Materialized Views. The Go API queries only the aggregate tables — no
raw-event queries at request time.

A background enrichment worker polls the [Livepeer public API](https://livepeer-api.livepeer.cloud)
every 5 minutes and upserts orchestrator and gateway metadata (ENS names, stake, service URIs,
deposits) into `naap.orch_metadata` and `naap.gateway_metadata`. It also reads the current
`agg_orch_state` snapshot, parses the `raw_capabilities` JSON blob to extract per-slot GPU info,
and writes structured rows to `naap.agg_gpu_inventory` (used by the Supply & Inventory dashboard).

## Project structure

```
api/                    Go REST API (port 8000)
  cmd/server/           Entrypoint
  internal/
    config/             Environment config (envconfig)
    enrichment/         Livepeer API polling worker (ENS names, stake, gateway data, GPU inventory)
    providers/          Cross-cutting: logger, telemetry, Kafka client
    repo/clickhouse/    ClickHouse query layer (R1–R6)
    service/            Business logic (leaderboard scoring, etc.)
    types/              Domain types shared across layers
    runtime/            HTTP handlers, middleware, Prometheus metrics, server wiring
      static/           Embedded assets (openapi.yaml)
    validation/         Scenario-based data-quality test harness (31 tests)

infra/
  clickhouse/
    config/             ClickHouse server config overrides (including Prometheus endpoint)
    init/               Docker entrypoint migration runner
    migrations/         SQL migrations (applied in sort order on first start)
  docker/               Dockerfiles for api
  grafana/
    dashboards/         Grafana dashboard JSON files (auto-loaded on startup)
                          naap-overview.json, naap-live-operations.json,
                          naap-economics.json, naap-performance-drilldown.json,
                          naap-supply-inventory.json
    provisioning/       Grafana datasource and dashboard provider config
                          datasources/clickhouse.yml  — grafana-clickhouse-datasource
                          datasources/prometheus.yml  — Prometheus datasource
  kafka/                Kafka topic definitions
  prometheus/           Prometheus scrape configuration

docs/
  DESIGN.md             Architecture overview and layer rules
  PLANS.md              Phase planning and status
  PRODUCT_SENSE.md      Product goals and success criteria
  design-docs/          Architecture decision records (ADRs) and behavioral contracts
  exec-plans/           Per-phase execution plans (active + completed)
  product-specs/        Feature requirements (R1–R6)

tests/load/             k6 load test script
tools/inspector/        Event inspection utility
```

## Prerequisites

| Tool | Version | Purpose |
|---|---|---|
| Go | 1.24+ | API service |
| Docker + Docker Compose | v2 | Full local stack |
| k6 | any | Load testing (optional) |

## Running locally

```bash
# 1. Copy and configure environment
cp .env.example .env
# Edit .env — at minimum set passwords; see Configuration below

# 2. Start the full stack
make up

# 3. Verify everything is healthy (~30s for migrations to apply)
curl http://localhost:8000/healthz
# → {"status":"ok"}

# 4. Browse the API and observability stack
open http://localhost:8000/docs   # Swagger UI
open http://localhost:9090        # Prometheus
open http://localhost:3000        # Grafana (anonymous admin, no login required)

# 5. Stop
make down
```

**Useful make targets:**

```bash
make logs             # Follow all service logs
make ch-query         # Interactive ClickHouse shell
make ch-smoke         # Quick smoke test (event counts, agg table rows)
make inspect          # Run event inspector against production broker
```

**Service ports (local):**

| Port | Service |
|---|---|
| 8000 | Go API (REST + `/metrics`) |
| 8080 | Kafka UI |
| 9090 | Prometheus |
| 3000 | Grafana |
| 9308 | Kafka exporter metrics |
| 8123 | ClickHouse HTTP |
| 9000 | ClickHouse native |
| 9100 | node-exporter host metrics (internal) |

## Building

```bash
make build            # Build all Docker images
make build-api        # Build API image only
make build-pipeline   # Build pipeline image only
```

To build the API binary directly (without Docker):

```bash
cd api && go build -o bin/server ./cmd/server
```

## Testing

```bash
make test             # Go unit tests with race detector (includes enrichment client + GPU parsing tests)

# Integration tests — requires a running ClickHouse (make up first)
CLICKHOUSE_ADDR=localhost:9000 make test-integration

# Data-quality validation tests — requires a running ClickHouse (make up first)
make test-validation

# Benchmarks
make bench

# Load test — requires the API running on localhost:8000
make load-test
```

## API endpoints

All endpoints are documented at **`GET /docs`** (Swagger UI) once the API is running.
The raw spec is at **`GET /docs/openapi.yaml`**.

| Group | Prefix | Endpoints |
|---|---|---|
| Health | `/healthz` | `GET /healthz` |
| Metrics | `/metrics` | `GET /metrics` (Prometheus exposition format) |
| Network (R1) | `/v1/net/` | summary, orchestrators, `/{address}`, gpu, models |
| Streams (R2) | `/v1/streams/` | active, summary, history |
| Performance (R3) | `/v1/perf/` | fps, fps/history, latency, webrtc |
| Payments (R4) | `/v1/payments/` | summary, history, by-pipeline, by-orch |
| Reliability (R5) | `/v1/reliability/` | summary, history, orchs, `/v1/failures` |
| Leaderboard (R6) | `/v1/leaderboard` | list, `/{address}` |
| Gateways (R7) | `/v1/net/gateways` | list, `/{address}`, `/{address}/orchs` |
| Stream Extensions (R8) | `/v1/streams/` | samples, `/{stream_id}`, attribution |
| E2E Latency (R9) | `/v1/perf/` | e2e-latency, e2e-latency/history |
| Pipelines (R10) | `/v1/pipelines` | list, `/{pipeline}` |
| Pricing (R11) | `/v1/net/pricing` | list, `/{address}` |
| Model Performance (R12) | `/v1/perf/by-model`, `/v1/net/model` | by-model list, model detail |
| Extended Payments (R13) | `/v1/payments/` | by-gateway, by-stream |
| Failure Analysis (R14) | `/v1/failures/` | by-pipeline, by-orch |
| Capacity (R15) | `/v1/net/capacity` | capacity summary |

Common query parameters: `org`, `start`, `end`, `limit`, `offset`, `active_only`.

### Gateways (R7)
| Method | Path | Description |
|--------|------|-------------|
| GET | `/v1/net/gateways` | List gateways with active stream counts |
| GET | `/v1/net/gateways/{address}` | Gateway profile: streams routed, orchs used, payments |
| GET | `/v1/net/gateways/{address}/orchs` | Orchestrators used by a specific gateway |

### Stream Extensions (R8)
| Method | Path | Description |
|--------|------|-------------|
| GET | `/v1/streams/samples` | Per-stream telemetry samples (fps, latency, state) |
| GET | `/v1/streams/{stream_id}` | Full timeline and state for a specific stream |
| GET | `/v1/streams/attribution` | Attribution rate: fraction of samples with resolved orch |

### E2E Latency (R9)
| Method | Path | Description |
|--------|------|-------------|
| GET | `/v1/perf/e2e-latency` | E2E latency p50/p95/p99 by pipeline and orchestrator |
| GET | `/v1/perf/e2e-latency/history` | Hourly E2E latency trend |

### Pipelines (R10)
| Method | Path | Description |
|--------|------|-------------|
| GET | `/v1/pipelines` | Cross-cutting summary per pipeline (streams, FPS, payments) |
| GET | `/v1/pipelines/{pipeline}` | Detailed pipeline stats with model breakdown |

### Pricing (R11)
| Method | Path | Description |
|--------|------|-------------|
| GET | `/v1/net/pricing` | Flat pricing rows for all active orchestrators |
| GET | `/v1/net/pricing/{address}` | Structured pricing profile for one orchestrator |

### Model Performance (R12)
| Method | Path | Description |
|--------|------|-------------|
| GET | `/v1/perf/by-model` | FPS performance broken down by AI model |
| GET | `/v1/net/model?model_id=` | Detail for one model (orchs, FPS, warm count) |

### Extended Payments (R13)
| Method | Path | Description |
|--------|------|-------------|
| GET | `/v1/payments/by-gateway` | Payment totals aggregated by gateway address |
| GET | `/v1/payments/by-stream` | Payment totals per stream session |

### Failure Analysis (R14)
| Method | Path | Description |
|--------|------|-------------|
| GET | `/v1/failures/by-pipeline` | Failure counts aggregated per pipeline |
| GET | `/v1/failures/by-orch` | Inference errors and restarts per orchestrator |

### Capacity (R15)
| Method | Path | Description |
|--------|------|-------------|
| GET | `/v1/net/capacity` | GPU supply (warm orchs, VRAM) vs. active stream demand |

Prometheus metrics exposed on `/metrics`:

| Metric | Type | Labels |
|---|---|---|
| `http_requests_total` | Counter | `method`, `route`, `status` |
| `http_request_duration_seconds` | Histogram | `method`, `route` |
| Standard Go runtime metrics | — | — |

## Configuration

All configuration is via environment variables. Copy `.env.example` to `.env`.

### API service

| Variable | Default | Description |
|---|---|---|
| `PORT` | `8000` | HTTP listen port |
| `ENV` | `development` | `development` or `production` |
| `LOG_LEVEL` | `debug` | `debug`, `info`, `warn`, `error` |
| `CLICKHOUSE_ADDR` | `localhost:9000` | ClickHouse native address |
| `CLICKHOUSE_DB` | `naap` | Database name |
| `CLICKHOUSE_USER` | `naap_reader` | Read-only user for API queries |
| `CLICKHOUSE_PASSWORD` | *(required)* | Reader password |
| `CLICKHOUSE_WRITER_USER` | `naap_writer` | Write user for the enrichment worker |
| `CLICKHOUSE_WRITER_PASSWORD` | *(required)* | Writer password |
| `KAFKA_BROKERS` | `localhost:9092` | Kafka broker(s) for the API service |
| `RATE_LIMIT_RPS` | `30` | Requests/sec per IP (0 = disabled) |
| `RATE_LIMIT_BURST` | `60` | Burst allowance |
| `OTLP_ENDPOINT` | *(empty)* | OTLP trace endpoint; empty disables telemetry |

### Enrichment worker

| Variable | Default | Description |
|---|---|---|
| `ENRICHMENT_ENABLED` | `true` | Enable/disable the enrichment worker |
| `ENRICHMENT_INTERVAL` | `5m` | How often to poll the Livepeer API |
| `LIVEPEER_API_URL` | `https://livepeer-api.livepeer.cloud` | Base URL for the Livepeer public API |

### ClickHouse Kafka Engine

These control how ClickHouse consumes from Kafka. Set in `.env` (docker-compose
passes them to the ClickHouse container at startup).

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BROKER_LIST` | `infra2.cloudspe.com:9092` | Broker for ClickHouse Kafka Engine tables |
| `KAFKA_AUTO_OFFSET_RESET` | `earliest` | `earliest` = full history; `latest` = new data only |
| `CLICKHOUSE_ADMIN_PASSWORD` | `changeme` | Admin user password |
| `CLICKHOUSE_WRITER_PASSWORD` | `naap_writer_changeme` | Writer user password |
| `CLICKHOUSE_READER_PASSWORD` | `naap_reader_changeme` | Reader user password |

> **Important:** `KAFKA_BROKER_LIST` is baked into the Kafka Engine table DDL at
> migration time. To change the broker on an already-running instance, set the new
> value in `.env` then run `make down && make up` (migrations re-apply on a fresh
> container). See `infra/clickhouse/README.md` for details.

## Deploying to production

The stack is fully Dockerised. For a single-host deployment:

```bash
# 1. Clone and configure
git clone <repo-url>
cd livepeer-naap-analytics-simplified
cp .env.example .env
```

Edit `.env` for production:

```bash
ENV=production
LOG_LEVEL=info

# Strong passwords
CLICKHOUSE_ADMIN_PASSWORD=<strong-password>
CLICKHOUSE_WRITER_PASSWORD=<strong-password>
CLICKHOUSE_READER_PASSWORD=<strong-password>

# Point at the production Kafka broker
KAFKA_BROKER_LIST=infra2.cloudspe.com:9092
KAFKA_AUTO_OFFSET_RESET=latest   # new data only; use 'earliest' for initial backfill

# Rate limiting (tune to traffic expectations)
RATE_LIMIT_RPS=100
RATE_LIMIT_BURST=200

# Optional: send traces to Grafana / Honeycomb / Jaeger
OTLP_ENDPOINT=https://your-otlp-endpoint
```

```bash
# 2. Start in detached mode
docker compose up -d

# 3. Verify
curl https://your-domain/healthz

# 4. Expose via a reverse proxy (nginx / Caddy) for TLS
# The API listens on port 8000; proxy HTTPS → http://localhost:8000
```

### Production checklist

- [ ] All default passwords changed in `.env` (admin, reader, writer)
- [ ] `ENV=production` and `LOG_LEVEL=info`
- [ ] TLS terminated at reverse proxy (Caddy or nginx)
- [ ] `KAFKA_AUTO_OFFSET_RESET=latest` (after initial backfill is complete)
- [ ] Rate limits tuned (`RATE_LIMIT_RPS`, `RATE_LIMIT_BURST`)
- [ ] ClickHouse data volume backed up or on persistent block storage
- [ ] `OTLP_ENDPOINT` set for OTLP tracing (optional; Prometheus metrics are always on)
- [ ] Firewall: only ports 80/443 exposed externally; 8123/9000/9363 internal only
- [ ] Enrichment worker: `ENRICHMENT_ENABLED=true`, `LIVEPEER_API_URL` reachable from host

### Initial backfill

On first deployment against a live broker, set `KAFKA_AUTO_OFFSET_RESET=earliest`
to consume full topic history. Monitor progress:

```bash
docker compose exec clickhouse clickhouse-client \
  --user naap_admin --password <password> \
  --query "SELECT count(), min(event_ts), max(event_ts) FROM naap.events"
```

Once the backfill is complete, change to `KAFKA_AUTO_OFFSET_RESET=latest` and
run `make down && make up` so new consumer sessions start from the current end.

After backfill, re-run the orch address backfill (migration 011 backfills
reliability and stream state from `naap.events` using the then-complete
`agg_orch_state` lookup):

```bash
docker compose exec clickhouse clickhouse-client \
  --user naap_admin --password <password> \
  --queries-file /migrations/011_fix_orch_address.sql
```

## ClickHouse management

```bash
# Interactive shell
make ch-query

# Check event ingestion
docker compose exec clickhouse clickhouse-client \
  --user naap_admin --password changeme \
  --query "SELECT event_type, count() FROM naap.events GROUP BY event_type"

# Check Kafka consumer progress
docker compose exec clickhouse clickhouse-client \
  --user naap_admin --password changeme \
  --query "SELECT table, sum(num_messages_read) FROM system.kafka_consumers GROUP BY table"

# Change data TTL (default 365 days)
docker compose exec clickhouse clickhouse-client \
  --user naap_admin --password changeme \
  --query "ALTER TABLE naap.events MODIFY TTL toDateTime(event_ts) + INTERVAL 180 DAY"
```

See `infra/clickhouse/README.md` for the full ClickHouse operations guide.

## Architecture notes

- **Layer rule:** dependencies flow forward only — `Types → Config → Repo → Service → Runtime`. No layer imports from a layer ahead of it.
- **Orch address resolution:** `ai_stream_status` events carry a node-local keypair address, not the on-chain ETH address. Migration 011 resolves this via `orchestrator_info.url → agg_orch_state.uri → orch_address`.
- **Deduplication:** `naap.events` is `ReplacingMergeTree` keyed on `event_id`. Kafka at-least-once duplicates are collapsed in background merges. Queries on raw `naap.events` without `FINAL` may transiently over-count until a merge runs; aggregate tables are not affected.
- **Ingest path:** ClickHouse consumes from Kafka directly via the Kafka Engine — there is no application-layer consumer between Kafka and ClickHouse.

## Documentation

| Document | Purpose |
|---|---|
| `docs/DESIGN.md` | System architecture and layer rules |
| `docs/PRODUCT_SENSE.md` | Product goals and success criteria |
| `docs/design-docs/` | Architecture decision records and behavioral contracts |
| `docs/design-docs/data-validation-rules.md` | Data validation behavioral contract (17 rules, 31 tests) |
| `docs/product-specs/` | Per-feature requirements (R1–R6) |
| `docs/exec-plans/` | Phase execution plans and history |
| `infra/clickhouse/README.md` | ClickHouse operations guide |
| `deploy/README.md` | Production deployment guide (Portainer) |
| `GET /docs` | Interactive API documentation (Swagger UI) |
| `GET /docs/openapi.yaml` | OpenAPI 3.0.3 spec |
