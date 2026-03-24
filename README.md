# NAAP Analytics

Analytics API for the Livepeer AI Network (NAAP). Events flow from Kafka into
ClickHouse via the Kafka Engine, and are surfaced through a Go REST API covering
network state, stream activity, performance, payments, reliability, and an
orchestrator leaderboard.

## How it works

```
Kafka topics                 ClickHouse                     Go API
─────────────────────────────────────────────────────────────────────
network_events   ──Kafka──►  Kafka Engine ──MV──►  naap.events
streaming_events ──Engine──►  tables          │      │
                                              │      └──► Aggregate MVs
                                              │             agg_orch_state
                                              │             agg_stream_*
                                              │             agg_payment_*
                                              │             agg_*_hourly
                                              └──────────────────► HTTP :8000
```

Two Kafka topics are consumed directly by ClickHouse:

| Topic | Org | Volume | Content |
|---|---|---|---|
| `network_events` | `daydream` | ~4.6M events/day | Orch capabilities, discovery, network caps |
| `streaming_events` | `cloudspe` | ~35K events/day | Stream lifecycle, payments, AI status |

Events land in `naap.events` (raw, 365-day TTL) and fan out to pre-aggregated
tables via Materialized Views. The Go API queries only the aggregate tables — no
raw-event queries at request time.

## Project structure

```
api/                    Go REST API (port 8000)
  cmd/server/           Entrypoint
  internal/
    config/             Environment config (envconfig)
    providers/          Cross-cutting: logger, telemetry, Kafka client
    repo/clickhouse/    ClickHouse query layer (R1–R6)
    service/            Business logic (leaderboard scoring, etc.)
    types/              Domain types shared across layers
    runtime/            HTTP handlers, middleware, server wiring
      static/           Embedded assets (openapi.yaml)

pipeline/               Python Kafka consumer (scaffold — not used in prod)
                        ClickHouse ingests directly from Kafka via Kafka Engine

infra/
  clickhouse/
    config/             ClickHouse server config overrides
    init/               Docker entrypoint migration runner
    migrations/         SQL migrations (applied in sort order on first start)
  docker/               Dockerfiles for api and pipeline
  kafka/                Kafka topic definitions

docs/
  DESIGN.md             Architecture overview and layer rules
  PLANS.md              Phase planning and status
  PRODUCT_SENSE.md      Product goals and success criteria
  design-docs/          Architecture decision records (ADRs)
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
| Python 3.11+ | | Pipeline (optional) |

## Running locally

```bash
# 1. Copy and configure environment
cp .env.example .env
# Edit .env — at minimum set passwords; see Configuration below

# 2. Start the full stack (ClickHouse + Kafka + API + Pipeline)
make up

# 3. Verify everything is healthy (~30s for migrations to apply)
curl http://localhost:8000/healthz
# → {"status":"ok"}

# 4. Browse the API
open http://localhost:8000/docs        # Swagger UI
# or
curl http://localhost:8000/docs/openapi.yaml  # raw spec

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
make test             # All tests (unit + fast integration)
make test-api         # Go unit tests with race detector
make test-pipeline    # Python tests (pytest)

# Integration tests — requires a running ClickHouse
CLICKHOUSE_ADDR=localhost:9000 make test-integration

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
| Network (R1) | `/v1/net/` | summary, orchestrators, `/{address}`, gpu, models |
| Streams (R2) | `/v1/streams/` | active, summary, history |
| Performance (R3) | `/v1/perf/` | fps, fps/history, latency, webrtc |
| Payments (R4) | `/v1/payments/` | summary, history, by-pipeline, by-orch |
| Reliability (R5) | `/v1/reliability/` | summary, history, orchs, `/v1/failures` |
| Leaderboard (R6) | `/v1/leaderboard` | list, `/{address}` |

Common query parameters: `org`, `start`, `end`, `limit`, `offset`, `active_only`.

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
| `CLICKHOUSE_USER` | `naap_reader` | Read-only user |
| `CLICKHOUSE_PASSWORD` | *(required)* | Reader password |
| `KAFKA_BROKERS` | `localhost:9092` | Kafka broker(s) for the API service |
| `RATE_LIMIT_RPS` | `30` | Requests/sec per IP (0 = disabled) |
| `RATE_LIMIT_BURST` | `60` | Burst allowance |
| `OTLP_ENDPOINT` | *(empty)* | OTLP trace endpoint; empty disables telemetry |

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

- [ ] All default passwords changed in `.env`
- [ ] `ENV=production` and `LOG_LEVEL=info`
- [ ] TLS terminated at reverse proxy (Caddy or nginx)
- [ ] `KAFKA_AUTO_OFFSET_RESET=latest` (after initial backfill is complete)
- [ ] Rate limits tuned (`RATE_LIMIT_RPS`, `RATE_LIMIT_BURST`)
- [ ] ClickHouse data volume backed up or on persistent block storage
- [ ] `OTLP_ENDPOINT` set for observability
- [ ] Firewall: only ports 80/443 exposed externally; 8123/9000 internal only

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
- **Python pipeline:** the `pipeline/` service is a scaffold and is not used in production. ClickHouse ingests from Kafka directly via the Kafka Engine.

## Documentation

| Document | Purpose |
|---|---|
| `docs/DESIGN.md` | System architecture and layer rules |
| `docs/PRODUCT_SENSE.md` | Product goals and success criteria |
| `docs/design-docs/` | Architecture decision records |
| `docs/product-specs/` | Per-feature requirements (R1–R6) |
| `docs/exec-plans/` | Phase execution plans and history |
| `infra/clickhouse/README.md` | ClickHouse operations guide |
| `GET /docs` | Interactive API documentation (Swagger UI) |
| `GET /docs/openapi.yaml` | OpenAPI 3.0.3 spec |
