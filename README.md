# NAAP Analytics

Analytics API for the Livepeer AI Network (NAAP). Events flow from Kafka into
ClickHouse via the Kafka Engine, and are surfaced through a Go REST API covering
network state, stream activity, performance, payments, reliability, and an
orchestrator leaderboard.

## Current Architecture

The supported pipeline is:

1. Kafka topics land in ClickHouse Kafka engine tables.
2. Ingest materialized views route rows into `accepted_raw_events` and `ignored_raw_events`.
3. `normalized_*` tables capture event-family facts and rollups.
4. The resolver publishes corrected current and serving state into `canonical_*_store` and `api_*_store` tables.
5. `dbt` publishes semantic `canonical_*` and `api_*` views.
6. The Go API and Grafana read those published serving contracts.

The hot read path is no longer built around `naap.events` or compatibility `v_api_*` views. The supported serving spine is the accepted-raw -> normalized -> resolver current/store -> dbt semantic-view path.

## Key Docs

- [`docs/design-docs/system-visuals.md`](docs/design-docs/system-visuals.md) shows the supported data-flow and deployment diagrams.
- [`docs/operations/run-modes-and-recovery.md`](docs/operations/run-modes-and-recovery.md) documents standard runtime modes, failure recovery, and rebuild procedures.
- [`docs/operations/compose-services.md`](docs/operations/compose-services.md) documents each Docker Compose service, profile, and responsibility.
- [`docs/generated/schema.md`](docs/generated/schema.md) lists the extracted v1 bootstrap schema inventory.
- [`infra/clickhouse/bootstrap/v1.sql`](infra/clickhouse/bootstrap/v1.sql) is the generated fresh-volume bootstrap baseline.

## Project structure

```
api/                    Go REST API (port 8000)
  cmd/server/           API server entrypoint
  cmd/resolver/         Resolver service entrypoint (separate binary)
  internal/
    config/             Environment config (envconfig)
    enrichment/         Livepeer API polling worker (ENS names, stake, gateway data, GPU inventory)
    providers/          Cross-cutting: logger, telemetry, Kafka client
    repo/clickhouse/    ClickHouse query layer (R1–R15)
    service/            Business logic (leaderboard scoring, etc.)
    types/              Domain types shared across layers
    runtime/            HTTP handlers, middleware, Prometheus metrics, server wiring
      static/           Embedded assets (openapi.yaml)
    validation/         Scenario-based data-quality test harness (31 tests)
    resolver/           Resolver service logic

warehouse/              dbt semantic analytics layer
  dbt_project.yml       dbt project config
  profiles.yml          ClickHouse connection profile
  models/
    staging/            Thin views over normalized tables
    canonical/          Authoritative semantic derivations
    api/                Presentation/read-model layer
  macros/               dbt macros
  tests/                dbt data tests
  README.md             Ownership boundaries and tier contracts

infra/
  clickhouse/
    config/             ClickHouse server config overrides (including Prometheus endpoint)
    bootstrap/          Generated fresh-volume bootstrap schema
    init/               Docker entrypoint schema runner (bootstrap or migrations)
    migrations/         Forward migrations only after the v1 bootstrap baseline
  docker/               Dockerfiles for api, clickhouse, dbt, resolver
  grafana/
    dashboards/         Grafana dashboard JSON files (auto-loaded on startup)
                          naap-overview.json        — system health overview
                          naap-live-operations.json — real-time stream activity
                          naap-economics.json       — payments and revenue metrics
                          naap-performance-drilldown.json — FPS, latency, WebRTC
                          naap-supply-inventory.json — GPU supply and capacity
    provisioning/       Grafana datasource and dashboard provider config
                          datasources/clickhouse.yml  — grafana-clickhouse-datasource
                          datasources/prometheus.yml  — Prometheus datasource
  kafka/                Kafka topic definitions
  prometheus/           Prometheus scrape configuration

deploy/
  infra1/               Production deployment stacks for infra1 (consumes infra2 Kafka)
    app/                API service stack
    resolver/           Resolver service stack
    clickhouse/         ClickHouse stack (SASL_SSL to infra2 Kafka)
    grafana/            Grafana stack
    prometheus/         Prometheus stack (180-day retention)
    traefik/            Traefik reverse proxy (Cloudflare DNS challenge TLS)
    warehouse/          dbt warehouse stack
  infra2/               Production deployment stacks for infra2 (hosts Kafka broker)
    app/                API service stack
    clickhouse/         ClickHouse stack (local Kafka, SASL_PLAINTEXT)
    grafana/            Grafana stack
    prometheus/         Prometheus stack (90-day retention)
    traefik/            Traefik reverse proxy (Cloudflare DNS challenge TLS)
    kafka/              Kafka KRaft stack (SCRAM-SHA-512 auth, external port 9092)
    kafka-ui/           Kafka UI stack (kafka-ui.cloudspe.com)
    mirrormaker2/       Kafka MirrorMaker2 — replicates network_events from Confluent Cloud
    warehouse/          dbt warehouse stack
    .env.example        Production env template for infra2
    README.md           infra2 deployment guide
  stack.yml             Legacy monolithic stack reference
  README.md             Production deployment guide (Portainer / Docker Swarm)

scripts/                Utility scripts (setup, backfill SQL, performance measurement)

docs/
  DESIGN.md             Architecture overview, layer rules, tier contract, key decisions
  PLANS.md              Phase planning and implementation status
  PRODUCT_SENSE.md      Product goals, success criteria, non-goals
  metrics-and-sla-reference.md  Community-facing metrics reference: formulas, SLA targets, glossary
  design-docs/          Architecture decision records (ADRs) and behavioral contracts
    index.md            Design docs index
    architecture.md     Layer rules and enforcement model
    core-beliefs.md     Project tenets and agent-first principles
    system-visuals.md   Mermaid diagrams: ingest flow, resolver, deployment topology
    adr-001-storage-architecture.md   ClickHouse + Kafka engine decision
    adr-002-api-design.md             REST/JSON, auth model, org model, rate limiting
    adr-003-tiered-serving-contract.md  Tier semantics and canonical derivation rules
    data-validation-rules.md          Behavioral contract for all 17 validation rules (31 tests)
    selection-centered-attribution.md  Attribution model specification
  operations/           Operational runbooks and reference guides
    operations-runbook.md       Deployment, alerting, troubleshooting, maintenance, backups
    devops-environment-guide.md Monitoring, local/production environment setup, Kafka/ClickHouse procedures
    data-retention-policy.md    Kafka and ClickHouse retention windows, replay strategy
    infra-hardening-runbook.md  Security posture, Kafka listener architecture, open hardening items
    incident-response.md        Severity definitions (P0–P3), escalation contacts, post-mortem template
    run-modes-and-recovery.md   Resolver run modes, failure recovery, rebuild procedures
    compose-services.md         Docker Compose services, profiles, and responsibilities
  generated/            Generated schema and baseline references
    schema.md           Extracted v1 bootstrap schema inventory
  exec-plans/           Per-phase execution plans (active + completed)
  product-specs/        Feature requirements (R1–R15)
    index.md            Spec index with requirement traceability

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

# 2. Start the runtime stack
make up

# 3. Verify everything is healthy (~30s for bootstrap and service startup)
curl http://localhost:8000/healthz
# → {"status":"ok"}

# 4. Browse the API and observability stack
open http://localhost:8000/docs   # Swagger UI
open http://localhost:9090        # Prometheus
open http://localhost:3000        # Grafana (anonymous admin, no login required)

# 5. Stop
make down
```

`make up` starts the always-on runtime and includes a one-shot `warehouse-init`
service so fresh volumes get the semantic `canonical_*` and `api_*` views
without keeping a warehouse container idling in the default stack.

**Make targets reference:**

```bash
# Stack
make up                       # Start full local stack
make down                     # Stop and remove containers
make logs                     # Follow all service logs
make up-tooling               # Start the optional dbt tooling container

# Build
make build                    # Build all Go binaries (go build ./...)
make push                     # Build and push all Docker images to registry
make push-api                 # Build and push API image only
make push-resolver            # Build and push resolver image only

# Testing
make test                     # Go unit tests with race detector
make test-integration         # Integration tests (requires running ClickHouse)
make test-validation          # Data-quality validation tests (isolated environment)
make bench                    # Benchmarks
make load-test                # k6 load test (requires API on localhost:8000)

# ClickHouse
make ch-query                 # Interactive ClickHouse shell
make ch-smoke                 # Quick smoke test (event counts, agg table rows)
make migrate-status           # Show migration state
make migrate-up               # Run pending migrations

# Warehouse (dbt)
make warehouse-run            # Publish dbt semantic views once
make warehouse-test           # Run dbt tests
make warehouse-compile        # Compile dbt project (dry-run)

# Resolver
make resolver-logs            # Follow resolver logs
make resolver-auto            # Run resolver in auto mode (bootstrap + tail)
make resolver-bootstrap       # Run resolver in bootstrap-only mode
make resolver-tail            # Run resolver in tail-only mode
make resolver-repair-window   # Repair a specific time window (requires FROM= and TO=)

# Inspect
make inspect                  # Run event inspector against production broker
make inspect-json             # Run event inspector with JSON output
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
| 9363 | ClickHouse Prometheus metrics |
| 9102 | Resolver Prometheus metrics |
| 9100 | node-exporter host metrics (internal) |

## Building

```bash
make build            # Build all Go binaries (no Docker)
make push             # Build and push all Docker images (api, clickhouse, dbt, resolver)
make push-api         # Build and push API image only
make push-resolver    # Build and push resolver image only
```

To build Go binaries directly:

```bash
cd api && go build -o bin/server ./cmd/server
cd api && go build -o bin/resolver ./cmd/resolver
```

Production images require registry access (`docker login tztcloud`). Set `IMAGE_TAG` to tag a specific version:

```bash
IMAGE_TAG=v1.2.3 make push
```

## Testing

```bash
make test             # Go unit tests with race detector (includes enrichment client + GPU parsing tests)

# Integration tests — requires a running ClickHouse (make up first)
CLICKHOUSE_ADDR=localhost:9000 make test-integration

# Data-quality validation tests — run against an isolated validation profile
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
| Network | `/v1/net/` | orchestrators, models, capacity |
| Performance | `/v1/perf/` | by-model |
| SLA | `/v1/sla/` | compliance |
| Network Demand | `/v1/network/` | demand |
| GPU | `/v1/gpu/` | network-demand, metrics |
| Dashboard | `/v1/dashboard/` | kpi, pipelines, orchestrators, gpu-capacity, pipeline-catalog, pricing, job-feed |

Common query parameters: `org`, `start`, `end`, `limit`, `offset`, `active_only`.

### Capacity
| Method | Path | Description |
|--------|------|-------------|
| GET | `/v1/net/capacity` | GPU supply (warm orchs, VRAM) vs. active stream demand |

## Resolver service

The resolver is an always-on background service that publishes corrected current and serving
state into the `canonical_*_store` and `api_*_store` tables. It runs alongside the API in the
local stack and has its own Prometheus metrics endpoint on port `9102`.

**Run modes** (controlled by `RESOLVER_MODE`):

| Mode | Description |
|---|---|
| `auto` | Bootstrap backlog then switch to continuous tail (production default) |
| `bootstrap` | One-shot: process the full closed historical backlog and exit |
| `tail` | Continuous: keep the live lateness window current only |
| `repair` | Repair a specific time window; use with `make resolver-repair-window FROM= TO=` |

**Useful targets:**

```bash
make resolver-auto                                              # auto mode (bootstrap + tail)
make resolver-bootstrap                                         # bootstrap only
make resolver-tail                                              # tail only
make resolver-repair-window FROM=2026-01-01T00:00:00Z TO=2026-01-02T00:00:00Z
```

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

### Resolver service

| Variable | Default | Description |
|---|---|---|
| `RESOLVER_ENABLED` | `true` | Enable/disable the resolver service |
| `RESOLVER_MODE` | `auto` | Run mode: `auto`, `bootstrap`, `tail`, or `repair` |
| `RESOLVER_INTERVAL` | `1m` | How often the resolver polls for new work in tail mode |
| `RESOLVER_LATENESS_WINDOW` | `10m` | Look-back window for late-arriving events |
| `RESOLVER_DIRTY_QUIET_PERIOD` | `2m` | Wait after last write before re-resolving a dirty stream |
| `RESOLVER_CLAIM_TTL` | `2m` | Lock TTL for in-flight resolution claims |
| `RESOLVER_VERSION` | `selection-centered-v1` | Attribution logic version |
| `RESOLVER_BATCH_SIZE` | `500` | Records processed per resolver batch |
| `RESOLVER_PORT` | `9102` | Prometheus metrics port for the resolver |

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
| `KAFKA_SECURITY_PROTOCOL` | *(empty)* | Leave empty for local plaintext; `SASL_SSL` for production |
| `KAFKA_SASL_MECHANISM` | *(empty)* | e.g. `SCRAM-SHA-512` for production |
| `KAFKA_SASL_USERNAME` | *(empty)* | SASL username for ClickHouse Kafka Engine |
| `KAFKA_SASL_PASSWORD` | *(empty)* | SASL password for ClickHouse Kafka Engine |
| `CLICKHOUSE_ADMIN_PASSWORD` | `changeme` | Admin user password |
| `CLICKHOUSE_WRITER_PASSWORD` | `naap_writer_changeme` | Writer user password |
| `CLICKHOUSE_READER_PASSWORD` | `naap_reader_changeme` | Reader user password |
| `CLICKHOUSE_SCHEMA_MODE` | `bootstrap` | `bootstrap` = apply full schema on fresh volume; `verify` = check-only on existing volume |

### Build / Deploy

| Variable | Default | Description |
|---|---|---|
| `IMAGE_TAG` | `latest` | Docker image tag used by `make push` and production stack files |

> **Important:** `KAFKA_BROKER_LIST` is baked into the Kafka Engine table DDL at
> migration time. To change the broker on an already-running instance, set the new
> value in `.env` then run `make down && make up` (bootstrap re-applies on a fresh
> container). See `infra/clickhouse/README.md` for details.

## Deploying to production

Production deployments run as **Docker Swarm stacks**, with each service in its own stack file under `deploy/infra1/` and `deploy/infra2/`. See [`deploy/README.md`](deploy/README.md) and [`deploy/infra2/README.md`](deploy/infra2/README.md) for full Portainer/Swarm deployment instructions.

### Environments

**infra2** — hosts the Kafka broker, ClickHouse, API, Grafana, Kafka UI, and MirrorMaker2.

**infra1** — hosts a second ClickHouse instance (consuming from infra2 Kafka via SASL_SSL), API, Resolver, and Grafana.

Both environments use **Traefik** as the reverse proxy with Cloudflare DNS challenge for automatic TLS.

### Quick start (infra2)

```bash
git clone <repo-url>
cd livepeer-naap-analytics
cp deploy/infra2/.env.example deploy/infra2/.env
# Edit .env — set all passwords and domain names
```

Edit `.env` for production:

```bash
# Strong passwords (avoid | character — used as delimiter in ClickHouse SQL)
CLICKHOUSE_ADMIN_PASSWORD=<strong-password>
CLICKHOUSE_WRITER_PASSWORD=<strong-password>
CLICKHOUSE_READER_PASSWORD=<strong-password>
NAAP_CONSUMER_PASSWORD=<strong-password>

# Kafka
KAFKA_LOG_RETENTION_HOURS=168

# Grafana
GF_ADMIN_USER=admin
GF_ADMIN_PASSWORD=<strong-password>

# Traefik (Cloudflare DNS challenge)
CLOUDFLARE_EMAIL=<your-cf-email>
CF_API_KEY=<your-cf-global-api-key>

# Domains
NAAP_API_DOMAIN=naap-api.cloudspe.com
NAAP_GRAFANA_DOMAIN=grafana.cloudspe.com

# Rate limiting
RATE_LIMIT_RPS=100
RATE_LIMIT_BURST=200

KAFKA_AUTO_OFFSET_RESET=latest   # use 'earliest' for initial backfill
```

Deploy each stack via Portainer or `docker stack deploy`:

```bash
docker stack deploy -c deploy/infra2/kafka/stack.yml naap-kafka
docker stack deploy -c deploy/infra2/clickhouse/stack.yml naap-clickhouse
docker stack deploy -c deploy/infra2/app/stack.yml naap-app
# etc.
```

### Production checklist

- [ ] All default passwords changed (admin, reader, writer, consumer, Grafana, Kafka UI)
- [ ] `LOG_LEVEL=info`
- [ ] Traefik Cloudflare credentials set (`CLOUDFLARE_EMAIL`, `CF_API_KEY`)
- [ ] Domain labels set for each service
- [ ] `KAFKA_AUTO_OFFSET_RESET=latest` (after initial backfill is complete)
- [ ] Rate limits tuned (`RATE_LIMIT_RPS`, `RATE_LIMIT_BURST`)
- [ ] ClickHouse data volume on persistent block storage
- [ ] `OTLP_ENDPOINT` set for OTLP tracing (optional; Prometheus metrics are always on)
- [ ] Firewall: only ports 80/443/9092 exposed externally; 8123/9000/9363 internal only
- [ ] Enrichment worker: `ENRICHMENT_ENABLED=true`, `LIVEPEER_API_URL` reachable from host
- [ ] MirrorMaker2 credentials set if replicating from Confluent Cloud (`DAYDREAM_SASL_USERNAME`, `DAYDREAM_SASL_PASSWORD`)

### Initial backfill

On first deployment against a live broker, set `KAFKA_AUTO_OFFSET_RESET=earliest`
to consume full topic history. Monitor progress:

```bash
docker compose exec clickhouse clickhouse-client \
  --user naap_admin --password <password> \
  --query "SELECT count(), min(event_ts), max(event_ts) FROM naap.accepted_raw_events"
```

Once the backfill is complete, change to `KAFKA_AUTO_OFFSET_RESET=latest` and
run `make down && make up` so new consumer sessions start from the current end.

After backfill, restart the stack or run the resolver repair path so the
bootstrap-backed aggregates and current-store surfaces catch up against the
completed accepted-raw history:

```bash
make resolver-repair-window FROM=2026-03-01T00:00:00Z TO=2026-03-02T00:00:00Z
```

## ClickHouse management

```bash
# Interactive shell
make ch-query

# Check event ingestion
docker compose exec clickhouse clickhouse-client \
  --user naap_admin --password changeme \
  --query "SELECT event_type, count() FROM naap.accepted_raw_events GROUP BY event_type"

# Check Kafka consumer progress
docker compose exec clickhouse clickhouse-client \
  --user naap_admin --password changeme \
  --query "SELECT table, sum(num_messages_read) FROM system.kafka_consumers GROUP BY table"

# Change data TTL on a table (example: accepted_raw_events, default 90 days)
docker compose exec clickhouse clickhouse-client \
  --user naap_admin --password changeme \
  --query "ALTER TABLE naap.accepted_raw_events MODIFY TTL toDateTime(event_ts) + INTERVAL 180 DAY"
```

See `infra/clickhouse/README.md` for the full ClickHouse operations guide.

## Architecture notes

- **Layer rule:** dependencies flow forward only — `Types → Config → Repo → Service → Runtime`. No layer imports from a layer ahead of it.
- **Orch address resolution:** `ai_stream_status` events carry a node-local keypair address, not the on-chain ETH address. Migration 011 resolves this via `orchestrator_info.url → agg_orch_state.uri → orch_address`.
- **Deduplication:** `accepted_raw_events` and `normalized_*` tables use `ReplacingMergeTree` keyed on `event_id`. Kafka at-least-once duplicates are collapsed in background merges. The legacy `naap.events` table also uses this pattern but is no longer the primary read path.
- **Ingest path:** ClickHouse consumes from Kafka directly via the Kafka Engine — there is no application-layer consumer between Kafka and ClickHouse.
- **MirrorMaker2:** `network_events` (gateway-originated) are replicated from Confluent Cloud into infra2 Kafka via MirrorMaker2. `streaming_events` are produced directly by Cloud SPE gateways.

### Kafka topics

| Topic | Partitions | Retention | Purpose |
|---|---|---|---|
| `naap.events.raw` | 6 | 7 days | Raw inbound events from gateways |
| `naap.analytics.aggregated` | 3 | 30 days | Pre-aggregated analytics |
| `naap.alerts` | 1 | 7 days | Alert signals |

## Documentation

### Architecture & design

| Document | Purpose |
|---|---|
| [`docs/DESIGN.md`](docs/DESIGN.md) | System architecture overview, layer rules, tier contract, key decisions |
| [`docs/PRODUCT_SENSE.md`](docs/PRODUCT_SENSE.md) | Product goals, success criteria, non-goals |
| [`docs/design-docs/index.md`](docs/design-docs/index.md) | Design docs index (ADRs, behavioral contracts) |
| [`docs/design-docs/architecture.md`](docs/design-docs/architecture.md) | Layer rules and enforcement model |
| [`docs/design-docs/system-visuals.md`](docs/design-docs/system-visuals.md) | Mermaid diagrams: ingest flow, resolver, deployment topology |
| [`docs/design-docs/adr-001-storage-architecture.md`](docs/design-docs/adr-001-storage-architecture.md) | ClickHouse + Kafka engine decision, compression, retention |
| [`docs/design-docs/adr-002-api-design.md`](docs/design-docs/adr-002-api-design.md) | REST/JSON, auth model, org model, rate limiting |
| [`docs/design-docs/adr-003-tiered-serving-contract.md`](docs/design-docs/adr-003-tiered-serving-contract.md) | Tier semantics and canonical derivation rules |
| [`docs/design-docs/data-validation-rules.md`](docs/design-docs/data-validation-rules.md) | Behavioral contract for all 17 validation rules (31 tests) |

### API & metrics

| Document | Purpose |
|---|---|
| [`docs/product-specs/index.md`](docs/product-specs/index.md) | Feature requirements index (R1–R15) with traceability |
| [`docs/metrics-and-sla-reference.md`](docs/metrics-and-sla-reference.md) | Community-facing metrics reference: formulas, SLA targets, scoring models, glossary |
| `GET /docs` | Interactive API documentation (Swagger UI) |
| `GET /docs/openapi.yaml` | OpenAPI 3.0.3 spec |

### Operations

| Document | Purpose |
|---|---|
| [`docs/operations/operations-runbook.md`](docs/operations/operations-runbook.md) | Deployment from scratch, alerting, troubleshooting, maintenance, backups |
| [`docs/operations/devops-environment-guide.md`](docs/operations/devops-environment-guide.md) | Monitoring, local/production environment setup, Kafka offset reset, ClickHouse reload/replay |
| [`docs/operations/data-retention-policy.md`](docs/operations/data-retention-policy.md) | Kafka and ClickHouse retention windows, replay strategy, known gaps |
| [`docs/operations/infra-hardening-runbook.md`](docs/operations/infra-hardening-runbook.md) | Security posture, Kafka listener architecture, open hardening action items |
| [`docs/operations/incident-response.md`](docs/operations/incident-response.md) | Severity definitions (P0–P3), response times, escalation contacts, post-mortem template |
| [`docs/operations/run-modes-and-recovery.md`](docs/operations/run-modes-and-recovery.md) | Resolver run modes, failure recovery, rebuild procedures |
| [`docs/operations/compose-services.md`](docs/operations/compose-services.md) | Docker Compose services, profiles, and responsibilities |

### Component READMEs

| Document | Purpose |
|---|---|
| [`infra/clickhouse/README.md`](infra/clickhouse/README.md) | ClickHouse schema, migrations, Kafka Engine config |
| [`warehouse/README.md`](warehouse/README.md) | dbt ownership boundaries and tier contracts |
| [`deploy/README.md`](deploy/README.md) | Production deployment guide (Portainer / Docker Swarm) |
| [`deploy/infra2/README.md`](deploy/infra2/README.md) | infra2-specific deployment guide |
