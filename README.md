# Livepeer NaaP Analytics

This project provides a real-time data pipeline for monitoring and analyzing **Livepeer Network-as-a-Product (NaaP)** metrics. It ingests streaming events from a Livepeer gateway, processes them using Apache Flink, and stores the analytics in ClickHouse for visualization via Grafana.

[![CI PR Smoke](https://github.com/livepeer/livepeer-naap-analytics/actions/workflows/ci-pr-smoke.yml/badge.svg)](https://github.com/livepeer/livepeer-naap-analytics/actions/workflows/ci-pr-smoke.yml)
[![CI Nightly Full](https://github.com/livepeer/livepeer-naap-analytics/actions/workflows/ci-nightly-full.yml/badge.svg)](https://github.com/livepeer/livepeer-naap-analytics/actions/workflows/ci-nightly-full.yml)
[![CI Manual Deep Verify](https://github.com/livepeer/livepeer-naap-analytics/actions/workflows/ci-manual-deep-verify.yml/badge.svg)](https://github.com/livepeer/livepeer-naap-analytics/actions/workflows/ci-manual-deep-verify.yml)

## Documentation and Agent Navigation

- Quick agent map: `AGENTS.md`
- Canonical documentation set: `docs/README.md`

## Quick Path by Task

| Task | Start here | Then use |
|---|---|---|
| Understand the system end-to-end | `docs/architecture/SYSTEM_OVERVIEW.md` | `docs/data/SCHEMA_AND_METRIC_CONTRACTS.md` |
| Change schema/metrics safely | `docs/data/SCHEMA_AND_METRIC_CONTRACTS.md` | `docs/quality/TESTING_AND_VALIDATION.md`, `docs/workflows/ENGINEERING_WORKFLOW.md` |
| Deploy/redeploy/replay | `docs/operations/RUNBOOKS_AND_RELEASE.md` | `docs/operations/FLINK_DEPLOYMENT.md`, `docs/operations/REPLAY_RUNBOOK.md` |
| Debug data quality issues | `docs/quality/TESTING_AND_VALIDATION.md` | `docs/quality/DATA_QUALITY.md` |
| Review open ideas and backlog | `docs/references/ISSUES_BACKLOG.md` | `docs/references/METRICS_SCHEMA_DESIGN_SCRATCHPAD.md` |

## Project Artifacts

### Infrastructure and Orchestration

* **`docker-compose.yml`**: The primary orchestration file that spins up the entire stack, including the gateway, Kafka, Flink, ClickHouse, and Grafana.
* **`Dockerfile.webapp`**: A multi-stage Dockerfile used to build the Stream Test UI and serve it via a Caddy webserver.
* **`Caddyfile`**: Configuration for the Caddy webserver, handling reverse proxying for the AI runner, orchestrator, and Kafka SSE API.
* **`.env.template`**: A template for environment variables required for the gateway and network settings.

### Stream Processing (Apache Flink)

* **`flink-jobs/src/main/java/com/livepeer/analytics/pipeline/StreamingEventsToClickHouse.java`**: The core Java application that enforces quality gates (schema validation + dedup), parses events, and routes them to ClickHouse tables.
* **`flink-jobs/src/main/java/com/livepeer/analytics/pipeline/DlqReplayJob.java`**: Replay job that re-ingests DLQ payloads into Kafka with replay metadata.
* **`pom.xml`**: Maven build for the Flink job(s), producing the fat JAR.
* **`submit-jobs.sh`**: Submits the main ingestion job to the Flink JobManager.
* **`submit-replay-job.sh`**: Submits the DLQ replay job on demand.

### Source Layout

Packages under `flink-jobs/src/main/java/com/livepeer/analytics` are organized by responsibility:

* `pipeline`: Flink entrypoints and Kafka source wiring.
* `quality`: Schema validation, deduplication, and DLQ envelope construction.
* `parse`: Typed event parsing helpers.
* `sink`: ClickHouse row mapping, sink factory, and sink guards.
* `model`: POJOs for events, envelopes, and payloads.
* `util`: Shared JSON and hashing utilities.

### Storage and Visualization

* **`01-schema.sql`**: SQL initialization script that defines the ClickHouse database schema and materialized views for metrics like FPS and ingest quality.
* **`default-user.xml`**: Configures the `analytics_user` with necessary permissions in ClickHouse.
* **`clickhouse.yml` & `dashboard.yml`**: Grafana provisioning files to automatically configure the ClickHouse datasource and dashboard folders.
* **`naap-overview.json`**: A pre-configured Grafana dashboard for real-time NaaP performance monitoring.
* **`quality-gate.json`**: Grafana dashboard for DLQ/quarantine volume and top failure classes.

### Gateway and Ingest

* **`mediamtx.yml`**: Configuration for the MediaMTX server, which handles WebRTC/RTMP ingest and egress.
* **`index.html`**: A simple event source viewer for testing Kafka messages via the Zilla SSE API.

## Getting Started

### 1. Prerequisites

Ensure you have **Docker** and **Docker Compose** installed.

### 2. Create Required Directory Structure

Ensure the following directories exist in your project root:

```bash
# Create Kafka data directory
mkdir -p data/kafka

# Create Flink checkpoint and savepoint directories
mkdir -p data/flink/tmp/

# Ensure other data directories exist
mkdir -p data/gateway
mkdir -p data/clickhouse
mkdir -p data/grafana

chmod -Rf 777 data/flink/tmp/
```

### 3. Environment Setup

Create a `.env` file from the provided template:

```bash
cp .env.template .env

```

Edit `.env` and provide your specific network details (e.g., `HOST_IP`, `ARB_ETH_URL`).

### 4. Gateway Credentials

Add your Livepeer credentials to the `data/gateway` folder:

*Note: the private key must be in a text file called `eth-secret.txt`*

* `eth-secret.txt` (containing your private key in plain text)
* `key.json` (your Ethereum keystore file)

### 5. Run the Application

Start the entire stack using Docker Compose:

```bash
docker compose up

```

This will automatically build the Flink job, initialize the ClickHouse schema, and start all monitoring services.

## Quality Gate and Replay

The Flink job enforces a quality gate before writing to ClickHouse:

- Schema validation and deterministic failure classification
- Deduplication with TTL (duplicates go to quarantine)
- DLQ envelopes with source pointers and payloads for replay
- Network capability + discovery events emit multiple rows per event when arrays are present

Replay runbook: `docs/operations/REPLAY_RUNBOOK.md`
Quality guardrails: `docs/quality/DATA_QUALITY.md`

### ClickHouse Sink (Flink Connector)

The job uses ClickHouse's `flink-connector-clickhouse` AsyncSink for table writes. Key settings:

- `CLICKHOUSE_URL` (default `http://clickhouse:8123`)
- `CLICKHOUSE_DATABASE` (default `livepeer_analytics`)
- `CLICKHOUSE_USER`
- `CLICKHOUSE_PASSWORD`
- `CLICKHOUSE_SINK_MAX_BATCH_SIZE`
- `CLICKHOUSE_SINK_MAX_IN_FLIGHT`
- `CLICKHOUSE_SINK_MAX_BUFFERED`
- `CLICKHOUSE_SINK_MAX_BATCH_BYTES`
- `CLICKHOUSE_SINK_MAX_TIME_MS`
- `CLICKHOUSE_SINK_MAX_RECORD_BYTES`
The sink uses `JSONEachRow` format to avoid CSV escaping issues with large JSON payloads.
`CLICKHOUSE_SINK_MAX_RECORD_BYTES` is also enforced by a pre-sink guard that routes oversized rows to the DLQ (DLQ/quarantine rows are dropped if oversized to avoid recursion).

### Schema Sync (Mapper Safety)

We keep ClickHouse JSON row mappers in code, so we enforce a schema sync test to prevent drift.
The test parses `configs/clickhouse-init/01-schema.sql` and asserts that every JSON row emitted by
`ClickHouseRowMappers` matches the table columns (excluding materialized/default columns).

When the ClickHouse schema or inbound JSON changes, update these files in this order:

1. `configs/clickhouse-init/01-schema.sql` (table definition changes)
2. `flink-jobs/src/main/java/com/livepeer/analytics/parse/EventParsers.java` (new/changed fields)
3. `flink-jobs/src/main/java/com/livepeer/analytics/model/EventPayloads.java` (payload shapes)
4. `flink-jobs/src/main/java/com/livepeer/analytics/sink/ClickHouseRowMappers.java` (column mapping)
5. Update tests/fixtures as needed: `flink-jobs/src/test/java/com/livepeer/analytics/sink/ClickHouseSchemaSyncTest.java`, `flink-jobs/src/test/java/com/livepeer/analytics/parse/EventParsersTest.java`

## Service Endpoints

Once running, the following interfaces are available:

*Note: Since the Stream Test UI has a requirement for HTTPS, ideally run the webserver behind a cloudflare tunnel for proper HTTPS support*

* **Stream Test UI**: http://localhost:8088/
* **Grafana Dashboards**: http://localhost:3000/ (Default: `admin/admin`)
* **ClickHouse UI**: http://localhost:8123/dashboard (User: `analytics_user` / Pass: `analytics_password`)
* **Flink Manager**: http://localhost:8081/
* **Kafka UI (Kafbat)**: http://localhost:8080/ui

---

## Kafka Topics (Quality Gate)

- `streaming_events` (input)
- `events.dlq.streaming_events.v1` (DLQ)
- `events.quarantine.streaming_events.v1` (duplicates / expected rejects)

## Troubleshooting and Permissions

If you encounter errors where containers (specifically Kafka or Flink) are crashing or "permission denied" errors appear in the logs, it is likely because the host-mounted data directories do not have the correct permissions.

### 1. Fix Permissions (chmod)

To ensure the Docker containers can read and write to these folders regardless of the internal container user, apply full permissions to the `data` tree:

```bash
# Apply recursive permissions to the entire data folder
chmod -R 777 data/

```

### 2. Verification

You can verify the setup by running:

```bash
ls -la data/flink/tmp

```

You should see `drwxrwxrwx` for both `checkpoints` and `savepoints`.

---
### Java 17 Module Access (Flink Tests)

If tests fail with `InaccessibleObjectException` related to `java.util.Arrays$ArrayList`, we enable:
`--add-opens java.base/java.util=ALL-UNNAMED` in `flink-jobs/pom.xml`. This is required by Flink's
Kryo/Chill serializers on Java 17+ to access JDK internals during serialization. Keep this flag in
test JVMs (and in production JVMs if you see the same error).

---

### Updated "Run the Project" Sequence

For a smooth first-time setup, the recommended command sequence is:

1. `cp .env.template .env` (and edit values)
2. `mkdir -p data/kafka data/flink/tmp/checkpoints data/flink/tmp/savepoints`
3. `chmod -R 777 data/`
4. `docker compose up`
---

## Build Tester UI (optional)
`Dockerfile.webapp` requires checking out Brad's live-video-to-video app:

```
git clone https://github.com/ad-astra-video/livepeer-app-pipelines.git
cd livepeer-app-pipelines/livep-video-to-video
git checkout 6e83149a09c74d316948d142758fa3dad7c8fc39
docker build -t tztcloud/livepeer-rtav-test-ui:latest -f Dockerfile.webapp .
```
