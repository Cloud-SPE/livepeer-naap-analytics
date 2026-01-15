# Livepeer NaaP Analytics

This project provides a real-time data pipeline for monitoring and analyzing **Livepeer Network-as-a-Product (NaaP)** metrics. It ingests streaming events from a Livepeer gateway, processes them using Apache Flink, and stores the analytics in ClickHouse for visualization via Grafana.

## 📂 Project Artifacts

### 🏗️ Infrastructure & Orchestration

* **`docker-compose.yml`**: The primary orchestration file that spins up the entire stack, including the gateway, Kafka, Flink, ClickHouse, and Grafana.
* **`Dockerfile.webapp`**: A multi-stage Dockerfile used to build the Stream Test UI and serve it via a Caddy webserver.
* **`Caddyfile`**: Configuration for the Caddy webserver, handling reverse proxying for the AI runner, orchestrator, and Kafka SSE API.
* **`.env.template`**: A template for environment variables required for the gateway and network settings.

### 🛠️ Stream Processing (Apache Flink)

* **`StreamingEventsToClickHouse.scala`**: The core Scala application that consumes raw JSON from Kafka, parses it, and routes it to specific ClickHouse tables.
* **`build.sbt` & `build.properties**`: Configuration files for the Scala Build Tool (SBT) used to compile the Flink job.
* **`plugins.sbt`**: Adds the `sbt-assembly` plugin to create "fat" JARs for Flink deployment.
* **`submit-jobs.sh`**: A shell script that automates the submission of the compiled JAR to the Flink JobManager.

### 📊 Storage & Visualization

* **`01-schema.sql`**: SQL initialization script that defines the ClickHouse database schema and materialized views for metrics like FPS and ingest quality.
* **`default-user.xml`**: Configures the `analytics_user` with necessary permissions in ClickHouse.
* **`clickhouse.yml` & `dashboard.yml**`: Grafana provisioning files to automatically configure the ClickHouse datasource and dashboard folders.
* **`naap-overview.json`**: A pre-configured Grafana dashboard for real-time NaaP performance monitoring.

### 📡 Gateway & Ingest

* **`mediamtx.yml`**: Configuration for the MediaMTX server, which handles WebRTC/RTMP ingest and egress.
* **`index.html`**: A simple event source viewer for testing Kafka messages via the Zilla SSE API.

## 🚀 Getting Started

### 1. Prerequisites

Ensure you have **Docker** and **Docker Compose** installed.


### 2. Create Required Directory Structure

Ensure the following directories exist in your project root:

```bash
# Create Kafka data directory
mkdir -p data/kafka

# Create Flink checkpoint and savepoint directories
mkdir -p data/flink/tmp/checkpoints
mkdir -p data/flink/tmp/savepoints

# Ensure other data directories exist
mkdir -p data/gateway
mkdir -p data/clickhouse
mkdir -p data/grafana

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

## 🔗 Service Endpoints

Once running, the following interfaces are available:

*Note: Since the Stream Test UI has a requirement for HTTPS, ideally run the webserver behind a cloudflare tunnel for proper HTTPS support*

* **Stream Test UI**: [http://localhost:8088/](https://www.google.com/search?q=http://localhost:8088/)
* **Grafana Dashboards**: [http://localhost:3000/](https://www.google.com/search?q=http://localhost:3000/) (Default: `admin/admin`)
* **ClickHouse UI**: [http://localhost:8123/dashboard](https://www.google.com/search?q=http://localhost:8123/dashboard) (User: `analytics_user` / Pass: `analytics_password`)
* **Flink Manager**: [http://localhost:8081/](https://www.google.com/search?q=http://localhost:8081/)
* **Kafka UI (Kafbat)**: [http://localhost:8080/ui](https://www.google.com/search?q=http://localhost:8080/ui)

---

## 🛠️ Troubleshooting & Permissions

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

### Updated "Run the Project" Sequence

For a smooth first-time setup, the recommended command sequence is:

1. `cp .env.template .env` (and edit values)
2. `mkdir -p data/kafka data/flink/tmp/checkpoints data/flink/tmp/savepoints`
3. `chmod -R 777 data/`
4. `docker compose up`
---

## 🛠️ Build Tester UI (optional)
`DockerFile.webapp` is used by requires the checking out of Brad's live-video-to-video app

```
git clone https://github.com/ad-astra-video/livepeer-app-pipelines.git
cd livepeer-app-pipelines/livep-video-to-video
git checkout 6e83149a09c74d316948d142758fa3dad7c8fc39
ocker build -t tztcloud/livepeer-rtav-test-ui:latest -f Dockerfile.webapp .
```

