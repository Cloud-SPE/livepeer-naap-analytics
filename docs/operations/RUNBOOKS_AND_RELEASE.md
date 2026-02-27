# Operations, Replay, and Release

## Local Bring-Up

1. `cp .env.template .env`
2. Ensure data directories exist and are writable:
   - `mkdir -p data/kafka data/flink/tmp/checkpoints data/flink/tmp/savepoints data/gateway data/clickhouse data/grafana`
   - `chmod -R 777 data/`
3. Start stack: `docker compose up -d`
4. Verify: `docker compose ps`

## Service Endpoints

| Service | Endpoint | Notes |
|---|---|---|
| Grafana | `http://localhost:3000` | Dashboards |
| Flink UI | `http://localhost:8081` | Job and task manager status |
| ClickHouse HTTP | `http://localhost:8123` | Query/API endpoint |
| Kafka UI | `http://localhost:8080/ui` | Topic and consumer diagnostics |

## Operational Configuration

### Core Environment Variables

| Variable | Default | Used by | Purpose |
|---|---|---|---|
| `QUALITY_KAFKA_BOOTSTRAP` | `kafka:9092` | Main Flink pipeline | Input Kafka bootstrap servers |
| `QUALITY_INPUT_TOPIC` | `streaming_events` | Main Flink pipeline | Primary input topic |
| `QUALITY_DLQ_TOPIC` | `events.dlq.streaming_events.v1` | Main Flink pipeline | DLQ routing target |
| `QUALITY_QUARANTINE_TOPIC` | `events.quarantine.streaming_events.v1` | Main Flink pipeline | Duplicate/expected reject target |
| `QUALITY_DEDUP_TTL_MINUTES` | `1440` | Main Flink pipeline | Dedup state retention |
| `REPLAY_KAFKA_BOOTSTRAP` | `QUALITY_KAFKA_BOOTSTRAP` | Replay job | Kafka bootstrap for replay |
| `REPLAY_DLQ_TOPIC` | `QUALITY_DLQ_TOPIC` | Replay job | Replay source topic |
| `REPLAY_OUTPUT_TOPIC` | `QUALITY_INPUT_TOPIC` | Replay job | Replay destination topic |
| `REPLAY_START_EPOCH_MS` | none | Replay job | Inclusive replay window start |
| `REPLAY_END_EPOCH_MS` | none | Replay job | Exclusive replay window end |
| `CLICKHOUSE_URL` | `http://clickhouse:8123` | Main Flink pipeline | ClickHouse sink endpoint |
| `CLICKHOUSE_DATABASE` | `livepeer_analytics` | Main Flink pipeline | Target database |
| `CLICKHOUSE_USER` | `analytics_user` | Main Flink pipeline | Sink authentication user |
| `CLICKHOUSE_PASSWORD` | none | Main Flink pipeline | Sink authentication secret |

### Docker Compose Profiles

| Profile | Command pattern | Purpose |
|---|---|---|
| default | `docker compose up -d` | Start main analytics stack |
| replay | `docker compose --profile replay up flink-replay-submitter` | Run bounded DLQ replay job |

## Flink Deployment Workflow

- First deployment:
  - upload jar and run job (see `docs/operations/FLINK_DEPLOYMENT.md`)
- Clean update:
  - stop with savepoint,
  - capture savepoint location,
  - upload new jar,
  - run with `savepointPath`.

### Deployment API Commands

- Cluster health:
  - `curl http://YOUR_FLINK_SERVER:8081/overview`
  - `curl http://YOUR_FLINK_SERVER:8081/jobs/overview`
- Upload jar:
  - `curl -X POST -H "Expect:" -F "jarfile=@livepeer-analytics-flink-0.1.0.jar" http://YOUR_FLINK_SERVER:8081/jars/upload`
- First run:
  - `curl -X POST http://YOUR_FLINK_SERVER:8081/jars/<jarid>/run -H "Content-Type: application/json" -d '{"entryClass":"com.livepeer.analytics.pipeline.StreamingEventsToClickHouse","parallelism":1}'`
- Savepoint stop:
  - `curl -X POST http://YOUR_FLINK_SERVER:8081/jobs/<jid>/stop -H "Content-Type: application/json" -d '{"drain":false}'`
- Savepoint location:
  - `curl http://YOUR_FLINK_SERVER:8081/jobs/<jid>/savepoints/<request-id>`
- Restore run:
  - `curl -X POST http://YOUR_FLINK_SERVER:8081/jars/<new_jarid>/run -H "Content-Type: application/json" -d '{"entryClass":"com.livepeer.analytics.pipeline.StreamingEventsToClickHouse","parallelism":1,"savepointPath":"/opt/flink/storage/savepoints/<savepoint-id>"}'`

## DLQ / Quarantine Operations

- Triage DLQ spikes with dashboard and ClickHouse queries.
- Apply producer or parser fix.
- Replay bounded windows using replay submitter:
  - set `REPLAY_START_EPOCH_MS` and `REPLAY_END_EPOCH_MS`
  - run `docker compose --profile replay up flink-replay-submitter`
- Validate no new DLQ spikes and no duplicate regressions.

### Replay Triage SQL

```sql
SELECT
  failure_class,
  failure_reason,
  count() AS total
FROM livepeer_analytics.streaming_events_dlq
WHERE ingestion_timestamp >= now() - INTERVAL 1 HOUR
GROUP BY failure_class, failure_reason
ORDER BY total DESC;
```

```sql
SELECT
  event_id,
  event_type,
  failure_class,
  failure_reason,
  payload_body
FROM livepeer_analytics.streaming_events_dlq
WHERE ingestion_timestamp >= now() - INTERVAL 1 HOUR
ORDER BY ingestion_timestamp DESC
LIMIT 20;
```

### Replay Regression Checklist

1. Confirm recovered rows in typed/fact tables for the replay window.
2. Confirm DLQ rate returns to baseline after replay.
3. Confirm no duplicate regressions in replayed windows.
4. Confirm replay window is compatible with dedup TTL assumptions.

## Disaster Recovery Runbooks

- Recovery priorities:
  - restore ClickHouse serving availability.
  - restore Flink ingestion and offset correctness.
  - rehydrate missing windows from replay/archive paths.
- Backup strategy:
  - ClickHouse: scheduled database backups plus optional table freeze snapshots.
  - Kafka: treat as short-retention transport; authoritative replay source is archive/replay path.
  - MinIO/archive store: long-retention replay source; mirror to external object storage on a schedule.
- Scenario: ClickHouse table corruption:
  1. isolate affected table(s).
  2. recreate schema from `configs/clickhouse-init/01-schema.sql`.
  3. restore from backup or replay bounded windows.
  4. validate rollup and API view parity post-restore.
- Scenario: complete data loss (host/volume):
  1. restore `data/` volumes from backup.
  2. restart stack and verify service health.
  3. replay bounded gaps if any source windows are missing.
- Scenario: Kafka offset loss / consumer-group drift:
  1. stop or savepoint running Flink jobs.
  2. repair/reset consumer group offset policy.
  3. relaunch job from approved offset/savepoint.
  4. verify freshness + duplicate safety checks.
- Scenario: schema migration rollback:
  1. deploy additive/compatible schema first.
  2. run dual-path or bounded backfill validation where required.
  3. cut consumers over only after parity checks pass.
  4. keep rollback path to previous view/table contract until release gate passes.

## Backup and Retention Policy

- Database retention in serving tables is contract-driven by table TTLs in `configs/clickhouse-init/01-schema.sql`.
- Keep backup retention windows explicit per environment (dev/staging/prod), and document overrides in release notes.
- Treat configuration and schema as Git-backed source-of-truth; treat data backups as operational recovery artifacts.

## Health Checks

- Input-to-fact freshness lag
- Rollup lag and API view parity
- Attribution coverage completeness
- Session/segment consistency
- DLQ/quarantine rates

## Release Readiness Checklist

- Flink tests: `cd flink-jobs && mvn test` pass.
- Integration SQL assertions: `tests/integration/run_all.sh` pass.
- Schema and row mapper sync is green.
- Rollup/API parity checks are green for target windows.
- Canonical docs in `docs/` are updated.
- Cutover/rollback notes are recorded in PR.

## Primary Runbook References

- `docs/operations/FLINK_DEPLOYMENT.md`
- `docs/operations/REPLAY_RUNBOOK.md`
- `docs/quality/DATA_QUALITY.md`
