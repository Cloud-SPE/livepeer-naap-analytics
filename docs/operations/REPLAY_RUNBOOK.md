# DLQ Replay Runbook

This runbook covers the standard workflow for triaging DLQ events, applying fixes, and replaying data safely.

## Replay Configuration

### Environment Variables

| Variable | Default | Required | Purpose |
|---|---|---|---|
| `REPLAY_KAFKA_BOOTSTRAP` | `QUALITY_KAFKA_BOOTSTRAP` | Yes | Kafka bootstrap for replay job |
| `REPLAY_DLQ_TOPIC` | `QUALITY_DLQ_TOPIC` | Yes | Replay source topic |
| `REPLAY_OUTPUT_TOPIC` | `QUALITY_INPUT_TOPIC` | Yes | Replay destination topic |
| `REPLAY_START_EPOCH_MS` | none | Yes | Inclusive replay window start (epoch ms) |
| `REPLAY_END_EPOCH_MS` | none | Yes | Exclusive replay window end (epoch ms) |

### Replay Topic Contract

| Topic | Role |
|---|---|
| `events.dlq.streaming_events.v1` | Replay source (failed envelopes) |
| `streaming_events` | Replay destination (re-ingest path) |

### Compose Profile

| Command | Purpose |
|---|---|
| `docker compose --profile replay up flink-replay-submitter` | Run replay submitter for configured time window |

## 1) Triage

- Check Grafana dashboard **"Quality Gate - DLQ & Quarantine"** for spikes.
- Query ClickHouse for recent DLQ events:

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

- Sample affected payloads to confirm root cause:

```sql
SELECT event_id, event_type, failure_class, failure_reason, payload_body
FROM livepeer_analytics.streaming_events_dlq
WHERE ingestion_timestamp >= now() - INTERVAL 1 HOUR
ORDER BY ingestion_timestamp DESC
LIMIT 20;
```

## 2) Fix

- Apply producer-side or pipeline fix (schema, enrichment, sink configuration).
- Validate the fix against a saved DLQ payload locally.

## 3) Replay

Replay a bounded time window from DLQ back into `streaming_events`.

1. Set replay window environment variables (epoch milliseconds):

```bash
export REPLAY_START_EPOCH_MS=1738368000000
export REPLAY_END_EPOCH_MS=1738371600000
```

2. Run the replay submitter using the compose profile:

```bash
docker compose --profile replay up flink-replay-submitter
```

The replay job reads from `events.dlq.streaming_events.v1`, adds `__replay` metadata, and republishes to `streaming_events`.

## 4) Regression Test

- Monitor ClickHouse for recovered rows in typed tables.
- Check for new DLQ spikes; if present, stop replay and adjust fixes.
- Validate idempotency by ensuring no duplicate rows appear in target tables within the replay window.

## Notes

- The main quality gate uses dedup state TTL; ensure your replay window fits inside the configured TTL.
- Non-JSON payloads are skipped by replay and remain in DLQ for manual handling.
