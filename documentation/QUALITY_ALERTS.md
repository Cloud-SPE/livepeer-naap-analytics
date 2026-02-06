# DLQ Spike Alert (Grafana)

Use the **Quality Gate - DLQ & Quarantine** dashboard to configure an alert rule on the DLQ volume panel.

Suggested alert query (ClickHouse):

```sql
SELECT
  toStartOfInterval(ingestion_timestamp, INTERVAL 1 MINUTE) AS time,
  count() AS dlq_count
FROM livepeer_analytics.streaming_events_dlq
WHERE ingestion_timestamp >= now() - INTERVAL 15 MINUTE
GROUP BY time
ORDER BY time
```

Suggested alert condition:

- **Reduce:** last()
- **Threshold:** dlq_count > 10 (adjust to baseline)
- **Evaluate every:** 1 minute

This alert intentionally ignores quarantine duplicates and only triggers on DLQ spikes.
