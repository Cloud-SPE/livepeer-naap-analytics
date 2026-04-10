# Replay AI Batch / BYOC Events from ignored_raw_events

| Field | Value |
|---|---|
| **Status** | Active |
| **Applies to** | `naap.normalized_ai_batch_job`, `naap.normalized_ai_llm_request`, `naap.normalized_byoc_job`, `naap.normalized_byoc_auth`, `naap.normalized_worker_lifecycle`, `naap.normalized_byoc_payment` |

---

## When to use this runbook

Use this procedure when AI batch or BYOC events were rejected during ingest (written to `naap.ignored_raw_events` with an `ignore_reason`) and need to be replayed into the normalized tables after the root cause is fixed.

This is a manual INSERT…SELECT replay. It is **not** a migration — there is no schema change involved.

---

## Prerequisites

- The underlying validation/parsing bug that caused the ignore has been fixed and deployed.
- The events are still within the 90-day TTL window of `naap.ignored_raw_events`.
- You have ClickHouse `INSERT` access to the normalized tables.

---

## Verify the ignored events

```sql
-- Count ignored events by family and reason
SELECT
    event_type,
    ignore_reason,
    count() AS cnt,
    min(event_ts) AS oldest,
    max(event_ts) AS newest
FROM naap.ignored_raw_events
WHERE event_type IN (
    'ai_batch_request', 'ai_llm_request',
    'job_gateway', 'job_orchestrator', 'job_auth',
    'worker_lifecycle', 'job_payment'
)
GROUP BY event_type, ignore_reason
ORDER BY event_type, cnt DESC;
```

---

## Replay procedure

Run each INSERT below for the relevant event families. Scope by `event_ts` range if needed.

### AI Batch jobs

```sql
INSERT INTO naap.normalized_ai_batch_job
SELECT
    event_id,
    event_ts,
    org,
    gateway,
    JSONExtractString(data, 'request_id')   AS request_id,
    JSONExtractString(data, 'pipeline')     AS pipeline,
    JSONExtractString(data, 'model_id')     AS model_id,
    event_subtype                           AS subtype,
    if(event_subtype = 'ai_batch_request_completed',
        toUInt8(JSONExtractBool(data, 'success')),
        CAST(NULL, 'Nullable(UInt8)')
    )                                       AS success,
    toUInt16(JSONExtractUInt(data, 'tries'))  AS tries,
    JSONExtractInt(data, 'duration_ms')     AS duration_ms,
    JSONExtractString(data, 'orch_url')     AS orch_url,
    lower(JSONExtractString(data, 'orch_url')) AS orch_url_norm,
    JSONExtractFloat(data, 'latency_score') AS latency_score,
    JSONExtractFloat(data, 'price_per_unit') AS price_per_unit,
    JSONExtractString(data, 'error_type')   AS error_type,
    JSONExtractString(data, 'error')        AS error
FROM naap.ignored_raw_events
WHERE event_type = 'ai_batch_request';
```

### AI LLM requests

```sql
INSERT INTO naap.normalized_ai_llm_request
SELECT
    event_id,
    event_ts,
    org,
    gateway,
    JSONExtractString(data, 'request_id')         AS request_id,
    JSONExtractString(data, 'model')              AS model,
    JSONExtractString(data, 'orch_url')           AS orch_url,
    lower(JSONExtractString(data, 'orch_url'))    AS orch_url_norm,
    event_subtype                                 AS subtype,
    toUInt8(JSONExtractBool(data, 'streaming'))   AS streaming,
    toUInt32(JSONExtractUInt(data, 'prompt_tokens'))     AS prompt_tokens,
    toUInt32(JSONExtractUInt(data, 'completion_tokens')) AS completion_tokens,
    toUInt32(JSONExtractUInt(data, 'total_tokens'))      AS total_tokens,
    JSONExtractInt(data, 'total_duration_ms')     AS total_duration_ms,
    JSONExtractFloat(data, 'tokens_per_second')   AS tokens_per_second,
    JSONExtractFloat(data, 'latency_score')       AS latency_score,
    JSONExtractFloat(data, 'price_per_unit')      AS price_per_unit,
    JSONExtractInt(data, 'ttft_ms')               AS ttft_ms,
    JSONExtractString(data, 'finish_reason')      AS finish_reason,
    JSONExtractString(data, 'error')              AS error
FROM naap.ignored_raw_events
WHERE event_type = 'ai_llm_request'
  AND event_subtype IN ('llm_request_completed', 'llm_stream_completed');
```

### BYOC jobs (job_gateway + job_orchestrator)

```sql
INSERT INTO naap.normalized_byoc_job
SELECT
    event_id,
    event_ts,
    org,
    gateway,
    JSONExtractString(data, 'request_id')                        AS request_id,
    JSONExtractString(data, 'capability')                        AS capability,
    event_subtype                                                AS subtype,
    event_type                                                   AS source_event_type,
    if(event_subtype = 'completed',
        toUInt8(JSONExtractBool(data, 'success')),
        CAST(NULL, 'Nullable(UInt8)')
    )                                                            AS success,
    JSONExtractInt(data, 'duration_ms')                          AS duration_ms,
    toUInt16(JSONExtractUInt(data, 'http_status'))                AS http_status,
    lower(JSONExtractString(data, 'orchestrator_info', 'address')) AS orch_address,
    JSONExtractString(data, 'orchestrator_info', 'url')          AS orch_url,
    lower(JSONExtractString(data, 'orchestrator_info', 'url'))   AS orch_url_norm,
    JSONExtractString(data, 'worker_url')                        AS worker_url,
    toUInt8(JSONExtractBool(data, 'charged_compute'))            AS charged_compute,
    JSONExtractInt(data, 'latency_ms')                           AS latency_ms,
    toInt32(JSONExtractInt(data, 'available_capacity'))          AS available_capacity,
    JSONExtractString(data, 'error')                             AS error
FROM naap.ignored_raw_events
WHERE event_type IN ('job_gateway', 'job_orchestrator');
```

### BYOC auth

```sql
INSERT INTO naap.normalized_byoc_auth
SELECT
    event_id,
    event_ts,
    org,
    gateway,
    JSONExtractString(data, 'request_id')                        AS request_id,
    JSONExtractString(data, 'capability')                        AS capability,
    event_subtype                                                AS subtype,
    lower(JSONExtractString(data, 'orchestrator_info', 'address')) AS orch_address,
    JSONExtractString(data, 'orchestrator_info', 'url')          AS orch_url,
    lower(JSONExtractString(data, 'orchestrator_info', 'url'))   AS orch_url_norm,
    toUInt8(JSONExtractBool(data, 'success'))                    AS success,
    JSONExtractString(data, 'error')                             AS error
FROM naap.ignored_raw_events
WHERE event_type = 'job_auth';
```

### Worker lifecycle

```sql
INSERT INTO naap.normalized_worker_lifecycle
SELECT
    event_id,
    event_ts,
    org,
    gateway,
    JSONExtractString(data, 'capability')                          AS capability,
    lower(JSONExtractString(data, 'orchestrator_info', 'address')) AS orch_address,
    JSONExtractString(data, 'orchestrator_info', 'url')            AS orch_url,
    lower(JSONExtractString(data, 'orchestrator_info', 'url'))     AS orch_url_norm,
    JSONExtractString(data, 'worker_url')                          AS worker_url,
    JSONExtractFloat(data, 'price_per_unit')                       AS price_per_unit,
    JSONExtractString(data, 'worker_options', 1, 'model')          AS model,
    JSONExtractRaw(data, 'worker_options')                         AS worker_options_raw
FROM naap.ignored_raw_events
WHERE event_type = 'worker_lifecycle';
```

### BYOC payments

```sql
INSERT INTO naap.normalized_byoc_payment
SELECT
    event_id,
    event_ts,
    org,
    gateway,
    JSONExtractString(data, 'request_id')                          AS request_id,
    JSONExtractString(data, 'capability')                          AS capability,
    lower(JSONExtractString(data, 'orchestrator_info', 'address')) AS orch_address,
    JSONExtractFloat(data, 'amount')                               AS amount,
    JSONExtractString(data, 'currency')                            AS currency,
    event_subtype                                                  AS payment_type
FROM naap.ignored_raw_events
WHERE event_type = 'job_payment';
```

---

## After replay

1. Verify row counts in each normalized table match expectations.
2. Trigger a dbt run to refresh canonical and api_ views:
   ```bash
   make dbt-run MODELS=canonical_ai_batch_jobs,canonical_byoc_jobs,...
   ```
3. Confirm API endpoints return the replayed data.

---

## Limitations

- Events older than 90 days are permanently purged from `ignored_raw_events`. They cannot be replayed via this procedure.
- For events older than 7 days but within 90 days: use this runbook.
- For events within the last 7 days: alternatively, reset the ClickHouse Kafka consumer group offset to re-consume from `naap.events.raw` (see `data-retention-policy.md`).
