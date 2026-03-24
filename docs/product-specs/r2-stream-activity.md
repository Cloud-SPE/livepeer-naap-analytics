# Spec: R2 — Stream Activity

**Status:** approved
**Priority:** P0 / launch
**Requirement IDs:** STR-001 through STR-010
**Source events:** `stream_trace`, `ai_stream_status`, `pipelines_api_request`
**ADRs:** ADR-001, ADR-002

---

## Problem

Consumers need to know how many streams are active right now, how many ran over
any historical time window, and what the success/failure split looks like.

## Key identifiers

A stream lifecycle is correlated via three IDs present across events:
- `stream_id` — primary identifier for a stream session (e.g. `stream-o5s73x-mk5zc8fy`)
- `request_id` — per-request identifier (e.g. `db936fab`)
- `session_id` — payment session identifier (e.g. `01b55632`)

A stream begins at `gateway_receive_stream_request` and ends at
`gateway_ingest_stream_closed`. If `gateway_no_orchestrators_available` is emitted
instead of progressing to `gateway_send_first_ingest_segment`, the stream is a failure.

**Design decision:** A stream is considered "active" if its most recent `stream_trace`
or `ai_stream_status` event is within the last 30 seconds and no `gateway_ingest_stream_closed`
event has been seen. Threshold is configurable.

---

## Requirements

### STR-001: Active streams count

`GET /v1/streams/active`

Returns the current count of active streams and a breakdown by pipeline and org.

**Response:**
```json
{
  "data": {
    "total_active": 93,
    "active_threshold_seconds": 30,
    "by_org": {
      "daydream": 71,
      "cloudspe": 22
    },
    "by_pipeline": {
      "streamdiffusion-sdxl": 48,
      "streamdiffusion-sdxl-faceid": 28,
      "streamdiffusion-sdxl-v2v": 12,
      "other": 5
    },
    "by_state": {
      "ONLINE": 78,
      "LOADING": 8,
      "DEGRADED_INPUT": 5,
      "DEGRADED_INFERENCE": 2
    }
  },
  "meta": { ... }
}
```

**Acceptance criteria:**
- [ ] STR-001-a: Count matches streams with events within `active_threshold_seconds`
- [ ] STR-001-b: `LOADING` / `DEGRADED_*` states are sourced from `ai_stream_status.data.state`
- [ ] STR-001-c: Streams with no pipeline info are bucketed as `other`

---

### STR-002: Stream summary for a time window

`GET /v1/streams/summary`

**Query params:**
- `?start=` / `?end=` — time window (default: last 24 hours)
- `?org=` — filter by org
- `?pipeline=` — filter by pipeline
- `?window=today|yesterday|7d|30d` — named windows as shortcuts

**Response:**
```json
{
  "data": {
    "time_window": { "start": "...", "end": "..." },
    "total_started": 4821,
    "total_completed": 4203,
    "total_failed": 618,
    "success_rate": 0.872,
    "no_orch_available_count": 411,
    "no_orch_available_rate": 0.085,
    "avg_duration_seconds": 47.3,
    "p50_duration_seconds": 38.1,
    "p99_duration_seconds": 210.4,
    "by_pipeline": { ... },
    "by_org": { ... }
  },
  "meta": { ... }
}
```

**Acceptance criteria:**
- [ ] STR-002-a: `total_started` = count of `gateway_receive_stream_request` events in window
- [ ] STR-002-b: `total_failed` = streams that received `gateway_no_orchestrators_available` before `gateway_send_first_ingest_segment`
- [ ] STR-002-c: `total_completed` = streams with `gateway_ingest_stream_closed` in window
- [ ] STR-002-d: `?window=today` uses calendar day in UTC
- [ ] STR-002-e: Response time ≤ 500ms for any 30-day window

---

### STR-003: Stream history (time-series)

`GET /v1/streams/history`

Returns a time-bucketed series of stream counts for charting.

**Query params:**
- `?start=` / `?end=`
- `?granularity=1m|5m|1h|1d` — bucket size (default: auto based on window)
- `?org=`
- `?pipeline=`

**Response:**
```json
{
  "data": {
    "granularity": "1h",
    "series": [
      {
        "timestamp": "2026-03-24T08:00:00Z",
        "started": 312,
        "completed": 287,
        "failed": 25,
        "no_orch_available": 18,
        "avg_active": 71
      }
    ]
  },
  "meta": { ... }
}
```

**Acceptance criteria:**
- [ ] STR-003-a: Buckets are aligned to UTC boundary (e.g., hourly = top of hour)
- [ ] STR-003-b: `avg_active` is the mean active stream count sampled within the bucket
- [ ] STR-003-c: No bucket is returned for periods with zero events (gaps are explicit nulls)
- [ ] STR-003-d: `granularity=auto` selects: 1m for ≤2h, 5m for ≤24h, 1h for ≤7d, 1d for longer

---

## Data sources and mapping

| API field | Source event | Source field path |
|-----------|-------------|------------------|
| Stream start | `stream_trace` | `data.type == "gateway_receive_stream_request"` |
| Stream end | `stream_trace` | `data.type == "gateway_ingest_stream_closed"` |
| Stream failure | `stream_trace` | `data.type == "gateway_no_orchestrators_available"` |
| `stream_id` | `stream_trace`, `ai_stream_status` | `data.stream_id` |
| `request_id` | multiple | `data.request_id` |
| Pipeline name | `ai_stream_status` | `data.pipeline` |
| Stream state | `ai_stream_status` | `data.state` |
| Org | Kafka topic | `network_events` → `daydream`, `streaming_events` → `cloudspe` |

---

## Out of scope

- Per-stream detail view (individual stream timeline)
- Client-side stream metrics (stream viewer counts)
