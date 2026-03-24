# Spec: R5 — Reliability & Failure Analysis

**Status:** approved
**Priority:** P0 / launch
**Requirement IDs:** REL-001 through REL-008
**Source events:** `stream_trace`, `ai_stream_status`, `discovery_results`
**ADRs:** ADR-001, ADR-002

---

## Problem

Consumers and operators need to understand how reliable the network is: failure rates,
orch swap rates, no-orch-available events, and which orchestrators are consistently
underperforming. From live data, ~10% of streams have failures and ~15% of stream
requests result in `gateway_no_orchestrators_available` — these are key operational KPIs.

---

## Failure taxonomy

Derived from `stream_trace.data.type` values observed in live data:

| Failure type | Event | Description |
|-------------|-------|-------------|
| No orch available | `gateway_no_orchestrators_available` | Gateway could not find any orch to handle request |
| Orch swap | `orchestrator_swap` | Gateway switched to a different orch mid-stream |
| Inference restart | `ai_stream_status.restart_count > 0` | Orch restarted inference pipeline |
| Inference error | `ai_stream_status.last_error != null` | Inference pipeline errored |
| Degraded inference | `ai_stream_status.state == DEGRADED_INFERENCE` | Inference running below acceptable quality |
| Degraded input | `ai_stream_status.state == DEGRADED_INPUT` | Input stream quality issue |

---

## Requirements

### REL-001: Reliability summary

`GET /v1/reliability/summary`

**Query params:**
- `?start=` / `?end=` (default: last 24 hours)
- `?org=`
- `?window=today|yesterday|7d|30d`

**Response:**
```json
{
  "data": {
    "time_window": { "start": "...", "end": "..." },
    "stream_success_rate": 0.872,
    "no_orch_available_rate": 0.151,
    "orch_swap_count": 14,
    "orch_swap_rate": 0.003,
    "inference_restart_rate": 0.098,
    "degraded_state_rate": 0.063,
    "failure_breakdown": {
      "no_orch_available": 411,
      "orch_swap": 14,
      "inference_restart": 267,
      "inference_error": 31,
      "degraded_inference": 89,
      "degraded_input": 129
    }
  },
  "meta": { ... }
}
```

**Acceptance criteria:**
- [ ] REL-001-a: `no_orch_available_rate` = count of `gateway_no_orchestrators_available` / count of `gateway_receive_stream_request` in window
- [ ] REL-001-b: `stream_success_rate` = streams reaching `gateway_receive_few_processed_segments` or `gateway_ingest_stream_closed` / total started
- [ ] REL-001-c: `orch_swap_rate` = count of `orchestrator_swap` events / total streams started
- [ ] REL-001-d: `inference_restart_rate` = streams with `restart_count > 0` / total active streams

---

### REL-002: Reliability history (time-series)

`GET /v1/reliability/history`

**Query params:**
- `?start=` / `?end=`
- `?granularity=5m|1h|1d`
- `?org=`

**Response:** Time-bucketed series with `success_rate`, `no_orch_available_rate`, `degraded_rate` per bucket.

**Acceptance criteria:**
- [ ] REL-002-a: Rates are null (not 0) for buckets with fewer than 5 stream events (avoid misleading percentages from tiny samples)

---

### REL-003: Orchestrator reliability

`GET /v1/reliability/orchestrators`

Per-orchestrator reliability breakdown.

**Query params:**
- `?start=` / `?end=`
- `?org=`
- `?limit=50&cursor=<opaque>`
- `?sort=failure_rate|swap_rate|restart_rate` (default: `failure_rate` desc)

**Response:**
```json
{
  "data": [
    {
      "address": "0x...",
      "name": "speedybird.xyz",
      "streams_handled": 312,
      "failure_rate": 0.021,
      "swap_rate": 0.009,
      "restart_rate": 0.006,
      "degraded_rate": 0.045,
      "avg_inference_fps": 17.8,
      "discovery_latency_ms_p95": 130
    }
  ]
}
```

**Acceptance criteria:**
- [ ] REL-003-a: Only orchs with ≥ 10 stream events in the window are included (avoids noise from rarely-used orchs)
- [ ] REL-003-b: `swap_rate` derived from `orchestrator_swap` events where this orch was the one being swapped away from

---

### REL-004: Failure event log

`GET /v1/reliability/failures`

Paginated log of individual failure events for debugging.

**Query params:**
- `?start=` / `?end=`
- `?org=`
- `?failure_type=no_orch_available|orch_swap|inference_error|inference_restart`
- `?orch_address=`
- `?limit=100&cursor=<opaque>`

**Response:**
```json
{
  "data": [
    {
      "timestamp": "2026-03-24T09:10:03Z",
      "failure_type": "no_orch_available",
      "stream_id": "stream-abc123",
      "request_id": "db936fab",
      "org": "cloudspe",
      "gateway": "0x...",
      "detail": { }
    }
  ]
}
```

**Acceptance criteria:**
- [ ] REL-004-a: Results ordered by `timestamp` descending
- [ ] REL-004-b: `detail` contains the full `data` payload from the source event
- [ ] REL-004-c: Max `?limit` is 500; requests above this return `HTTP 400`

---

## Data sources and mapping

| Failure type | Source event | Detection logic |
|-------------|-------------|-----------------|
| No orch available | `stream_trace` | `data.type == "gateway_no_orchestrators_available"` |
| Orch swap | `stream_trace` | `data.type == "orchestrator_swap"` |
| Inference restart | `ai_stream_status` | `data.inference_status.restart_count > 0` |
| Inference error | `ai_stream_status` | `data.inference_status.last_error != null` |
| Degraded inference | `ai_stream_status` | `data.state == "DEGRADED_INFERENCE"` |
| Degraded input | `ai_stream_status` | `data.state == "DEGRADED_INPUT"` |

---

## Out of scope

- Root cause analysis of failures (would require correlation with infrastructure logs)
- Alerting / notifications on failure threshold breach (future feature)
