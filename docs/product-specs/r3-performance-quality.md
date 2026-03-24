# Spec: R3 — Performance & Quality

**Status:** approved
**Priority:** P0 / launch
**Requirement IDs:** PERF-001 through PERF-008
**Source events:** `ai_stream_status`, `stream_ingest_metrics`, `webrtc_stats`, `discovery_results`
**ADRs:** ADR-001, ADR-002

---

## Problem

Consumers need to understand the quality of AI inference and stream delivery:
inference FPS per orchestrator and pipeline, WebRTC quality metrics (jitter, packet loss),
and orch discovery latency. These metrics are key for identifying degraded orchs and
pipeline bottlenecks.

---

## Requirements

### PERF-001: Inference FPS summary

`GET /v1/performance/fps`

Returns inference and input FPS statistics aggregated by orchestrator and pipeline.

**Query params:**
- `?start=` / `?end=` (default: last 1 hour)
- `?org=`
- `?pipeline=`
- `?orch_address=`

**Response:**
```json
{
  "data": {
    "time_window": { "start": "...", "end": "..." },
    "by_pipeline": [
      {
        "pipeline": "streamdiffusion-sdxl",
        "inference_fps": { "avg": 17.2, "p50": 17.5, "p5": 8.1, "p99": 18.9 },
        "input_fps":     { "avg": 19.8, "p50": 20.0, "p5": 10.2, "p99": 20.5 },
        "sample_count": 4821
      }
    ],
    "by_orchestrator": [
      {
        "address": "0xdef1c70578b2b5e8589a42e26980687fc5153079",
        "name": "speedybird.xyz",
        "pipeline": "streamdiffusion-sdxl",
        "inference_fps": { "avg": 17.8, "p50": 18.0, "p5": 9.1, "p99": 18.9 },
        "input_fps":     { "avg": 19.9, "p50": 20.1, "p5": 11.0, "p99": 20.4 },
        "sample_count": 312
      }
    ]
  },
  "meta": { ... }
}
```

**Acceptance criteria:**
- [ ] PERF-001-a: FPS values derived from `ai_stream_status.data.inference_status.fps` and `input_status.fps`
- [ ] PERF-001-b: P5 (not P1) is used as the low-end because single-sample noise is high
- [ ] PERF-001-c: Samples with `fps == 0` are excluded (indicate stream not yet started)

---

### PERF-002: FPS time-series

`GET /v1/performance/fps/history`

Time-bucketed FPS series for charting degradation over time.

**Query params:**
- `?start=` / `?end=`
- `?granularity=5m|1h|1d`
- `?pipeline=`
- `?orch_address=`

**Response:** Same structure as `/streams/history` with `avg_inference_fps`, `avg_input_fps` per bucket.

**Acceptance criteria:**
- [ ] PERF-002-a: Detectable degradation: if avg FPS drops ≥20% from prior bucket, the bucket is flagged with `degraded: true`

---

### PERF-003: Discovery latency

`GET /v1/performance/latency`

Returns orch discovery latency from the gateway's perspective.

**Query params:**
- `?start=` / `?end=`
- `?org=`
- `?orch_address=`

**Response:**
```json
{
  "data": {
    "by_orchestrator": [
      {
        "address": "0x...",
        "name": "speedybird.xyz",
        "latency_ms": { "avg": 77, "p50": 72, "p95": 130, "p99": 210 },
        "sample_count": 184
      }
    ],
    "network_avg_latency_ms": 89
  }
}
```

**Acceptance criteria:**
- [ ] PERF-003-a: Latency sourced from `discovery_results.data[].latency_ms`
- [ ] PERF-003-b: Latency is parsed as numeric (stored as string `"77"` in events — convert at ingest)

---

### PERF-004: WebRTC stream quality

`GET /v1/performance/quality`

Returns WebRTC ingest quality metrics aggregated across streams.

**Query params:**
- `?start=` / `?end=`
- `?org=`
- `?stream_id=`

**Response:**
```json
{
  "data": {
    "time_window": { "start": "...", "end": "..." },
    "video": {
      "avg_jitter_ms": 7.2,
      "packet_loss_rate": 0.002,
      "avg_packets_lost_per_stream": 0.3
    },
    "audio": {
      "avg_jitter_ms": 19.8,
      "packet_loss_rate": 0.001
    },
    "conn_quality_distribution": {
      "good": 0.94,
      "fair": 0.05,
      "poor": 0.01
    }
  }
}
```

**Acceptance criteria:**
- [ ] PERF-004-a: Data sourced from `stream_ingest_metrics.data.stats.track_stats[]`
- [ ] PERF-004-b: `conn_quality` values from `stats.conn_quality` field
- [ ] PERF-004-c: `packet_loss_rate` = total `packets_lost` / total `packets_received` across all samples

---

## Data sources and mapping

| API field | Source event | Source field path |
|-----------|-------------|------------------|
| Inference FPS | `ai_stream_status` | `data.inference_status.fps` |
| Input FPS | `ai_stream_status` | `data.input_status.fps` |
| Orch address | `ai_stream_status` | `data.orchestrator_info.address` |
| Pipeline | `ai_stream_status` | `data.pipeline` |
| Discovery latency | `discovery_results` | `data[].latency_ms` (string → numeric at ingest) |
| Video jitter | `stream_ingest_metrics` | `data.stats.track_stats[type=video].jitter` |
| Packet loss | `stream_ingest_metrics` | `data.stats.track_stats[].packets_lost` / `packets_received` |
| Conn quality | `stream_ingest_metrics` | `data.stats.conn_quality` |

---

## Out of scope

- Per-stream quality detail (individual stream deep-dive)
- Output-side WebRTC metrics (viewer-facing quality)
- GPU utilisation percentages (not in event data)
