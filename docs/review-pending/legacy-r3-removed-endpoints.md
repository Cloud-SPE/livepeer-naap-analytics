# Legacy R3 Removed Endpoint Drafts

Moved out of `docs/product-specs/r3-performance-quality.md` on 2026-04-09 during
documentation realignment. This file preserves the pre-realignment R3 draft
body for manual review and is intentionally not linked from the active docs
set.

The original draft header claimed `PERF-001` through `PERF-008`, but the body
only defined draft requirements `PERF-001` through `PERF-004`.

## Historical draft route set

- `GET /v1/performance/fps`
- `GET /v1/performance/fps/history`
- `GET /v1/performance/latency`
- `GET /v1/performance/quality`

These routes are not part of the current live router or OpenAPI contract.

## Preserved Draft Body

### Problem

Consumers need to understand the quality of AI inference and stream delivery:
inference FPS per orchestrator and pipeline, WebRTC quality metrics (jitter,
packet loss), and orchestrator discovery latency. These metrics were intended
to help identify degraded orchestrators and pipeline bottlenecks.

### PERF-001: Inference FPS summary

`GET /v1/performance/fps`

Returns inference and input FPS statistics aggregated by orchestrator and
pipeline.

**Draft query params:**

- `?start=` / `?end=` (default: last 1 hour)
- `?org=`
- `?pipeline=`
- `?orch_address=`

**Draft acceptance criteria:**

- FPS values derived from `ai_stream_status.data.inference_status.fps` and `input_status.fps`
- P5, not P1, used as the low-end percentile because single-sample noise is high
- samples with `fps == 0` excluded because they indicate a stream that has not yet started

### PERF-002: FPS time-series

`GET /v1/performance/fps/history`

Time-bucketed FPS series for charting degradation over time.

**Draft query params:**

- `?start=` / `?end=`
- `?granularity=5m|1h|1d`
- `?pipeline=`
- `?orch_address=`

**Draft acceptance criteria:**

- buckets flagged `degraded: true` when average FPS drops by at least 20 percent from the prior bucket

### PERF-003: Discovery latency

`GET /v1/performance/latency`

Returns orchestrator discovery latency from the gateway perspective.

**Draft query params:**

- `?start=` / `?end=`
- `?org=`
- `?orch_address=`

**Draft acceptance criteria:**

- latency sourced from `discovery_results.data[].latency_ms`
- string latency values converted to numeric during ingest

### PERF-004: WebRTC stream quality

`GET /v1/performance/quality`

Returns WebRTC ingest quality metrics aggregated across streams.

**Draft query params:**

- `?start=` / `?end=`
- `?org=`
- `?stream_id=`

**Draft acceptance criteria:**

- data sourced from `stream_ingest_metrics.data.stats.track_stats[]`
- `conn_quality` sourced from `stats.conn_quality`
- `packet_loss_rate` computed as total `packets_lost / packets_received`

## Preserved Source Mapping

| Draft field family | Draft source |
|---|---|
| inference FPS | `ai_stream_status.data.inference_status.fps` |
| input FPS | `ai_stream_status.data.input_status.fps` |
| orchestrator identity | `ai_stream_status.data.orchestrator_info.address` |
| discovery latency | `discovery_results.data[].latency_ms` |
| video jitter | `stream_ingest_metrics.data.stats.track_stats[type=video].jitter` |
| packet loss | `stream_ingest_metrics.data.stats.track_stats[].packets_lost / packets_received` |
| connection quality | `stream_ingest_metrics.data.stats.conn_quality` |

## Notes

- The current repo still contains lower-layer query implementations related to these
  shapes in `api/internal/repo/clickhouse/performance.go`.
- Their presence in lower layers does not make them active product requirements.
- Any future restoration should start by deciding whether they belong in the
  public router and OpenAPI contract, not by reactivating old requirement IDs as
  if they were still shipped.
