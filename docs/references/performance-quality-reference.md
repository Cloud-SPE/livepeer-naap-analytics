# Performance & Quality Reference

This document supports the active R3 product spec with implementation-level
semantics that are useful for engineers and agents but do not belong in the
route contract itself.

## Active API Surface

The current public performance surface is:

- `GET /v1/perf/by-model`

This route is defined in:

- [`../../api/internal/runtime/server.go`](../../api/internal/runtime/server.go)
- [`../../api/internal/runtime/static/openapi.yaml`](../../api/internal/runtime/static/openapi.yaml)

## Current Serving Semantics

### Window and filters

- `org`, `start`, `end`, and `pipeline` are the active query parameters
- if `start` and `end` are omitted, the serving layer defaults to the last 24 hours

### Data path

The current model-performance surface is assembled from published semantic
layers rather than request-time scans of raw events:

1. `ai_stream_status` contributes status-derived performance signals
2. [`../../warehouse/models/canonical/canonical_status_hours.sql`](../../warehouse/models/canonical/canonical_status_hours.sql) materializes canonical hourly session status
3. [`../../warehouse/models/api/api_fps_hourly.sql`](../../warehouse/models/api/api_fps_hourly.sql) publishes hourly FPS aggregates and excludes terminal-tail artifacts
4. current capability inventory and warm state are joined through `api_gpu_inventory`
5. [`../../api/internal/repo/clickhouse/model_ext.go`](../../api/internal/repo/clickhouse/model_ext.go) serves the final model-level rows

### Field semantics

| Field | Current implementation note |
|---|---|
| `AvgFPS` | Computed as `sum(inference_fps_sum) / sum(sample_count)` across the requested window |
| `WarmOrchCount` | Counts distinct orchestrators whose inventory row is warm under the active warm-state threshold |
| `P50FPS`, `P99FPS` | Present in the live schema, but currently approximated from the same hourly aggregate as `AvgFPS` because the model-performance route does not yet retain percentile state |
| `TotalStreams` | Present in the live schema, but the current implementation does not yet join stream-volume facts for this route and may return `0` |

## Adjacent Metrics

The repo still tracks quality signals that are important to the broader
performance story, even when they are not separate public routes today:

- discovery latency
- WebRTC ingest quality
- prompt-to-first-frame latency
- end-to-end latency
- output viability and SLA quality scores

Those semantics are documented primarily in:

- [`../metrics-and-sla-reference.md`](../metrics-and-sla-reference.md)
- [`../design-docs/data-validation-rules.md`](../design-docs/data-validation-rules.md)

## Dormant Lower-Layer Query Shapes

The repo still contains lower-layer ClickHouse query implementations for the
older performance surfaces that were once drafted as:

- FPS summary
- FPS history
- discovery latency
- WebRTC quality

Those queries remain in:

- [`../../api/internal/repo/clickhouse/performance.go`](../../api/internal/repo/clickhouse/performance.go)

They are not part of the active router or OpenAPI contract. Treat them as
engineering artifacts or future starting points, not as shipped API surfaces.
