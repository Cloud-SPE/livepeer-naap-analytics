# Spec: R3 — Performance & Quality

**Status:** approved  
**Priority:** P0 / launch  
**Requirement IDs:** PERF-001 through PERF-003
**Source surfaces:** `api_hourly_streaming_sla`, `api_hourly_streaming_demand`, `api_hourly_streaming_gpu_metrics`

## Problem

Consumers need streaming performance and quality surfaces that are safe to page
and safe to aggregate, without rebuilding them from raw session rows.

## Solution

Serve performance and quality from the published hourly `api_*` layer through:

- `GET /v1/streaming/models`
- `GET /v1/streaming/sla`
- `GET /v1/streaming/demand`
- `GET /v1/streaming/gpu-metrics`

## Requirements

### PERF-001: Model-level performance

`GET /v1/streaming/models`

Required behavior:

- one row per live-video-to-video `(pipeline, model)`
- warm supply comes from current inventory
- active demand comes from current active stream state
- 24-hour FPS is recomputed from additive hourly SLA inputs

### PERF-002: Orchestrator SLA compliance

`GET /v1/streaming/sla`

Required behavior:

- one row per `(hour, orchestrator, pipeline, model, gpu)` at the public API grain
- cursor order is stable on the full public row identity
- `effective_success_rate`, `no_swap_rate`, `avg_output_fps`, and `sla_score` reflect the published hourly SLA contract

### PERF-003: Gateway demand and GPU metrics

`GET /v1/streaming/demand`, `GET /v1/streaming/gpu-metrics`

Required behavior:

- demand rows are unique on `(hour, gateway, pipeline, model)`
- GPU metric rows are unique on `(hour, orchestrator, pipeline, model, gpu)`
- additive support fields remain exposed so weighted recomputation stays exact
- neither route exposes internal region-grain duplication

## Out of Scope

- viewer-side delivery quality
- legacy performance-route aliases
- handler-side recomputation from canonical session facts
