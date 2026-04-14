# Spec: R3 — Performance & Quality

**Status:** approved  
**Priority:** P0 / launch  
**Requirement IDs:** PERF-001  
**Source events:** `ai_stream_status`, `network_capabilities`

## Problem

Consumers need a current performance surface that shows how AI models are
performing across the network without reconstructing that view from raw status
history.

## Solution

Serve model-level performance from the published semantic layer through:

- `GET /v1/streaming/models`

The active product contract for R3 is intentionally limited to the live route
set. Supporting semantics and lower-layer historical context are preserved in
[`../references/performance-quality-reference.md`](../references/performance-quality-reference.md).

## Requirements

### PERF-001: Performance by model

`GET /v1/streaming/models`

Returns one row per `(pipeline, model)` for the requested window.

**Query params:**

- `?org=` — optional org filter
- `?start=` / `?end=` — optional window bounds; default window is the last 24 hours
- `?pipeline=` — optional pipeline filter

**Response fields:**

| Field | Meaning |
|---|---|
| `ModelID` | Canonical model identifier |
| `Pipeline` | Canonical pipeline identifier |
| `AvgFPS` | Weighted average output FPS across the requested window |
| `P50FPS` | Current schema field; currently approximated from the same hourly aggregate used for `AvgFPS` |
| `P99FPS` | Current schema field; currently approximated from the same hourly aggregate used for `AvgFPS` |
| `WarmOrchCount` | Distinct warm orchestrators currently advertising the model |
| `TotalStreams` | Schema field reserved for stream-volume joins; current implementation may return `0` |

**Required behavior:**

- derives FPS from published hourly performance aggregates rather than request-time raw scans
- joins performance rows with current warm inventory so only currently advertised model and pipeline pairs appear
- treats warm inventory as orchestrators seen within the active warm-state threshold
- remains aligned with the live router and OpenAPI schema

## Data Sources

| Surface field | Source layer |
|---|---|
| `AvgFPS`, `P50FPS`, `P99FPS` | published hourly performance aggregate |
| `WarmOrchCount` | published GPU inventory and warm-orchestrator state |
| `ModelID`, `Pipeline` | canonical capability inventory and semantic serving layer |

## Out of Scope

- per-orchestrator FPS detail as a public route
- standalone FPS history endpoints
- standalone discovery-latency or WebRTC-quality endpoints
- viewer-side delivery quality metrics
