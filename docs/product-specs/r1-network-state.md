# Spec: R1 — Network State

**Status:** approved  
**Priority:** P0 / launch  
**Requirement IDs:** NET-001 through NET-003  
**Source surfaces:** `api_current_orchestrator`, `api_current_capability_offer`, `api_current_capability_hardware`, `api_current_byoc_worker`, `api_hourly_streaming_demand`

## Problem

API consumers need current orchestrator inventory, model availability, and
request-serving supply without reconstructing those views from raw capability
payloads.

## Solution

Expose the current network-state surfaces from the published `api_*` layer:

- `GET /v1/dashboard/orchestrators`
- `GET /v1/streaming/models`
- `GET /v1/streaming/orchestrators`
- `GET /v1/requests/models`
- `GET /v1/requests/orchestrators`
- `GET /v1/discover/orchestrators`

## Requirements

### NET-001: Dashboard orchestrator table

`GET /v1/dashboard/orchestrators`

Returns one row per orchestrator with SLA rollups, current pipeline/model
inventory, and GPU count. The default window is `7d`.

Required behavior:

- one row per orchestrator address
- SLA metrics are weighted by requested session volume
- current pipeline and model inventory comes from the published current-state layer
- GPU count is deduped on orchestrator plus GPU identity

### NET-002: Streaming model availability

`GET /v1/streaming/models`

Returns current live-video-to-video supply and recent performance by model.

Required behavior:

- one row per `(pipeline, model)`
- warm inventory comes from current capability offers
- active stream count comes from current active stream state
- 24-hour FPS is recomputed from additive streaming SLA support fields

### NET-003: Request-serving supply inventory

`GET /v1/requests/models`, `GET /v1/requests/orchestrators`, `GET /v1/discover/orchestrators`

These routes expose current non-streaming supply, including BYOC worker-backed
capabilities.

Required behavior:

- request inventory includes both builtin request offers and current BYOC workers
- `GET /v1/requests/orchestrators` emits `pipeline:model` capability strings
- `GET /v1/discover/orchestrators` ranks rows by the latest measured score for that pipeline
- discover `caps` filtering uses OR semantics across repeated `caps` values

## Out of Scope

- historical GPU inventory trends
- per-region public inventory breakdowns
- legacy route aliases
