# Spec: R1 — Network State

**Status:** approved  
**Priority:** P0 / launch  
**Requirement IDs:** NET-001 through NET-003  
**Source surfaces:** `api_observed_orchestrator`, `api_observed_capability_offer`, `api_observed_capability_hardware`, `api_observed_byoc_worker`, `api_hourly_streaming_demand`

## Problem

API consumers need observed orchestrator inventory, model availability, and
request-serving supply without reconstructing those views from raw capability
payloads.

## Solution

Expose observed-window network-state surfaces from the published `api_*` layer:

- `GET /v1/dashboard/orchestrators`
- `GET /v1/streaming/models`
- `GET /v1/streaming/orchestrators`
- `GET /v1/requests/models`
- `GET /v1/requests/orchestrators`
- `GET /v1/discover/orchestrators`

## Requirements

### NET-001: Dashboard orchestrator table

`GET /v1/dashboard/orchestrators`

Returns one row per orchestrator with SLA rollups, observed pipeline/model
inventory, and observed GPU UUID count. The default window is `7d`.

Required behavior:

- one row per orchestrator address
- SLA metrics are weighted by requested session volume
- pipeline and model inventory comes from the published observed inventory layer for the selected window
- GPU UUID count is deduped on orchestrator plus reported GPU identity; it is
  not an authoritative physical-device count because cloud and virtualized
  orchestrators can report changing UUIDs

### NET-002: Streaming model availability

`GET /v1/streaming/models`

Returns observed live-video-to-video supply over the last 24 hours and recent performance by model.

Required behavior:

- one row per `(pipeline, model)`
- warm inventory comes from observed 24-hour capability offers
- active stream count comes from current active stream state
- 24-hour FPS is recomputed from additive streaming SLA support fields

### NET-003: Request-serving supply inventory

`GET /v1/requests/models`, `GET /v1/requests/orchestrators`, `GET /v1/discover/orchestrators`

These routes expose observed non-streaming supply, including BYOC worker-backed
capabilities.

Required behavior:

- request inventory includes both builtin request offers and observed BYOC workers from the selected window
- `GET /v1/requests/orchestrators` emits `pipeline:model` capability strings
- `GET /v1/discover/orchestrators` ranks rows by the latest measured score for that pipeline
- discover `caps` filtering uses OR semantics across repeated `caps` values

## Out of Scope

- inventory beyond the route window
- per-region public inventory breakdowns
- legacy route aliases
