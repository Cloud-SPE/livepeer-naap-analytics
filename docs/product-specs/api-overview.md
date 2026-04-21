# Spec: API Overview

**Status:** approved
**Requirement IDs:** API-001 through API-010
**ADRs:** ADR-002

---

## Problem

External developers and partners need one stable HTTP API for dashboard,
streaming, request, and discovery analytics derived from the Livepeer network's
semantic serving layer.

## Solution

A versioned REST/JSON API served by the Go `api/` service and backed by the
published `api_*` ClickHouse contracts. The public `v1` surface is read-only
and uses RFC 7807 errors plus cursor envelopes on paginated routes.

## Base URL

```text
https://naap-api.livepeer.cloud
```

## Cross-cutting requirements

| ID | Requirement | Acceptance criteria |
|----|-------------|-------------------|
| API-001 | The public contract is served from `/v1/*` plus `/healthz` | OpenAPI, router, and docs agree on the live route set |
| API-002 | Cursor routes return `{data, pagination, meta}` | Runtime tests validate `data`, `pagination`, and `meta` on every paginated route |
| API-003 | Non-paginated routes return the payload directly | Handler tests validate arrays and objects for non-cursor routes |
| API-004 | Cursor routes accept only `limit` and `cursor` for pagination | Legacy `offset`, `page`, and `page_size` are rejected with `400` |
| API-005 | Time windows use RFC 3339 `start` / `end` or endpoint-specific `window` | Invalid values return `400` with RFC 7807 problem JSON |
| API-006 | Omitted time windows default to the last 24 hours unless the route documents otherwise | Repo and handler tests cover the default-window behavior |
| API-007 | Public metrics are derived from published `api_*` contracts | Validation tests prevent handler-side aggregate reconstruction from lower layers |
| API-008 | Discover `caps` uses OR semantics | OpenAPI text and implementation agree on OR filtering |
| API-009 | Error responses use RFC 7807 Problem Details | Runtime tests validate content type and status behavior |
| API-010 | Breaking changes require a new API version | The `v1` OpenAPI file is treated as the source of truth for the served surface |

## Endpoint map

| Domain | Endpoint |
|--------|----------|
| Dashboard | `GET /v1/dashboard/kpi` |
| Dashboard | `GET /v1/dashboard/pipelines` |
| Dashboard | `GET /v1/dashboard/orchestrators` |
| Dashboard | `GET /v1/dashboard/gpu-capacity` |
| Dashboard | `GET /v1/dashboard/pipeline-catalog` |
| Dashboard | `GET /v1/dashboard/pricing` |
| Dashboard | `GET /v1/dashboard/job-feed` |
| Streaming | `GET /v1/streaming/models` |
| Streaming | `GET /v1/streaming/orchestrators` |
| Streaming | `GET /v1/streaming/sla` |
| Streaming | `GET /v1/streaming/demand` |
| Streaming | `GET /v1/streaming/gpu-metrics` |
| Requests | `GET /v1/requests/models` |
| Requests | `GET /v1/requests/orchestrators` |
| Requests | `GET /v1/requests/ai-batch/summary` |
| Requests | `GET /v1/requests/ai-batch/jobs` |
| Requests | `GET /v1/requests/ai-batch/llm-summary` |
| Requests | `GET /v1/requests/byoc/summary` |
| Requests | `GET /v1/requests/byoc/jobs` |
| Requests | `GET /v1/requests/byoc/workers` |
| Requests | `GET /v1/requests/byoc/auth` |
| Discover | `GET /v1/discover/orchestrators` |
| Health | `GET /healthz` |

## Cursor Pagination Contract

The cursor-paginated `v1` routes share this envelope:

```json
{
  "data": [],
  "pagination": {
    "next_cursor": "opaque-string",
    "has_more": true,
    "page_size": 50
  },
  "meta": {
    "generated_at": "2026-04-15T12:00:00Z",
    "request_id": "optional-request-id"
  }
}
```

The paginated routes are:

- `GET /v1/streaming/sla`
- `GET /v1/streaming/demand`
- `GET /v1/streaming/gpu-metrics`
- `GET /v1/requests/ai-batch/jobs`
- `GET /v1/requests/byoc/jobs`

Clients request the first page with `?limit=<n>` and continue with
`?limit=<n>&cursor=<pagination.next_cursor>`. Legacy `offset`, `page`, and
`page_size` are rejected on those routes.
