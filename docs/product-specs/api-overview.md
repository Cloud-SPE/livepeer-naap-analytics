# Spec: API Overview

**Status:** approved
**Requirement IDs:** API-001 through API-010
**ADRs:** ADR-002

---

## Problem

External developers and partners need a single, well-documented HTTP API to query
analytics data derived from the Livepeer network's Kafka event stream.

## Solution

A versioned REST/JSON API served by the Go `api/` service, backed by ClickHouse
materialized views. No authentication required. IP-based rate limiting for abuse
protection.

## Base URL

```
https://analytics.livepeer.cloud/v1/
```

## Cross-cutting requirements

| ID | Requirement | Acceptance criteria |
|----|-------------|-------------------|
| API-001 | All endpoints respond within 1 second at P99 under normal load | Load test confirms P99 ≤ 1000ms at 60 req/min per IP |
| API-002 | Rate limiting: 60 req/min, 1000 req/hour per IP | `HTTP 429` returned with `Retry-After` header when exceeded |
| API-003 | All responses include `meta.generated_at`, `meta.query_time_ms`, `meta.org` | Integration test validates meta fields on every endpoint |
| API-004 | All endpoints accept `?org=daydream\|cloudspe` filter; omit for all orgs | Test: filtered responses contain only events from that org's Kafka topic |
| API-005 | All time-range params accept ISO 8601 or Unix epoch ms | Invalid values return `HTTP 400` with RFC 7807 error body |
| API-006 | HTTPS only; HTTP redirects to HTTPS | HTTP → HTTPS redirect verified in integration test |
| API-007 | API version in URL path (`/v1/`); breaking changes use `/v2/` | Breaking change policy documented in `docs/design-docs/adr-002-api-design.md` |
| API-008 | List endpoints use cursor-based pagination with `?cursor` and `?limit` | Test confirms stable pagination across inserts |
| API-009 | Error responses use RFC 7807 Problem Details format | All 4xx/5xx responses validated against schema |
| API-010 | Rate limit config (req/min, req/hour) is env-var driven, no code change required | Verified by changing env var and re-running rate limit test |

## Endpoint map (active endpoints)

| Domain | Endpoint | Spec |
|--------|---------|------|
| Network state | `GET /v1/net/orchestrators` | `r1-network-state.md` |
| Network state | `GET /v1/net/models` | `r1-network-state.md` |
| Network state | `GET /v1/net/capacity` | `r1-network-state.md` |
| Performance | `GET /v1/perf/stream/by-model` | `r3-performance-quality.md` |
| SLA | `GET /v1/sla/compliance` | Built-in |
| Network demand | `GET /v1/network/demand` | Built-in |
| GPU | `GET /v1/gpu/network-demand` | Built-in |
| GPU | `GET /v1/gpu/metrics` | Built-in |
| Dashboard | `GET /v1/dashboard/kpi` | Built-in |
| Dashboard | `GET /v1/dashboard/pipelines` | Built-in |
| Dashboard | `GET /v1/dashboard/orchestrators` | Built-in |
| Dashboard | `GET /v1/dashboard/gpu-capacity` | Built-in |
| Dashboard | `GET /v1/dashboard/pipeline-catalog` | Built-in |
| Dashboard | `GET /v1/dashboard/pricing` | Built-in |
| Dashboard | `GET /v1/dashboard/job-feed` | Built-in |
| Dashboard (jobs) | `GET /v1/dashboard/jobs/overview` | Built-in |
| Dashboard (jobs) | `GET /v1/dashboard/jobs/by-pipeline` | Built-in |
| Dashboard (jobs) | `GET /v1/dashboard/jobs/by-capability` | Built-in |
| Jobs (request/response) | `GET /v1/jobs/demand` | Built-in |
| Jobs (request/response) | `GET /v1/jobs/sla` | Built-in |
| Jobs (request/response) | `GET /v1/jobs/by-model` | Built-in |
| AI Batch | `GET /v1/ai-batch/summary` | Built-in |
| AI Batch | `GET /v1/ai-batch/jobs` | Built-in |
| AI Batch | `GET /v1/ai-batch/llm/summary` | Built-in |
| BYOC | `GET /v1/byoc/summary` | Built-in |
| BYOC | `GET /v1/byoc/jobs` | Built-in |
| BYOC | `GET /v1/byoc/workers` | Built-in |
| BYOC | `GET /v1/byoc/auth` | Built-in |
| Health | `GET /healthz` | Built-in, no spec needed |
