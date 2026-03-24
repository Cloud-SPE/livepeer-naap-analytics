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

## Endpoint map (all P0 / launch)

| Domain | Endpoint | Spec |
|--------|---------|------|
| Network state | `GET /v1/network/summary` | `r1-network-state.md` |
| Network state | `GET /v1/network/orchestrators` | `r1-network-state.md` |
| Network state | `GET /v1/network/gpus` | `r1-network-state.md` |
| Network state | `GET /v1/network/models` | `r1-network-state.md` |
| Stream activity | `GET /v1/streams/active` | `r2-stream-activity.md` |
| Stream activity | `GET /v1/streams/summary` | `r2-stream-activity.md` |
| Stream activity | `GET /v1/streams/history` | `r2-stream-activity.md` |
| Performance | `GET /v1/performance/fps` | `r3-performance-quality.md` |
| Performance | `GET /v1/performance/latency` | `r3-performance-quality.md` |
| Performance | `GET /v1/performance/quality` | `r3-performance-quality.md` |
| Payments | `GET /v1/payments/summary` | `r4-payments-economics.md` |
| Payments | `GET /v1/payments/history` | `r4-payments-economics.md` |
| Payments | `GET /v1/payments/by-pipeline` | `r4-payments-economics.md` |
| Reliability | `GET /v1/reliability/summary` | `r5-reliability-failures.md` |
| Reliability | `GET /v1/reliability/orchestrators` | `r5-reliability-failures.md` |
| Reliability | `GET /v1/reliability/failures` | `r5-reliability-failures.md` |
| Leaderboard | `GET /v1/leaderboard/orchestrators` | `r6-orch-leaderboard.md` |
| Health | `GET /healthz` | Built-in, no spec needed |
