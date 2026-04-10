# ADR-002: API Design

**Status:** Accepted
**Date:** 2026-03-24
**Deciders:** Project team

---

## Context

The analytics API must serve external developers and partners with no authentication
barrier, while protecting against abuse. Data must be queryable by organisation and
include real-time network state, stream activity, payments, performance, and orch
reliability data.

---

## Decisions

### Authentication: Open + IP-based rate limiting

No API key required. Rate limiting is enforced per client IP.

**Proposed rate limits (configurable via env vars):**
- 60 requests / minute per IP (burst)
- 1,000 requests / hour per IP (sustained)
- Returns `HTTP 429 Too Many Requests` with `Retry-After` header when exceeded

**Tradeoff accepted:** No auth means no per-consumer analytics, no abuse attribution
beyond IP, and no ability to revoke individual access. Accepted because the goal is
maximum developer accessibility at launch. API key support can be added as an additive
layer later without breaking existing consumers.

**Rejected:** API key requirement at launch. Adds onboarding friction that slows
adoption. The data is not sensitive enough to warrant it at this stage.

### Protocol: REST / JSON over HTTPS

- Base URL: `https://analytics.livepeer.cloud/v1/`
- All responses: `application/json`
- Versioning: URL path prefix (`/v1/`). Breaking changes introduce `/v2/`.
- Pagination: cursor-based for list endpoints (`?cursor=<opaque>&limit=<n>`)
- Time parameters: ISO 8601 strings or Unix epoch milliseconds
- Errors: RFC 7807 Problem Details format

**Tradeoff accepted:** REST is less efficient than gRPC for high-frequency polling.
Accepted because external developer accessibility is the priority. gRPC can be added
as a parallel interface for high-frequency consumers if demand exists.

### Organisation model

Events are tagged by source organisation:

| Kafka topic | Org ID | Org name |
|-------------|--------|----------|
| `network_events` | `daydream` | Daydream |
| `streaming_events` | `cloudspe` | CloudSPE |

- All API endpoints accept an optional `?org=<org_id>` filter
- Omitting `?org` returns data across all orgs (merged)
- Events with empty `gateway` field are attributed to an `other` bucket within their org

### Response standards

Cursor-paginated list responses include:

```json
{
  "data": { ... },
  "meta": {
    "generated_at": "...",
    "request_id": "optional-request-id"
  }
}
```

These responses additionally include:

```json
{
  "data": [...],
  "pagination": {
    "next_cursor": "opaque-string",
    "has_more": true,
    "page_size": 50
  },
  "meta": { ... }
}
```

For cursor-paginated list endpoints, clients send `?limit=<n>&cursor=<opaque>`.
The first page omits `cursor`; subsequent pages reuse `pagination.next_cursor`.
Legacy `offset`, `page`, and `page_size` parameters are rejected with `HTTP 400`.

---

## Rejected alternatives

| Alternative | Reason rejected |
|-------------|----------------|
| GraphQL | Higher implementation cost, steeper client learning curve for partners unfamiliar with it. REST is universally understood. |
| WebSocket / SSE for real-time push | Additive feature, not needed at launch. Polling against sub-second ClickHouse queries is sufficient. |
| Multi-tenant API keys | Adds onboarding barrier. Not needed when data is not sensitive. Revisit if data access controls become a requirement. |

---

## Security considerations

- HTTPS only (TLS 1.2+). HTTP requests redirect to HTTPS.
- Rate limiting is the primary abuse defence.
- No PII is exposed. ETH addresses are pseudonymous and already public on-chain.
- Payment amounts are aggregated; individual ticket values are not exposed.
- IP addresses used for rate limiting are not logged beyond a rolling 1-hour window.

See `docs/product-specs/api-overview.md` for full endpoint listing.
