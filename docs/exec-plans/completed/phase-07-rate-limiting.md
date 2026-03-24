# Phase 7 — Rate Limiting Middleware

**Status:** complete
**Depends on:** Phase 4
**Started:** 2026-03-24
**Completed:** 2026-03-24

## Goal

Add per-IP token-bucket rate limiting to all API routes so that abusive
clients receive `429 Too Many Requests` before hitting ClickHouse.

## Design

- **Algorithm**: token bucket via `golang.org/x/time/rate`
- **Scope**: per `RemoteAddr` (chi's `RealIP` middleware resolves
  `X-Forwarded-For` / `X-Real-IP` before our middleware runs)
- **Response**: `429` with RFC 7807 body and `Retry-After: 1` header
- **Disabled**: when `RATE_LIMIT_RPS=0` the middleware is a no-op (useful
  for local development and tests)
- **Memory**: stale IP entries pruned every minute (TTL 3 min)

## Configuration

| Env var | Default | Description |
|---------|---------|-------------|
| `RATE_LIMIT_RPS` | `30` | Sustained requests per second per IP |
| `RATE_LIMIT_BURST` | `60` | Initial burst capacity |

Tests use the zero-value `Config{}` so `RateLimitRPS=0` → disabled.

## Files

| File | Change |
|------|--------|
| `api/internal/config/config.go` | Added `RateLimitRPS float64`, `RateLimitBurst int` |
| `api/internal/runtime/middleware_ratelimit.go` | New — `rateLimitMiddleware`, `ipRateLimiter` |
| `api/internal/runtime/middleware_ratelimit_test.go` | New — disabled, burst, per-IP, Retry-After tests |
| `api/internal/runtime/server.go` | Wired `rateLimitMiddleware` after `RealIP` |
| `api/go.mod` / `go.sum` | Added `golang.org/x/time v0.9.0` direct dep |

## Validation

```
go test ./internal/...   # all pass
```
