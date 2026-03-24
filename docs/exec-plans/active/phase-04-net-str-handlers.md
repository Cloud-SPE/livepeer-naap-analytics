# Plan: Phase 4 — NET and STR HTTP Handlers

**Status:** in_progress
**Started:** 2026-03-24
**Depends on:** Phase 3 (Go API repo layer)
**Blocks:** Phase 5 (PERF/PAY/REL endpoints)

---

## Goal

Replace the 501 stubs for the NET (R1) and STR (R2) endpoints with real handlers
that parse query params, call the service, and return JSON. All other endpoints
remain as 501 stubs.

Phase ends when: `GET /v1/net/summary` and `GET /v1/streams/active` return real
data from ClickHouse and all new tests pass.

---

## Scope

**In scope:**
- `writeError` RFC 7807 helper in `server.go`
- `handlers_net.go`: 4 NET handlers
- `handlers_stream.go`: 3 STR handlers
- Wire the 7 routes in `buildRouter`
- Update `server_test.go` to remove the 7 routes from the 501 table
- `handlers_net_test.go` and `handlers_stream_test.go`

**Out of scope:**
- PERF, PAY, REL, leaderboard handlers (Phase 5)
- Granularity-based table routing (deferred, TODO comment added)
- Strict `?start`/`?end` validation (silent fallback kept, tested explicitly)

---

## Steps

- [x] Create exec-plan
- [ ] `writeError` helper in server.go
- [ ] `handlers_net.go` (4 handlers)
- [ ] `handlers_stream.go` (3 handlers)
- [ ] Wire 7 routes in buildRouter; remove notImplemented stubs
- [ ] Update server_test.go (remove 7 routes from 501 table)
- [ ] `handlers_net_test.go`
- [ ] `handlers_stream_test.go`
- [ ] go build + go test pass

---

## Decisions

1. **nil slices → empty JSON arrays in handlers** — The repo returns `nil` when
   there are no rows (idiomatic Go). The HTTP handler normalises nil to `[]T{}`
   before encoding. Service layer is unchanged.

2. **Silent fallback for bad `?start`/`?end`** — `parseQueryParams` already
   substitutes defaults when params are malformed. Phase 4 keeps this contract
   and documents it with `_SilentFallback` test names. Strict rejection deferred.

3. **`?granularity` accepted but ignored** — Passed through to service/repo but
   the repo always uses hourly buckets. A TODO comment marks where routing would
   hook in Phase 5.

4. **`problemDetail` struct is unexported** — It is a serialisation detail, not
   a domain type. Defined adjacent to `writeError` in `server.go`.

---

## Progress log

2026-03-24: Plan created. Beginning implementation.
