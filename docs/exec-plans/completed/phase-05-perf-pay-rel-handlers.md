# Plan: Phase 5 — PERF, PAY, REL HTTP Handlers

**Status:** complete
**Started:** 2026-03-24
**Depends on:** Phase 4 (NET/STR handlers)
**Blocks:** Phase 6 (leaderboard scoring)

---

## Goal

Replace the remaining 501 stubs for PERF (R3), PAY (R4), and REL (R5) endpoints
with real handlers. Pattern is identical to Phase 4.

Phase ends when all 12 endpoints return real data from ClickHouse.

---

## Scope

**In scope:**
- `handlers_perf.go`: 4 PERF handlers
- `handlers_pay.go`: 4 PAY handlers
- `handlers_rel.go`: 4 REL handlers
- Wire 12 routes in `buildRouter`; leaderboard route stays as 501
- Update `server_test.go` 501 table
- Handler tests for all three files
- Fix UInt64 scan targets in `performance.go` (SampleCount int64 ← count() UInt64)

**Out of scope:**
- Leaderboard (Phase 6)
- Rate limiting (Phase 7)

---

## Steps

- [x] Create exec-plan
- [x] Fix UInt64 scan issues in performance.go
- [x] `handlers_perf.go`
- [x] `handlers_pay.go`
- [x] `handlers_rel.go`
- [x] Wire 12 routes in buildRouter
- [x] Update server_test.go
- [x] Handler tests (perf, pay, rel)
- [x] go build + go test pass
- [x] Validate live endpoints

---

## Progress log

2026-03-24: Plan created. Beginning implementation.
