# Plan: Phase 5 — PERF, PAY, REL HTTP Handlers

**Status:** in_progress
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
- [ ] Fix UInt64 scan issues in performance.go
- [ ] `handlers_perf.go`
- [ ] `handlers_pay.go`
- [ ] `handlers_rel.go`
- [ ] Wire 12 routes in buildRouter
- [ ] Update server_test.go
- [ ] Handler tests (perf, pay, rel)
- [ ] go build + go test pass
- [ ] Validate live endpoints

---

## Progress log

2026-03-24: Plan created. Beginning implementation.
