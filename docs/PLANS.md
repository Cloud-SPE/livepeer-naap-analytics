# Plans

Index of execution plans. Plans are first-class artifacts in this repository.

## Active plans

See `docs/exec-plans/active/` for in-progress work.

*(None — Phases 1, 3, and 4 complete. Start Phase 5 next.)*

## Implementation sequencing

| Phase | Work | Depends on | Status |
|-------|------|------------|--------|
| 1 | ClickHouse schema + Kafka engine tables | Nothing | ✅ complete |
| 2 | Python pipeline decommission / ClickHouse direct ingest | Phase 1 | pending |
| 3 | Go API — ClickHouse repo layer | Phase 1 | ✅ complete |
| 4 | Go API — service + runtime for NET, STR endpoints | Phase 3 | ✅ complete |
| 5 | Go API — PERF, PAY, REL endpoints | Phase 4 | pending |
| 6 | Go API — leaderboard + scoring | Phase 5 | pending |
| 7 | Rate limiting middleware | Phase 4 | pending |
| 8 | Integration tests + load tests | Phase 6 | pending |

Create an exec-plan in `docs/exec-plans/active/` before starting each phase.

## Completed plans

See `docs/exec-plans/completed/` for historical plans and outcomes.

## Technical debt

See `docs/exec-plans/tech-debt-tracker.md`.
