# Phase 6 — Leaderboard & Composite Scoring

**Status:** complete
**Depends on:** Phase 5
**Started:** 2026-03-24
**Completed:** 2026-03-24

## Goal

Implement the leaderboard endpoints with composite scoring so that
`GET /v1/leaderboard` returns orchestrators ranked by a normalised score
rather than raw stream volume.

## Endpoints

| Method | Path | Handler |
|--------|------|---------|
| GET | `/v1/leaderboard` | `handleGetLeaderboard` |
| GET | `/v1/leaderboard/{address}` | `handleGetLeaderboardProfile` |

## Scoring algorithm

Min-max normalisation across the current result set, then weighted sum:

```
Score = (0.30 × normFPS + 0.30 × (1 − normDegradedRate)
       + 0.20 × normVolume + 0.20 × (1 − normLatencyMS)) × 100
```

When all values are equal (e.g. single orchestrator), `norm = 1.0`.

## Changes

### `api/internal/repo/clickhouse/leaderboard.go`

Updated `GetLeaderboard` to LEFT JOIN two additional pre-aggregated subqueries:
- `agg_fps_hourly` → `avg_fps` per orchestrator
- `agg_discovery_latency_hourly` → `avg_lat` per orchestrator

Scan `avg_fps` and `avg_lat` as `*float64` (nullable — orchestrators with no
FPS or latency data get 0.0 rather than a scan error).

### `api/internal/service/service.go`

Overrode `GetLeaderboard` to call `scoreLeaderboard()` after the repo fetch.
Added:
- `scoreLeaderboard(entries)` — in-place scoring + sort
- `normMinMax(vals)` — min-max normalisation helper

Added `"sort"` import.

### `api/internal/runtime/handlers_ldr.go` (new)

Two handlers: `handleGetLeaderboard`, `handleGetLeaderboardProfile`.
Nil-slice normalised to `[]LeaderboardEntry{}`. Profile returns 404 on nil.

### `api/internal/runtime/handlers_ldr_test.go` (new)

Tests: happy path (200 + JSON array), org filter, profile 404.

### `api/internal/runtime/server.go`

Replaced `notImplemented` stubs for `/v1/leaderboard` and
`/v1/leaderboard/{address}` with real handlers.

### `api/internal/runtime/server_test.go`

Removed `TestRoutes_NotImplemented` — no 501 routes remain.

## Validation

```
go test ./internal/...   # all pass
```
