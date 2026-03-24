# Plan: Phase 3 — Go API ClickHouse Repo Layer

**Status:** complete
**Started:** 2026-03-24
**Depends on:** Phase 1 (ClickHouse schema)
**Blocks:** Phase 4 (NET/STR endpoints), Phase 5 (PERF/PAY/REL endpoints)

---

## Goal

Replace the scaffold placeholder types and `NoopAnalyticsRepo` with real domain
types and a ClickHouse-backed repo. All 21 API methods are defined and implemented
at the repo level. The service layer becomes a thin delegator. The runtime registers
all routes as 501 stubs ready for Phase 4 to fill in.

Phase ends when: the Go API binary starts, connects to ClickHouse, and the `/healthz`
endpoint returns a real ClickHouse ping result.

---

## Scope

**In scope:**
- Domain types for all 6 requirement areas (R1–R6)
- ClickHouse DSN config fields
- `AnalyticsRepo` interface with 21 methods + `Ping`
- `ClickHouseRepo` implementation (clickhouse-go/v2 native protocol)
- Migration 010: fix `uri` field extraction bug from migration 005
- Service layer: updated interface, thin delegation to repo
- Runtime: all 22 routes registered, 501 stubs pending Phase 4
- `/healthz` extended to verify ClickHouse connectivity

**Out of scope:**
- HTTP response serialization (Phase 4)
- Business logic in service layer (Phase 4+)
- Leaderboard scoring algorithm (Phase 6)
- Rate limiting (Phase 7)

---

## Steps

- [x] Create exec-plan
- [x] Migration 010: fix orch_uri extraction in mv_orch_state
- [x] Domain types (api/internal/types/ split by domain)
- [x] ClickHouse config fields
- [x] go.mod: add clickhouse-go/v2
- [x] AnalyticsRepo interface (all 21 methods + Ping)
- [x] ClickHouseRepo implementation (repo/clickhouse/ split by domain)
- [x] Service interface update + thin delegation impl
- [x] Runtime: register all routes, 501 stubs
- [x] main.go: wire ClickHouseRepo
- [x] go mod tidy + build passes
- [x] /healthz returns real ClickHouse ping

---

## Decisions made in this plan

1. **clickhouse-go/v2 native protocol (not database/sql)** — UInt64 columns (WEI
   amounts) are handled natively. database/sql can silently truncate uint64 values
   above int64 max. Tradeoff: non-standard scan API; mitigated by small abstraction
   surface.

2. **WEI as custom Go type with JSON string marshaling** — `type WEI uint64`
   implements `json.Marshaler` to emit decimal strings. Matches PAY-001-a requirement.
   Prevents float64 precision loss in both storage and wire format.

3. **GPU/model extraction in Go, not SQL** — `raw_capabilities` stores deeply nested
   JSON with dict keys (GPU slot indices, integer capability IDs). Extracting these
   in ClickHouse SQL requires complex nested `JSONExtractKeys`+`arrayJoin` patterns.
   Fetching raw JSON and parsing in Go is simpler and correct at the current orch
   count (~85). Revisit if orch count grows beyond 1000.

4. **FPS/latency percentiles from raw `naap.events`** — aggregate tables store sum/count
   for avg FPS but not quantile states. Correct p5/p50/p99 require scanning raw events.
   Acceptable: `ai_stream_status` is ~5% of total event volume; query scans ~5K–50K rows
   for a 24h window. Revisit with AggregatingMergeTree in Phase 8 if P99 latency is a problem.

5. **Leaderboard scoring stubbed to 0.0 in Phase 3** — composite score (FPS 30%,
   reliability 30%, volume 20%, latency 20%) is Phase 6 work. Phase 3 returns orchs
   with raw metrics; score field is 0.0.

6. **Migration 010 fixes the `uri` extraction bug** — migration 005 extracted
   `JSONExtractString(orch_json, 'uri')` but the actual field is `orch_uri`. Migration
   010 drops and recreates the MV with correct extraction. Existing rows are cleared
   and repopulated from live events within minutes.

---

## Schema deviations discovered (live data vs spec)

| Field | Spec assumed | Actual live data |
|-------|-------------|-----------------|
| Orch URI | `uri` | `orch_uri` |
| Orch name | `name` (human) | `local_address` = checksummed ETH address; name derived from URI hostname |
| GPU memory | GB float | bytes UInt64 (divide by 1024³) |
| Pricing field | `price_per_unit` | `pricePerUnit` (camelCase) |
| Capability ID | pipeline name | integer (mapped via `capabilities_names`) |

---

## Progress log

2026-03-24: Plan created. Live data inspection completed. Beginning implementation.
