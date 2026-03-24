# Phase 8 — Integration Tests + Load Tests

**Status:** complete
**Depends on:** Phase 6
**Started:** 2026-03-24
**Completed:** 2026-03-24

## Goal

Add tests that verify the full stack against real infrastructure and measure
handler throughput under load.

## Deliverables

### Integration tests — `api/internal/repo/clickhouse/integration_test.go`

- Build tag `//go:build integration` keeps them out of `make test`
- Skip automatically when `CLICKHOUSE_ADDR` is not set or ClickHouse is unreachable
- One test per repo method (22 methods, all domains)
- Assert: no SQL error, non-nil pointer responses — no data assertions (test DB may be empty)
- Run with: `make test-integration` (requires `make up` first)

### Benchmark tests — `api/internal/runtime/bench_test.go`

- One `BenchmarkXxx` per endpoint (16 total) in `package runtime_test`
- Uses NoopAnalyticsRepo so results reflect routing + JSON encoding overhead only
- `newTestServer` signature updated to accept `testing.TB` (compatible with both T and B)
- Run with: `make bench`

### k6 load test script — `tests/load/script.js`

- Hits all 21 endpoints randomly across configured VUs / duration
- Custom `rate_limited_responses` counter tracks 429s separately from errors
- `setup()` pre-flight health check aborts the run if API is not healthy
- Default: 10 VUs × 30s, threshold p(95) < 500ms, error rate < 1%
- Run with: `make load-test` (requires k6 and `make up`)

### Makefile targets added

| Target | What it does |
|--------|-------------|
| `make test-integration` | Run integration tests against `localhost:9000` |
| `make bench` | Run Go benchmarks (3 runs, allocs reported) |
| `make load-test` | Run k6 script against `localhost:8000` |

## Files changed

| File | Change |
|------|--------|
| `api/internal/repo/clickhouse/integration_test.go` | New — 22 integration tests |
| `api/internal/runtime/bench_test.go` | New — 16 benchmarks |
| `api/internal/runtime/server_test.go` | `newTestServer` signature `*testing.T` → `testing.TB` |
| `tests/load/script.js` | New — k6 load test script |
| `Makefile` | Added `test-integration`, `bench`, `load-test` targets |

## Validation

```
go test ./internal/... -race -count=1   # all pass
go build ./...                          # clean build
```
