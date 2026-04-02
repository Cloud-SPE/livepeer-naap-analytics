# BACKLOG-029: ClickHouse Rollup Inflation — Live Data Assessment

**Date:** 2026-04-02
**Ref:** [livepeer-treasury-proposals#300](https://github.com/Cloud-SPE/livepeer-treasury-proposals/issues/300)
**Priority:** P1

---

## Summary Verdict

The developer's research is accurate and well-scoped. Two concrete bugs are confirmed in
production-served data. The `FINAL` omission risk is real but currently latent.

---

## Bug 1 — `health_signal_coverage_ratio` Inflation (CONFIRMED, customer-visible)

`health_signal_count` exceeded `health_expected_signal_count` during the 2026-03-23 backfill,
driving the SLA formula above 100. These corrupt scores are still present in the
customer-facing `api_sla_compliance_by_org` table.

```
api_sla_compliance_by_org:
  total rows:          11,397
  sla_score > 100:        312  (daydream org, max score 346.67)
  sla_score < 0:           33
  health_coverage > 1:    396 rows
```

**Time distribution of corruption:**

| Date | Bad Windows | Total | % Bad | Avg Coverage Ratio | Max |
|---|---|---|---|---|---|
| 2026-03-23 | 243 | 261 | 93% | 1.65 | 5.0 |
| 2026-03-24 | 57 | 789 | 7.2% | ~0.99 | 2.2 |
| 2026-03-25+ | 0–3/day | ~1,200/day | <0.1% | ~0.98 | ~1.04 |

**Root cause:** The initial backfill on 2026-03-23 re-replayed events, creating duplicate health
signal records. The SLA formula multiplies all components by the ratio, so a ratio of 5.0 produces
scores up to 5× the maximum (e.g., 346.67).

**Fix:**
```sql
-- In api_sla_compliance_by_org.sql and api_sla_compliance.sql
LEAST(
  health_signal_count / nullif(health_expected_signal_count, 0),
  1.0
) AS health_signal_coverage_ratio
```

---

## Bug 2 — `output_viability_rate` Double-Counting (CONFIRMED, ongoing)

Sessions that satisfy both `loading_only` AND `zero_output_fps` are counted twice in the
failure numerator, making the rate go negative.

```
cloudspe / live-video-to-video, 32 affected windows:
  requested_sessions      = 1
  loading_only_sessions   = 1
  zero_output_fps_sessions = 1
  output_viability_rate   = 1 - (1 + 1) / 1 = -1
  sla_score               = -13.33
```

**Root cause:** The formula uses additive counting:
```
output_viability_rate = 1 - (loading_only_sessions + zero_output_fps_sessions) / requested_sessions
```
A session satisfying both conditions should be counted once. The failure numerator must use
union (`OR`) semantics, not a sum.

**Fix:**
```sql
-- Replace additive failure count with union-based count
countIf(is_loading_only OR has_zero_output_fps) AS effective_failed_sessions,
1 - effective_failed_sessions / nullif(requested_sessions, 0) AS output_viability_rate
```

---

## FINAL Omission Risk (Latent, not currently manifesting)

Checked all three normalized ReplacingMergeTree tables at time of assessment:

| Table | Without FINAL | With FINAL | Inflation |
|---|---|---|---|
| `normalized_ai_stream_status` | 4,086,735 | 4,086,735 | **0%** |
| `normalized_ai_stream_events` | 1,505,050 | 1,505,050 | **0%** |
| `normalized_stream_trace` | 4,070,626 | 4,070,626 | **0%** |

All ReplacingMergeTree merges are complete — zero transient inflation at time of check.

The risk described by the developer remains valid: `performance.go` and `reliability.go` read
from these tables without `FINAL`. After a crash/restart, unmerged duplicate rows would
transiently over-count until background merges complete. This is unresolved tech debt.

---

## agg_* vs api_* Discrepancy — Not a Bug

```
agg_orch_reliability_hourly (2026-04-02):  ai_stream_count = 320,233
api_orchestrator_reliability_hourly:       ai_stream_count =  36,545
Apparent ratio: ~8.8×
```

The `agg_*` tables count **status samples** (~60 per hour per session), while `api_*` tables
count **session-hours**. This is an expected grain difference, not inflation. The developer's
characterization of these as "not the authoritative correctness layer" is correct — they should
not be used for session-level SLA or reliability calculations.

---

## Raw Events — Clean

```
raw_events:          18,301,702 rows — 0 duplicate event_ids
accepted_raw_events: 18,301,774 rows — 0 duplicate event_ids
```

No replayed duplicate raw events present at time of assessment.

---

## Developer Research Validation

| Inflation Mode Identified | Live Data Status |
|---|---|
| Replayed duplicate raw events | Not currently present (0 duplicates) |
| Duplicate logical status versions | Pre-dedup store has 139k version rows, correctly resolved by View |
| Unsafe re-rollups | **CONFIRMED** — caused 93% corruption on 2026-03-23 backfill |
| Unsafe join grain | **CONFIRMED** — double-counting in `output_viability_rate` formula |
| Tail-noise leakage | `is_terminal_tail_artifact` flag present in canonical layer; not directly tested |
| FINAL omission (crash/restart) | Risk confirmed; currently latent |

---

## Actions Required

| Priority | File | Action |
|---|---|---|
| **P1** | `warehouse/models/api/api_sla_compliance_by_org.sql` | Cap `health_signal_coverage_ratio` at 1.0 |
| **P1** | `warehouse/models/api/api_sla_compliance.sql` | Same cap + fix `output_viability_rate` OR logic |
| **Tech debt** | `api/internal/repo/clickhouse/performance.go` | Add `FINAL` to normalized table reads |
| **Tech debt** | `api/internal/repo/clickhouse/reliability.go` | Add `FINAL` to normalized table reads |
| **Test** | `api/internal/validation/aggregate_test.go` | Add test for coverage ratio cap (≤ 1.0) |

No data backfill is required. Once the SQL models are corrected and refreshed, the 345 impossible
SLA rows (3.03% of `api_sla_compliance_by_org`) will be resolved on the next model run.
