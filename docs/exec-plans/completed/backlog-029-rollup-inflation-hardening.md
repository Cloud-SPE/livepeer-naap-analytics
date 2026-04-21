# Plan: BACKLOG-029 — Gold Rollup Inflation Hardening

**Status:** completed
**Started:** 2026-04-08
**Completed:** 2026-04-09
**Depends on:** Phase 11 canonical analytics model
**Blocks:** none

## Outcome

Closed. The rollup inflation and bounds hardening described here is now treated
as shipped in the current repo state. The active backlog excludes `BACKLOG-029`,
and ongoing follow-up work should be tracked as new backlog items or technical
debt rather than leaving this plan in the active queue.

## Goal

Remove the current inflation paths in gold serving data and add regression
coverage that fails whenever rollup algebra, overlap semantics, or bounds
handling drift.

## Locked constraints

- Do not add production ClickHouse migration or repair code.
- Do not add production Kafka replay or offset-reset logic.
- Production correction happens via a fresh clean run against production Kafka
  outside this code change.
- Public API changes are additive only:
  existing fields stay in place and keep the same wire types, while new support
  fields are added alongside corrected derived values.
- Dashboard and internal query surfaces must migrate to the corrected semantics
  when they aggregate public rows.

## Findings

### 1. Still-live SLA formula bug

`output_viability_rate` is still computed with additive counting of
`loading_only_sessions + zero_output_fps_sessions` in both the resolver-fed
store and the public re-rollup view.

Impact:

- one session that satisfies both conditions is double-counted
- `output_viability_rate` can go negative
- `sla_score` can go negative

Robust fix:

- add an additive `output_failed_sessions` support field to
  `canonical_streaming_sla_input_hourly_store`
- compute that field with union semantics:
  `loading_only_session = 1 OR zero_output_fps_session = 1`
- compute `output_viability_rate` only from `output_failed_sessions`
- do not try to reconstruct union failure counts from separate loading/zero
  counters at higher grains

### 2. Uncapped health coverage at serving layer

Current session-building logic emits at most `3/3` health coverage per session,
but the store and public view do not defend against impossible
`health_signal_count > health_expected_signal_count` inputs.

Impact:

- replay/backfill corruption can still push `health_signal_coverage_ratio > 1`
- `sla_score` can exceed `100`
- downstream consumers see impossible gold-layer values instead of a bounded
  defensive output

Robust fix:

- clamp the ratio in the resolver-fed store and the public view with:
  `least(sum(health_signal_count) / nullif(sum(health_expected_signal_count), 0), 1.0)`
- keep the raw additive support fields so the corruption remains inspectable

### 3. Org-aware GPU metrics already uses aggregate-of-aggregate math

`insertGPUMetricsRollups` averages `canonical_status_hours.avg_output_fps`,
`canonical_status_hours.avg_e2e_latency_ms`, and related per-session-hour
derived values instead of recomputing from additive support evidence. It also
computes p95 from already-aggregated FPS values.

Impact:

- `/v1/gpu/metrics?org=...` can overstate or understate FPS and latency
- the public GPU metrics rollup compounds the same error

Robust fix:

- compute org-aware GPU metrics from sample-level or additive support evidence
- persist the sums, counts, and mergeable quantile states needed for safe
  downstream re-rollups

### 4. Public demand and GPU-demand re-rollups average org-level averages

`api_network_demand.sql` and `api_gpu_network_demand.sql` use
`avg(s.avg_output_fps)` across org-aware rows.

Impact:

- a small high-FPS org can inflate a much larger low-FPS cohort
- the public `avg_output_fps` is not derivable from additive support fields

Robust fix:

- add `status_samples` and `output_fps_sum` to the org-aware demand stores
- compute public `avg_output_fps` as
  `sum(output_fps_sum) / sum(status_samples)`

### 5. Public GPU metrics repeats the same invalid re-rollup

`api_gpu_metrics.sql` averages scalar `avg_*` values, averages
`health_signal_coverage_ratio`, and computes p95 from scalar `avg_output_fps`.

Impact:

- public GPU metrics is mathematically invalid across orgs even if the
  org-aware store is corrected
- percentiles and ratios can drift far from lower-grain truth

Robust fix:

- public GPU metrics must merge support/state columns
- never average scalar averages, scalar ratios, or scalar percentiles

### 6. Not confirmed at HEAD

I did not confirm the older `FINAL` omission risk from `performance.go` and
`reliability.go` against this branch. Current code reads `api_*` relations, not
raw normalized tables. Treat that older note as production-branch-specific
until proven on HEAD.

## Test Plan

1. Validation test: overlapping no-output conditions

- Raw scenario: one requested session with only `LOADING` zero-FPS status
- Assert:
  - `loading_only_sessions = 1`
  - `zero_output_fps_sessions = 1`
  - `output_failed_sessions = 1`
  - `output_viability_rate = 0`
  - `sla_score` is never negative
- File: `api/internal/validation/aggregate_test.go`

2. Validation test: defensive coverage clamp

- Insert a synthetic `canonical_streaming_sla_input_hourly_store` row with impossible
  `health_signal_count > health_expected_signal_count`
- Assert `api_hourly_streaming_sla` caps
  coverage to `1.0` and keep `sla_score <= 100`
- This is intentionally a store-level invariant test because the goal is to
  defend downstream consumers even when upstream backfill/replay logic is wrong

3. Validation test: org-aware GPU metrics recomputes from additive evidence

- Raw scenario: same org, same window, same GPU slice, two sessions with highly
  imbalanced status-sample counts and FPS values
- Assert `api_hourly_streaming_gpu_metrics.avg_output_fps` equals direct recomputation
  from lower-grain evidence, not an average of session-hour averages
- Extend the same pattern to latency fields when the synthetic inputs are easy
  to express

4. Validation test: public demand and GPU metrics reweights lower-grain evidence

- Raw scenario: two synthetic orgs share the same public grain but have
  materially different support counts
- Assert:
  - `api_hourly_streaming_demand.avg_output_fps`
  - `api_hourly_streaming_gpu_metrics.avg_output_fps`
  match independent recomputation from lower-grain additive evidence

5. Static CI guard

- Extend `api/internal/validation/contract_static_test.go` with a narrow
  forbidden-pattern check for aggregate-of-aggregate rollups in resolver SQL
  and `warehouse/models/api/`
- Initial forbidden shapes:
  - `avg(s.avg_`
  - `avg(b.avg_`
  - `quantile(...)(s.avg_`
  - `quantile(...)(b.avg_`
  - `avg(health_signal_coverage_ratio)` in re-rollup code
- Keep the guard scoped to serving re-rollups so it stays precise

## Fix Plan

### Phase 1: Add Safe Support Columns

`canonical_streaming_sla_input_hourly_store`

- add `output_failed_sessions`

`canonical_streaming_demand_hourly_store`

- add `status_samples`
- add `output_fps_sum`

retired GPU-demand compatibility store

- add `status_samples`
- add `output_fps_sum`

`canonical_streaming_gpu_metrics_hourly_store`

- add `output_fps_sum`
- add `health_signal_count`
- add `health_expected_signal_count`
- add latency sum/count fields needed for averages
- add mergeable quantile state columns needed for public p95 rollups

Reason:

- every public re-rollup must be derivable from additive or mergeable support
  fields, never scalar summaries

### Phase 2: Correct Rollup Production

Resolver `insertSLAComplianceRollups`

- compute `output_failed_sessions` with `OR`
- compute `output_viability_rate` from `output_failed_sessions`
- clamp `health_signal_coverage_ratio`
- keep additive support fields intact for inspection

Resolver `insertGPUMetricsRollups`

- stop averaging `canonical_status_hours.avg_*`
- compute from sample-level or support-field evidence
- persist the support/state columns needed for later re-rollups

dbt public models

- `api_sla_compliance.sql`
  - use `sum(output_failed_sessions)`
  - use capped coverage
- `api_network_demand.sql`
  - use `sum(output_fps_sum) / sum(status_samples)`
- `api_gpu_network_demand.sql`
  - same
- `api_gpu_metrics.sql`
  - merge support/state columns
  - never average scalar percentiles or scalar ratios

### Phase 3: Harden Contract And Docs

Update `docs/design-docs/data-validation-rules.md`

- `output_viability_rate` must use union semantics
- rollup algebra examples must show support-field recomputation

Update `docs/metrics-and-sla-reference.md`

- the failure numerator is the union of no-output failure modes
- not additive double-counting

Update validation coverage so these bounds are explicit:

- `0 <= health_signal_coverage_ratio <= 1`
- `0 <= output_viability_rate <= 1`
- `0 <= sla_score <= 100`

## Acceptance Gates

- Validation reproduces the pre-fix bugs and passes only after the fixes
- Public and org-aware rollups match independent recomputation from canonical
  low-level evidence
- No serving model or resolver rollup averages precomputed ratios, averages, or
  percentiles directly
- Impossible bounds breaches become unrepresentable or immediately detected in
  CI
