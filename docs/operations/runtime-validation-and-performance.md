# Runtime Validation & Performance

| Field | Value |
|---|---|
| **Status** | Active |
| **Effective date** | 2026-04-09 |
| **Audience** | Operators, developers, release reviewers |

Related docs:
- [`devops-environment-guide.md`](devops-environment-guide.md) — dashboards, local stack setup, smoke commands
- [`run-modes-and-recovery.md`](run-modes-and-recovery.md) — resolver modes and rebuild workflows
- [`operations-runbook.md`](operations-runbook.md) — deployment, troubleshooting, incident response entry point
- [`../metrics-and-sla-reference.md`](../metrics-and-sla-reference.md) — metric meanings and SLA formulas
- [`../design-docs/data-validation-rules.md`](../design-docs/data-validation-rules.md) — correctness contract for rollups and serving outputs

---

## 1. Purpose

This document defines the standard measurement set for evaluating:

- backlog catch-up vs steady state
- resolver health
- API responsiveness
- attribution quality
- SLA correctness
- rollup inflation safety

Use it after:

- a fresh backlog run
- an API or resolver redeploy
- a warehouse publication change
- a correctness or performance investigation

This is the operational checklist. It does not redefine metric formulas.

---

## 2. Standard Measurement Set

### Ingest and catch-up

Use these to decide whether the system is still catching up or is effectively
steady state.

- latest watermark lag in seconds
- recent `resolver_runs` by mode, duration, and rows processed
- active resolver claims
- accepted raw event growth
- ClickHouse/Kafka consumer health

Primary sources:

- `resolver_runs`
- `resolver_window_claims`
- `accepted_raw_events`
- `system.kafka_consumers`

### Component performance

Use these to judge runtime cost and user-visible responsiveness.

- resolver run duration and rows processed
- ClickHouse CPU and memory
- representative API latency for:
  - `/healthz`
  - `/v1/dashboard/kpi`
  - `/v1/dashboard/orchestrators`
  - `/v1/sla/compliance`
  - `/v1/jobs/demand`
  - `/v1/jobs/sla`
  - `/v1/network/demand`
  - `/v1/gpu/network-demand`
  - `/v1/gpu/metrics`

Primary tools:

- `docker stats`
- `curl -w`
- `scripts/measure_refactor_replay.py`

### Attribution quality

Measure attribution on the correct denominator.

- session counts by `selection_outcome`, broken out by org
- attribution subtype breakdown within `selection_outcome = 'selected'`
- primary success metric: `resolved + hardware_less` on selected sessions
- request/response job counts by `selection_outcome`
- request/response attribution subtype breakdown within `selection_outcome = 'selected'`
- request/response worked attribution: `resolved + hardware_less + stale` on selected jobs
- supporting subtype counts:
  - `resolved`
  - `hardware_less`
  - `stale`
  - `ambiguous`
  - `unresolved`

Do not judge attribution quality from all sessions combined. Sessions with
`selection_outcome = 'no_orch'` are not expected to resolve to an orchestrator.
Do not judge request/response attribution quality from all jobs combined either.
Jobs with `selection_outcome = 'no_orch'` or `selection_outcome = 'unknown'`
must stay visible, but they are not part of the selected-job denominator.

Operational interpretation:

- `resolved + hardware_less` means orchestrator identity and pipeline/model
  canonicalization succeeded
- `hardware_less` is still attribution success for orchestration; it only means
  hardware metadata was missing
- `stale`, `ambiguous`, and `unresolved` are the remaining failure-safe classes
- for healthy recent windows, the combined `resolved + hardware_less` rate on
  selected sessions should usually be above `80%`

Primary sources:

- `canonical_session_current`
- `canonical_ai_batch_jobs`
- `canonical_byoc_jobs`
- `scripts/measure_job_baseline.py`

For SLA-specific inspection, include representative low / medium / high rows for
the largest recent cohorts so operators can verify that `sla_score` still
correlates with the exposed additive inputs and component scores.

### SLA correctness

Use these to confirm the contracted SLA behavior is live and bounded.

- `sla_semantics_version`
- `gpu_model_name` visibility on SLA rows when hardware is known
- `sla_score` bounds
- `health_signal_coverage_ratio` bounds
- `output_viability_rate` bounds
- cold SLA request latency against the precomputed final SLA rows

Primary sources:

- `api_sla_compliance`
- `api_sla_compliance_by_org`
- live API responses

### Rollup inflation safety

Use these to confirm serving outputs are not inflating metrics via unsafe
aggregate-of-aggregate math.

- weighted recomputation drift for:
  - `api_network_demand`
  - `api_gpu_network_demand`
  - `api_gpu_metrics`
  - `api_sla_compliance`
- latency support-field recomputation drift
- overlap check for:
  - `loading_only_sessions`
  - `zero_output_fps_sessions`
  - `output_failed_sessions`
- canonical job duplicate sentinels:
  - `count() - uniq(request_id)` on `canonical_ai_batch_jobs`
  - `count() - uniq(request_id)` on `canonical_byoc_jobs`
- source-versus-canonical coverage for AI-batch and BYOC:
  - normalized completed events
  - canonical job rows

Primary sources:

- public `api_*` views
- additive support fields exposed on those views
- `canonical_ai_batch_jobs`
- `canonical_byoc_jobs`

---

## 3. Standard Commands

### Quick runtime checks

```bash
make ch-smoke
make resolver-logs
docker stats --no-stream
curl -sS -o /dev/null -w 'health total=%{time_total} code=%{http_code}\n' http://localhost:8000/healthz
curl -sS -o /dev/null -w 'kpi total=%{time_total} code=%{http_code}\n' http://localhost:8000/v1/dashboard/kpi
curl -sS -o /dev/null -w 'sla total=%{time_total} code=%{http_code}\n' 'http://localhost:8000/v1/sla/compliance?limit=10'
```

### Standard script-based measurements

```bash
python3 scripts/measure_refactor_replay.py
python3 scripts/measure_job_baseline.py
```

Both scripts read repository defaults from `.env` where available.
Both scripts write local-only artifacts under `.local/baselines/` by default.

`measure_job_baseline.py` is the richer runtime baseline:

- it probes the major public API families, not just health and SLA
- it includes a broader attribution lookback by default, not just a narrow live slice
- it includes selected-session attribution breakdowns
- it includes sample low / medium / high SLA cohort rows with their additive
  support values
- it should be paired with duplicate and coverage checks for non-streaming job
  stores after resolver or warehouse changes

Keep `make ch-smoke` lightweight. Do not move the richer cohort/API baseline
checks into the smoke target.

## 4. Release And Runtime Gates

### A. Catch-up is effectively complete

Good signs:

- watermark lag is measured in seconds, not sustained minutes or hours
- `active_claims = 0` or draining toward `0`
- recent resolver runs are mostly `tail`
- accepted raw event timestamps and ingest timestamps stay close together

### B. Resolver is healthy

Good signs:

- recent `resolver_runs.status = success`
- `mismatch_count = 0`
- `error_summary` is null
- rows continue to process in `auto` or `tail` mode

### C. API is healthy

Required:

- `/healthz` returns `200`
- `/v1/dashboard/kpi` returns `200`
- `/v1/sla/compliance` returns `200`

Deployment verification should also confirm:

- served OpenAPI reflects the intended endpoint behavior
- the response shape matches the current contract

### D. Attribution quality is acceptable

Judge this only on `selection_outcome = 'selected'`.

Required:

- attribution is measured by org, not only in aggregate
- the primary operational metric is combined `resolved + hardware_less`
- `hardware_less` is interpreted as successful orchestration canonicalization
  with missing hardware metadata, not as unresolved attribution
- `stale`, `ambiguous`, and `unresolved` remain visible as separate buckets
- no invented orchestrator attribution appears in unresolved/no-orch cases

### E. SLA behavior is correct

Required:

- `0 <= sla_score <= 100`
- `0 <= output_viability_rate <= 1`
- `0 <= health_signal_coverage_ratio <= 1`
- `sla_semantics_version` is the expected active contract
- cold `/v1/sla/compliance` timing reflects a thin final-store-backed read, not
  a live benchmark-history recomputation

### F. Rollup inflation is not present

Required:

- zero weighted recomputation drift for the checked public rollups
- no negative viability caused by overlapping failure classes
- overlap rows satisfy:
  - `output_failed_sessions <= loading_only_sessions + zero_output_fps_sessions`
  - union semantics are preserved

---

## 5. Measurement Interpretation Notes

- A fast `/healthz` does not prove serving queries are healthy.
- A healthy overall attribution rate can be misleading if `no_orch` sessions are
  included in the denominator.
- SLA correctness requires both formula checks and bound checks.
- Rollup safety is proven by recomputation from additive support fields, not by
  trusting a derived metric directly.
- SLA should now be assessed as a cold read from precomputed final serving rows;
  warm-request cache behavior is no longer part of the acceptance criteria.

---

## 6. Source Of Truth Split

This document is the operational measurement checklist.

Keep these concerns separate:

- metric meaning and formulas:
  [`../metrics-and-sla-reference.md`](../metrics-and-sla-reference.md)
- correctness contract:
  [`../design-docs/data-validation-rules.md`](../design-docs/data-validation-rules.md)
- SLA rationale and evolution boundaries:
  [`../design-docs/adr-004-quality-aware-sla.md`](../design-docs/adr-004-quality-aware-sla.md)
- script implementation details:
  [`../../scripts/measure_job_baseline.py`](../../scripts/measure_job_baseline.py),
  [`../../scripts/measure_refactor_replay.py`](../../scripts/measure_refactor_replay.py)
