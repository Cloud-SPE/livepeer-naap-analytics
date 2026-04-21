# ADR-004: Quality-Aware SLA

## Status

Current

## Summary

`sla_score` is intentionally a composite reliability and quality score, not a
pure uptime score.

The contracted field remains `sla_score`; there is no parallel legacy score or
versioned `sla_score_v2` surface.

The active formulation is:

```text
sla_score = 100
          * health_signal_coverage_ratio
          * ((0.7 * reliability_score) + (0.3 * quality_score))

reliability_score = (0.4 * startup_success_rate)
                  + (0.2 * no_swap_rate)
                  + (0.4 * output_viability_rate)

latency_score = (0.6 * ptff_score) + (0.4 * e2e_score)
quality_score = (0.6 * latency_score) + (0.4 * fps_score)
```

All component scores are bounded to `[0,1]`. `sla_score` is clamped to
`[0,100]`.

## Why this exists

The original reliability-only SLA was too narrow for downstream consumers.

- A node could be "reliable" while still being materially slow.
- Raw FPS and latency should influence the contracted score because they are
  part of the user-visible service experience.
- A global hard threshold for FPS or latency is not defensible across
  pipelines, models, and GPUs.

The quality-aware SLA solves that by keeping reliability dominant while still
penalizing poor user-visible quality inside a comparable cohort.

## Design choices

### Reliability stays dominant

Reliability remains `70%` of the final score.

This keeps the score anchored to production-grade service behavior:

- startup success
- swap avoidance
- viable output production

Quality contributes `30%`, which is large enough to matter but not large enough
to let a fast unreliable node outrank a slower reliable one.

### Quality is benchmark-relative, not threshold-based

Quality components are scored against broad network-wide cohort benchmarks,
rather than against one global latency or FPS target.

This avoids mathematically neat but operationally wrong comparisons such as:

- `streamdiffusion` vs `streamdiffusion-sdxl-v2v`
- different models with materially different baseline PTFF or FPS

The active cohort key is:

1. `(pipeline_id, model_id)`

The active benchmark window is:

1. the prior `7` full UTC days, network-wide across orgs

Hardware is intentionally not normalized out of the score. A node with
stronger hardware should still be able to outperform the broader `pipeline +
model` benchmark. The API exposes `gpu_model_name` for drilldown so consumers
can inspect the hardware context directly instead of having the score silently
normalize that distinction away.

If the prior 7 full days do not contain enough benchmark rows, quality defaults
to neutral `0.5`.

### PTFF is weighted above E2E

Latency quality uses:

- `60%` prompt-to-first-frame
- `40%` end-to-end latency

PTFF is the preferred user-facing latency signal because it best represents
when the stream first becomes visibly useful. E2E is retained because it still
captures full-path degradation that PTFF alone can miss.

### FPS is quality, not throughput-only telemetry

`avg_output_fps` is part of the contracted SLA because low or unstable output
throughput is a real user-facing degradation, not merely a diagnostic metric.

It is scored against the cohort benchmark rather than via an absolute target
for the same reason as latency.

### Coverage still gates confidence

`health_signal_coverage_ratio` remains a multiplier on the final score.

This is intentional. A high-quality score with sparse or broken telemetry should
not be treated as equally trustworthy to a fully observed window.

The coverage ratio is capped to `[0,1]` so impossible upstream inputs cannot
inflate `sla_score`.

## Rollup and implementation rules

The quality-aware SLA must follow the repository rollup-safety rule.

- Do not compute public `sla_score` by aggregating lower-grain `sla_score`
  values.
- Do not average `avg_output_fps`, `avg_prompt_to_first_frame_ms`, or
  `avg_e2e_latency_ms` across rows.
- Recompute current-window inputs from additive support fields.
- Build benchmark state from additive org-hour SLA inputs, never from scored
  SLA rows.
- Compare current rows to merged benchmark state from the prior 7 full UTC
  days, not to a hot-path history self-join.
- Publish final SLA serving rows from resolver-owned additive input slices so
  the API reads thin final-store-backed `api_*` relations rather than scoring
  the benchmark join on every request.

Required support fields include:

- `output_failed_sessions`
- `output_fps_sum`
- `status_samples`
- `prompt_to_first_frame_sum_ms`
- `prompt_to_first_frame_sample_count`
- `e2e_latency_sum_ms`
- `e2e_latency_sample_count`

## Evolution rules

Future changes to `sla_score` require explicit signoff if they change any of:

- top-level reliability vs quality weighting
- PTFF vs E2E weighting
- cohort key or benchmark window
- sparse-history neutralization behavior
- support-field contract needed for safe recomputation
- the meaning of the public `sla_score` field

Changes that only improve performance or implementation structure without
changing the metric meaning do not require a new ADR.

The current implementation change from live view scoring to resolver-published
final SLA rows is one of those structural changes: the formula is unchanged,
but the serving ownership is intentionally different so cold API reads stay
cheap without introducing a second freshness owner.

## Source of truth

This ADR captures rationale and evolution boundaries.

The operational formula and field contract remain defined in:

- [../metrics-and-sla-reference.md](../metrics-and-sla-reference.md)
- [data-validation-rules.md](data-validation-rules.md)
- [../design.md](../design.md)
