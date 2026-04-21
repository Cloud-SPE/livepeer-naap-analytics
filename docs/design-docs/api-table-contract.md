# API Table Contract — Three-Families Serving Layer

**Status:** current
**Date:** 2026-04-17 (verified 2026-04-20)
**Related:** [`../exec-plans/completed/serving-layer-v2.md`](../exec-plans/completed/serving-layer-v2.md), [ADR-003](adr-003-tiered-serving-contract.md) (amended)

## Summary

Every object in the `api_*` layer belongs to exactly one of three physical families. User-facing endpoints and Grafana panels must anchor on published `api_*` contracts rather than raw, normalized, or canonical tables. Some routes combine a current surface with an hourly surface, but the read boundary remains `api_*`.

| Family | Shape | Purpose |
|---|---|---|
| `api_hourly_*` | wide hourly rollup, append-only with `refresh_run_id` slicing | time-series analytics |
| `api_current_*` | latest-per-entity snapshot over a MergeTree store | entity lookups and live state |
| `api_fact_*` | append-only event log | paginated per-event lists |

## Required columns (every `api_*_store` table)

| Column | Type | Source | Purpose |
|---|---|---|---|
| `refresh_run_id` | `String` | resolver `RunRequest.RunID` | slice identifier for atomic swaps |
| `refreshed_at` | `DateTime64(3, 'UTC')` | resolver write time (deterministic from `RunRequest.now`, never `now64()` inline) | tiebreaker for `argMax` |
| `artifact_checksum` | `String` | `sipHash128` over the row payload | replay-determinism assertion |

Entity-key columns are family-specific (below).

## Family 1: `api_hourly_*`

- **Engine:** `MergeTree`
- **Partition by:** `toYYYYMM(window_start)`
- **Sort key:** `(window_start, dim1, dim2, …, refresh_run_id)` — always `window_start` first
- **Primary key:** the sort-key prefix up to but not including `refresh_run_id`
- **Required columns:** `window_start DateTime('UTC')`, plus the dimension columns, plus the metric columns, plus the three slice columns above
- **Serving pattern:** reader joins an `argMax(refresh_run_id, refreshed_at) GROUP BY window_start` CTE to pick the latest slice
- **Alias view:** `api_hourly_<name>` is a `{{ config(materialized='view') }}` that does the latest-slice selection and projects only public columns (no `refresh_run_id`, `refreshed_at`, `artifact_checksum`)

Tables in this family (post-Phase-8):

- `api_hourly_streaming_sla`
- `api_hourly_streaming_demand`
- `api_hourly_streaming_gpu_metrics`
- `api_hourly_request_demand`
- `api_hourly_byoc_auth`
- `api_hourly_byoc_payments`

## Family 2: `api_current_*`

- **Engine:** `MergeTree`
- **Partition by:** none (or a coarse partition like `org` if cardinality is extreme)
- **Sort key:** `(entity_key)` or `(org, entity_key)` — never starts with a time column
- **Entity key rules:**
  - One row per entity, period. No duplication across dimensions.
  - Latest-slice selection is keyed by `(refreshed_at, refresh_run_id)` in the alias view.
- **Serving pattern:** reader uses an `argMax(refresh_run_id, (refreshed_at, refresh_run_id))` latest-slice CTE over the entity key.
- **Alias view:** `api_current_<name>` is a view that joins the store against that latest-slice selection and projects public columns.

Tables in this family:

- `api_current_capability` — one row per `(org, orch_address, capability_id, canonical_pipeline, model_id, gpu_id)`
- `api_current_active_stream_state` — one row per `(org, canonical_session_key)`; only uncompleted rows
- `api_current_orchestrator` — one org-agnostic row per `orch_address` denormalizing identity + latest reliability + capability counts
- `api_current_gpu_inventory` — one row per `(org, orch_address, gpu_id)` for hot dashboard inventory reads

`api_current_orchestrator` is intentionally org-agnostic because it is an
entity summary used by orchestrator-listing endpoints. Repeating the same
orchestrator once per observing org would make list endpoints and global
counts easy to overcount. Org-scoped provenance stays on observed and
activity surfaces such as `api_observed_orchestrator`,
`api_observed_capability_offer`, `api_current_gpu_inventory`, and hourly
SLA/request tables. If an API needs current org provenance at orchestrator
grain, add an explicit org-observation surface or provenance columns rather
than changing the entity-summary grain implicitly.

## Family 3: `api_fact_*`

- **Engine:** `MergeTree`
- **Partition by:** `toYYYYMM(window_start)` (or `toYYYYMM(completed_at)` for retrospective fact tables)
- **Sort key:** `(window_start, entity_key)` — `window_start` always first, for time-range pagination
- **Retention:** handled by table TTL in the dbt `post_hook`
- **Serving pattern:** reader uses a keyset cursor on `(window_start, entity_key)` via the shared `api/internal/runtime/cursor.go` helper

Tables in this family:

- `api_fact_ai_batch_job`
- `api_fact_ai_batch_llm_request`
- `api_fact_byoc_job`

## Denormalization rules

Two pieces of entity data are denormalized into every `api_*` row that references them:

1. **Orchestrator identity** — service-facing URI fields are written directly into the `api_hourly_*` and `api_current_*` rows that need them. API handlers do not `LEFT JOIN api_orchestrator_identity` to serve request paths. Grafana panels may still use `api_orchestrator_identity` for display labels when the backing fact table only carries an address.
2. **Capability family flag** — `capability_family ∈ {builtin, byoc}` is carried on every row that touches capabilities. Handlers never branch on `if capability_family = 'byoc' then ...` at read time; they filter.

Other dimensions (pipeline, model, GPU) are already naturally present as columns and stay as dimension keys.

## Additivity rule

Every `api_hourly_*` table exposes the **additive primitives** alongside any derived metric. Consumers re-aggregate from primitives over wider windows; they never average ratios or average averages.

Required shape for any derived metric at the API layer:

| Derived metric | Required primitive columns |
|---|---|
| any ratio `X / Y` | `X_numerator` (or the named count) and `Y_denominator` (or the named count), both summable |
| any average `sum(x) / count` | `x_sum` and `x_sample_count`, both summable |
| any percentile | an AggregatingMergeTree `*_state` column plus the pre-merged scalar |
| any score / index | the primitives the score is defined from, plus the scored value |

Example — `api_hourly_streaming_sla` keeps `sla_score` **and** keeps `requested_sessions`, `startup_success_sessions`, `total_swapped_sessions`, `loading_only_sessions`, `zero_output_fps_sessions`, `health_signal_count`, `health_expected_signal_count`. A downstream weekly rollup reconstructs the weekly SLA by summing the primitives and re-applying the scoring formula — it does not average the hourly `sla_score` values.

This rule is lifted from ADR-003's warehouse rollup rule into the API contract so every downstream consumer (API client, Grafana panel, future analytical pipeline) inherits the safety property.

Enforced by dbt test `assert_additive_primitives_present` — the required primitive list is declared in `warehouse/tests/test_api_hourly_additive_primitives.sql`, and the test verifies those columns are present and non-nullable.

## Core-logic ownership rule

**Definitional logic** is written exactly once, in the resolver. The API layer never redefines it.

| Allowed at the API layer | Forbidden at the API layer |
|---|---|
| `SUM`, `AVG`, `COUNT`, `MIN`, `MAX` over additive primitives | scoring formulas (SLA, reliability, quality, latency) |
| ratio recomputation from primitives | attribution status assignment |
| simple arithmetic (addition, subtraction, clamps, `nullIf`) | excusal reason classification |
| time-window filtering and grouping | selection-outcome mapping |
| keyset cursor math | success-criteria definitions |
| `capability_family` filtering | `capability_family` derivation |
| null-coalescing of display values | SLA semantics version |

The test for "is this a recalc": if the logic would produce a different result after the resolver's semantics change in a future version, it is definitional and must live in the resolver.

Simple aggregation and ratio recomputation are not recalcs — they are the point of the additivity rule. Case-by-case judgement applies only at the margin (e.g. computing a dashboard-side "delta over previous window" from two primitive sums is allowed; recomputing the SLA score from constituent component scores is not).

Enforced by code review plus a `grep`-based CI check that fails if any SQL in `api/` or `warehouse/models/api/` contains the formula signatures of known definitional logic (`0.4 *`, `0.6 *`, `0.7 *`, `multiIf(…health…)`, etc. — listed in `scripts/core-logic-signatures.txt`).

## Endpoint → table mapping

This mapping is canonical. Adding an endpoint means adding or extending a table in this list; it does not mean composing existing tables at read time.

| Endpoint | Backing table |
|---|---|
| `/v1/dashboard/kpi` | `api_hourly_streaming_demand`, `api_observed_orchestrator`, `api_observed_capability_offer` |
| `/v1/dashboard/pipelines` | `api_hourly_streaming_demand` |
| `/v1/dashboard/orchestrators` | `api_current_orchestrator` |
| `/v1/dashboard/gpu-capacity` | `api_current_capability`, `api_current_active_stream_state` |
| `/v1/dashboard/pipeline-catalog` | `api_current_capability` |
| `/v1/dashboard/pricing` | `api_current_capability` |
| `/v1/dashboard/job-feed` | `api_current_active_stream_state` |
| `/v1/streaming/models` | `api_current_capability` |
| `/v1/streaming/orchestrators` | `api_current_orchestrator` |
| `/v1/streaming/sla` | `api_hourly_streaming_sla` |
| `/v1/streaming/demand` | `api_hourly_streaming_demand` |
| `/v1/streaming/gpu-metrics` | `api_hourly_streaming_gpu_metrics` |
| `/v1/requests/models` | `api_current_capability` |
| `/v1/requests/orchestrators` | `api_current_orchestrator` |
| `/v1/requests/ai-batch/summary` | `api_hourly_request_demand` |
| `/v1/requests/ai-batch/jobs` | `api_fact_ai_batch_job` |
| `/v1/requests/ai-batch/llm-summary` | `api_hourly_request_demand` |
| `/v1/requests/byoc/summary` | `api_hourly_request_demand` |
| `/v1/requests/byoc/jobs` | `api_fact_byoc_job` |
| `/v1/requests/byoc/workers` | `api_current_capability` (filter `capability_family = 'byoc'`) |
| `/v1/requests/byoc/auth` | `api_hourly_byoc_auth` |
| `/v1/discover/orchestrators` | `api_observed_capability_offer`, `api_observed_byoc_worker`, `api_hourly_streaming_sla`, `api_hourly_request_demand` |

## Enforcement

Five machine-checked rules:

1. **Layer discipline** — dbt test `assert_layer_discipline` walks the `{{ ref() }}` graph and fails if:
   - any `api_*` model references `stg_*` or `normalized_*`
   - any `canonical_*` model references `api_*`
   - any `stg_*` model references anything other than `raw_events`

2. **Grafana contract** — `scripts/grafana-lint.go` parses each panel JSON and fails if:
   - the SQL references any table outside the `api_*` family
   - the panel is missing `"meta": { "backing_table": "naap.api_…" }`
   - the `backing_table` does not exist in the warehouse

3. **Replay determinism** — the layer-by-layer replay harness in `tools/replay/` drives raw events through every boundary (raw → normalized → canonical → api) and asserts byte-identical `artifact_checksum` across two runs at each boundary. Per-layer granularity means divergence is localized to the layer that introduced it.

4. **Additive primitives present** — dbt test `assert_additive_primitives_present` checks the required primitive list declared in `warehouse/tests/test_api_hourly_additive_primitives.sql` and verifies every listed column is non-nullable and present.

5. **No core-logic recalc** — CI grep check against `scripts/core-logic-signatures.txt` fails if any SQL in `api/internal/service/` or `warehouse/models/api/` contains a formula signature matching known definitional logic (scoring coefficients, classification branches, attribution predicates).

## Deliberate non-uses

- **No `api_base_*` tier.** Intermediate computation lives in the resolver (stateful) or in MV definitions (stateless) — never in a view chain.
- **No `SELECT *` into views over billion-row fact tables.** Every alias view names the exact public columns.
- **No `now()` / `today()` / `rand()` inside any `stg_*`, `normalized_*`, `canonical_*`, or `api_*` transformation.** The resolver parameterizes time via `RunRequest.now`.
- **No cross-layer skips.** Always `stg → normalized → canonical → api`. Adding a new topic means extending every layer.

## Open questions

- Should `/v1/discover/orchestrators` eventually collapse onto `api_current_orchestrator` plus `api_current_capability` so discovery no longer rescans observed inventory on the read path?
- Does `api_current_active_stream_state` need a separate `api_hourly_active_stream_state` for retention, or is 30-min TTL sufficient? Decide based on the live-operations dashboard requirements.
