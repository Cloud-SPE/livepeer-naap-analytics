# Plan: Serving Layer v2 — Three-Families Medallion Rewrite

**Status:** Phase 0 complete; Phase 1 next.
**Started:** 2026-04-17
**Depends on:** Phase 11 (minimal canonical analytics model)
**Blocks:** retirement of `api_base_*` tier, resolver store-table consolidation, Grafana serving-contract enforcement
**Supersedes (partial):** ADR-003 — removes the `api_base_*` tier; collapses serving into three `api_*` families

## Phase 0 shipped commits

| PR | Commit | Lands |
|---|---|---|
| PR 1 | `c45967b` | Replay harness scaffold + golden fixture (raw, normalized). |
| PR 2 | `4e01d63` | Resolver clock freeze — `RunRequest.Now` threaded through every persistent write. |
| PR 3 | `e97da55` | Canonical-phase extension of the harness. |
| PR 4 | `86724c3` | API-phase extension via dbt. |
| PR 5 | `e902b75` | Medallion lints (layer discipline, grafana, additive primitives, core-logic recalc). |
| PR 6 | `729094a` | Daily dev fixture (744k rows, 6-min four-layer replay). |
| PR 7 | `5467d6d` | CI activation — lint workflow, replay gate, S3 fetch path. |
| PR 8 | *this* | Store-DDL ownership under `warehouse/ddl/stores/` + drift lint. |

Phase 0 deliberately narrowed Phase 8's original "every `_store` moves
to `materialized='table'`" into "DDL declaration moves to
`warehouse/ddl/stores/` with a drift lint." Full dbt materialization
ownership would require a custom non-dropping materialization and
wholesale ALTER migration for 15 tables — a larger scope than Phase 0
closeout should carry. The current shape gives the plan the property
that matters (**`warehouse/` is the source of truth for every table
shape, drift is caught in CI**) without the data-loss risk.

## Goal

Collapse the serving layer into **three physical `api_*` table families**, each endpoint and each Grafana panel mapping to exactly one table. Make the resolver the sole author of canonical and — where stateful — of `api_*` stores. Eliminate view chains, runtime JOINs of large tables, and re-computation of already-materialized rollups.

Ownership boundary:

- ClickHouse Kafka engine: Kafka → `raw_events`
- Materialized views: `raw_events` → `normalized_*` (one MV per topic; no logic beyond dedup and shape)
- Resolver (Go): canonical joins + stateful rollups → `canonical_*` and `api_*_store`
- dbt in `warehouse/`: physical table DDL for every non-Kafka table, MV definitions, alias views, contract tests
- Go API: single-SELECT reader against one `api_*` table per endpoint

## Motivation

Measured in April 2026:

- Staging SLA endpoints read `canonical_streaming_sla_input_hourly_store` **6×** per request, plus compute 7-day `quantileTDigestMerge` benchmark cohorts at query time. Prod (pre-materialized `api_*_store`) reads its store **2×**.
- The resolver already writes a fully-scored `canonical_streaming_sla_hourly_store` (sla_score, quality_score, ptff_score, …). **No API view reads it.**
- `api_hourly_request_demand` contains a duplicated UNION CTE (same CTE block appears twice in the generated SQL).
- Five handlers and five Grafana dashboards `LEFT JOIN api_orchestrator_identity` on every request.
- Grafana panels reach into `normalized_*`, `canonical_status_samples_recent_store`, `accepted_raw_events`, and `agg_*` — bypassing the serving contract.

Root cause is design, not scale: staging has less data than prod across every comparable store yet is slower, because the resolver writes canonical inputs while the API views recompute the scored outputs on each request.

## Target Shape

Three `api_*` families, nothing else:

| Family | Shape | Sort key convention | Serves |
|---|---|---|---|
| `api_hourly_*` | wide hourly rollup, append-only + `refresh_run_id` slice | `(window_start, dim1, dim2, …, refresh_run_id)`, partitioned `toYYYYMM(window_start)` | `/v1/streaming/{sla,demand,gpu-metrics}`, `/v1/requests/*/summary`, `/v1/dashboard/kpi`, all Grafana time-series |
| `api_current_*` | latest-per-entity, ReplacingMergeTree | `(entity_key)` with deterministic version column | `/v1/*/orchestrators`, `/v1/*/models`, pricing, inventory, live capacity |
| `api_fact_*` | append-only event log | `(window_start, entity_key)`, partitioned `toYYYYMM(window_start)` | `/v1/requests/{ai-batch,byoc}/jobs`, job feeds |

The full expected table set after Phase 8:

**`api_hourly_*`**: `streaming_sla`, `streaming_demand`, `streaming_gpu_metrics`, `request_demand`, `byoc_auth`, `payment`.

**`api_current_*`**: `capability`, `active_stream_state`, `orchestrator`.

**`api_fact_*`**: `ai_batch_job`, `ai_batch_llm_request`, `byoc_job`, `byoc_payment`.

Every endpoint → one table. Every dashboard panel → one table (declared in panel meta). Every handler → one `SELECT … WHERE … ORDER BY … LIMIT …`, no runtime joins of heavy tables.

Details live in [`../../design-docs/api-table-contract.md`](../../design-docs/api-table-contract.md).

## Conventions for this plan

- **Branch model:** one feature branch per phase, merged into a long-lived `refactor/medallion-v2` integration branch. Cut to `main` after Phase 8.
- **Change unit:** each phase ships (a) dbt models, (b) resolver changes, (c) handler/service changes, (d) Grafana changes, (e) tests — as one PR per phase. Co-located, not a "schema PR plus code PR" split.
- **Validation bar:** dbt tests green, Go tests green, `make lint` clean, golden-fixture replay byte-identical, benchmark report committed for every touched endpoint.
- **No back-compat:** rename / drop in place. Full reload from `naap.raw_events` at the end of each phase. No dual-write shims, no `_v2` suffixes, no legacy tolerance clauses.
- **Determinism is non-negotiable.** Same raw events → same final output, byte for byte. No `now()` / `today()` / `rand()` inside transformations. Explicit `ORDER BY` tiebreakers on every `argMax`, `LIMIT 1`, window function.

---

## Phase 0 — Contract, lint, replay harness, fixtures

**Duration:** 3–4 days. **Risk:** low. **User-visible:** none.

This phase is deliberately front-loaded: the replay harness and contract tests built here are the verification substrate for every later phase. Nothing in Phases 1–8 merges without green replay across touched layers.

| Task | Deliverable |
|---|---|
| Three-families contract doc | `docs/design-docs/api-table-contract.md` — column requirements (`refresh_run_id`, `refreshed_at`, entity key), naming, sort-key rules, endpoint mapping, additivity rule, core-logic ownership rule |
| dbt layer lint | Custom dbt test `assert_layer_discipline` — walks `{{ ref() }}` graph; fails if `api_*` refs `normalized_*` or `stg_*`, or if `canonical_*` refs `api_*` or `api_base_*` |
| Grafana lint | `scripts/grafana-lint.go` — parses panel JSON, fails if any SQL mentions `normalized_*`, `stg_*`, `raw_events`, `agg_*`, or `canonical_*_store` directly, or if panel meta is missing `backing_table` |
| Additive-primitives lint | dbt test `assert_additive_primitives_present` — every `api_hourly_*` model declares its primitive→derived mapping in `schema.yml`; test verifies primitives are non-nullable |
| Core-logic recalc lint | `scripts/core-logic-signatures.txt` plus a CI grep check against `api/internal/service/` and `warehouse/models/api/` |
| Golden fixture | `tests/fixtures/raw_events_golden.ndjson` — 72h raw-event snapshot covering every topic family (streaming, AI batch, BYOC, LLM, worker lifecycle, capability snapshots, payments) |
| **Layer-by-layer replay harness** | **`tools/replay/`** — see dedicated subsection below |
| Unify store ownership in dbt | Every `_store` MergeTree currently declared in `infra/clickhouse/migrations/*.sql` moves to dbt with `materialized='table'`, explicit `order_by`, `partition_by`, and `post_hook` for projections/indexes. After this phase, dbt owns every warehouse table; migrations become a thin DB/user bootstrap |
| Rename `api_base/` → `intermediate/` | Mechanical rename in `dbt_project.yml`; signals intent to delete in Phase 5 |

### The replay harness (`tools/replay/`)

A first-class tool, not a single test. Drives raw events through every boundary and asserts byte-identical output at each one, so divergence is localized to the layer that introduced it.

**Shape:**

```
tools/replay/
  main.go              # CLI entry: replay --fixture=<path> --layers=all|raw,normalized,canonical,api
  pipeline.go          # orchestrates the four stages
  assertions.go        # artifact_checksum rollups per table, cross-run diffing
  fixtures/            # named fixtures (golden, empty, boundary-cases, full-day, week-rollup)
  reports/             # per-run HTML/JSON reports written to ./target/replay/
```

**Stages:**

1. **Load** — `TRUNCATE` all layer tables; insert the fixture into `naap.raw_events`.
2. **Normalized** — wait for Kafka MV propagation (deterministic with bounded-time fixture); snapshot every `normalized_*` table; record `artifact_checksum` rollup.
3. **Canonical** — run the resolver over the frozen `RunRequest.now`; snapshot every `canonical_*` table; record rollup.
4. **API** — run `dbt build --select tag:api`; snapshot every `api_*` table; record rollup.

**Assertions:**

- Two consecutive full runs over the same fixture produce identical rollups at every boundary.
- A fixture → layer pin: committed `fixtures/golden/expected.json` maps each table to its expected `artifact_checksum`. A failing layer names the first table that diverged.
- Selective replay: `--layers=canonical,api` skips earlier stages if their inputs are already loaded — supports fast iteration during Phases 1–6.
- Per-phase hook: `make replay-phase-N` runs just the layers touched by Phase N.

**Use in later phases:**

- Every phase's exit criterion becomes "replay harness green for touched layers."
- A regression shows up as "canonical_* layer diverged; first divergent table is X" — not "something somewhere is off."
- The harness is how we prove the "same raw events → same final output" invariant in CI, per the determinism mandate.

**Exit criteria:** `dbt build` passes; `make replay` produces bit-identical rollups across two consecutive runs at all four layer boundaries; all four linters run in CI; `tools/replay/` documented in `docs/operations/replay-harness.md`.

---

## Phase 1 — Wire the already-written SLA rollup

**Duration:** 1 day. **Risk:** low. **User-visible:** SLA endpoint p95 drops significantly.

This reclaims the single largest latency win with near-zero new code.

1. **Promote** `canonical_streaming_sla_hourly_store` → `api_hourly_streaming_sla_store`. Move DDL into dbt:

   ```sql
   -- warehouse/models/api/api_hourly_streaming_sla_store.sql
   {{ config(
       materialized='table',
       order_by=['window_start', 'orchestrator_address', 'pipeline_id',
                 "ifNull(model_id, '')", "ifNull(gpu_id, '')", 'refresh_run_id'],
       partition_by='toYYYYMM(window_start)'
   ) }}
   ```

2. **Rewrite** `warehouse/models/api/api_hourly_streaming_sla.sql` to a thin alias view:

   ```sql
   {{ config(materialized='view') }}
   WITH latest_slices AS (
     SELECT window_start, argMax(refresh_run_id, refreshed_at) AS refresh_run_id
     FROM {{ ref('api_hourly_streaming_sla_store') }}
     GROUP BY window_start
   )
   SELECT s.* EXCEPT (refresh_run_id, refreshed_at, artifact_checksum)
   FROM {{ ref('api_hourly_streaming_sla_store') }} s
   INNER JOIN latest_slices l USING (window_start, refresh_run_id)
   ```

3. **Update resolver** at `api/internal/resolver/repo.go:3240` — rename `INSERT INTO naap.canonical_streaming_sla_hourly_store` → `naap.api_hourly_streaming_sla_store`. Regenerate `docs/generated/schema.md`.

4. **Tests:**
   - dbt uniqueness on `(window_start, orchestrator_address, pipeline_id, ifNull(model_id, ''), ifNull(gpu_id, ''), refresh_run_id)`.
   - Parity test: new view returns the same rows as the previous `api_base_sla_compliance_scored_by_org` view on the golden window.

5. **Benchmark** both `EXPLAIN PLAN indexes=1` outputs and recorded p50/p95 query durations, committed as `docs/exec-plans/active/serving-layer-v2-phase1-bench.md`.

**Exit criteria:** staging SLA endpoint p95 within 2× of prod baseline; all tests green; `make replay-phase-1` (layers: canonical, api) green with byte-identical rollups at both boundaries.

---

## Phase 2 — Pre-compute SLA benchmark cohort

**Duration:** 2–3 days. **Risk:** low-medium (resolver insert path change). **User-visible:** further SLA p95 drop; reliable benchmark stability.

Kills the 7-day `arrayJoin` plus `quantileTDigest*Merge` work at request time.

1. **New canonical table:** `canonical_sla_benchmark_daily_store`
   - Sort key: `(pipeline_id, ifNull(model_id, ''), cohort_date, refresh_run_id)`
   - Partition: `toYYYYMM(cohort_date)`
   - Columns: `cohort_date`, `pipeline_id`, `model_id`, `ptff_p50`, `ptff_p90`, `e2e_p50`, `e2e_p90`, `fps_p10`, `fps_p50`, `ptff_row_count`, `e2e_row_count`, `fps_row_count`, `refresh_run_id`, `refreshed_at`, `artifact_checksum`

2. **Resolver writer:** new `repo.insertSLABenchmarkDaily(ctx, runID, queryID)`. Aggregates `api_hourly_streaming_sla_store` for completed days into resolved p50/p90 directly — no intermediate `*State` columns, no cohort-daily-state view. Wired from `engine.go` after `insertFinalSLAComplianceRollups`.

3. **Resolver rewrite** of `insertFinalSLAComplianceRollups`:
   - Read `canonical_streaming_sla_input_hourly_store` (inputs).
   - Point-lookup join to `canonical_sla_benchmark_daily_store` on `(pipeline_id, model_id, cohort_date)`.
   - Drop the dependency on `api_base_sla_compliance_scored_by_org`.

4. **Retire** `api_base_sla_quality_cohort_daily_state.sql` last — after the resolver no longer references it.

5. **Tests:**
   - dbt `relationships` test — benchmark has coverage for every (pipeline, model, day) present in inputs.
   - Uniqueness on `(pipeline_id, ifNull(model_id, ''), cohort_date, refresh_run_id)`.
   - Golden-fixture comparison confirming the scored output is identical to Phase 1 baseline.

**Exit criteria:** resolver produces identical `api_hourly_streaming_sla_store` rows with cohort state removed; `make replay-phase-2` green (canonical + api); SLA endpoint plan shows no `quantileTDigestMerge` node.

---

## Phase 3 — Denormalize orchestrator identity

**Duration:** 2–3 days. **Risk:** medium (touches 10+ files and 5 dashboards). **User-visible:** latency drop on orchestrator-facing endpoints and dashboards.

1. **Resolver change:** at rollup-write time, join `canonical_capability_orchestrator_inventory` and write `orchestrator_name` (ENS → `orch_name` → `orch_address` fallback), `orchestrator_uri_norm`, `orchestrator_version` directly into every `api_hourly_*` and `api_current_*` row.

2. **Schema additions:**
   - `api_hourly_streaming_{sla,demand,gpu_metrics}_store` — add `orchestrator_name String`, `orchestrator_uri_norm String`, `orchestrator_version Nullable(String)`.
   - `api_current_capability_offer_store`, `api_current_byoc_worker_store`, `api_current_active_stream_state_store` — same three columns.

3. **Handler cleanup** in `api/internal/service/service.go` — delete every `LEFT JOIN naap.api_orchestrator_identity` clause. `api_orchestrator_identity` remains as a lookup for the one endpoint that returns the full identity object.

4. **Grafana:** replace `LEFT JOIN api_orchestrator_identity` in five dashboard JSONs with the inline column. `grafana-lint` catches any leftovers.

5. **Tests:**
   - `assert_orchestrator_name_present` — dbt test that `orchestrator_name != ''` on every row of every affected table.
   - Parity test — joined-result vs. denormalized-result row-equal on fixture.

**Exit criteria:** zero `api_orchestrator_identity` joins in `api/` and `infra/grafana/`; query plans lose one JOIN node per touched endpoint; `make replay-phase-3` green (canonical + api) — the denormalized columns carry deterministic values across runs.

---

## Phase 4 — Unified capability spine

**Duration:** 3–4 days. **Risk:** medium-high (semantic unification of builtin/BYOC). **User-visible:** consistent model listings; discover endpoint is a single scan.

1. **Promote** `canonical_capability_catalog` to a seed + resolver-augmented canonical. Columns: `capability_id`, `capability_name`, `capability_family ∈ {builtin, byoc}`, `canonical_pipeline`, `supports_stream`, `supports_request`, `constraint_kind`.

2. **New api table:** `api_current_capability_store` — one row per `(org, orch_address, capability_id, canonical_pipeline, model_id, gpu_id)` with `capability_family` flag, pricing, `advertised_capacity`, `hardware_present`, `last_seen`, `orchestrator_name`. Resolver writes it from `canonical_capability_offer_inventory` + `canonical_capability_pricing_inventory` + `canonical_byoc_workers` in one insert.

3. **Retire** by replacing with filtered alias views:
   - `api_observed_capability_offer` → `WHERE hardware_present = 1 AND capability_family = 'builtin'`
   - `api_observed_capability_pricing` → `WHERE price_per_unit > 0`
   - `api_observed_capability_hardware` → `WHERE hardware_present = 1`
   - `api_observed_byoc_worker` → `WHERE capability_family = 'byoc'`

4. **Handler unification:** `GetRequestsModels`, `GetRequestsOrchestrators`, `DiscoverOrchestrators`, `GetDashboardPricing` each collapse to a single filtered `SELECT`. Delete the in-memory Go UNIONs and the `if capability_family = 'byoc' then ... else ...` branching.

5. **Grafana:** supply-inventory, overview, economics dashboards updated to query the unified table.

6. **Tests:**
   - Migrate `test_api_current_capability_pricing_positive` to the unified table.
   - New `test_api_current_capability_row_per_offer_unique` — primary-key uniqueness.
   - Contract test confirming retired alias views return the same rows as their pre-unification definitions on the fixture.

**Exit criteria:** supply-inventory dashboard renders from one table; discover endpoint issues one scan, not two; `capability_family` branching removed from `api/internal/service/service.go`; `make replay-phase-4` green (canonical + api) with the retired alias views producing row-equal output to their pre-unification definitions on the fixture.

---

## Phase 5 — Retire `api_base_*` entirely

**Duration:** 1–2 days. **Risk:** low. **User-visible:** none (already unreferenced after Phase 2).

1. Delete `warehouse/models/intermediate/` (renamed from `api_base/` in Phase 0). All `api_base_sla_*` models are dead after Phase 2.
2. Delete the six `tests/test_api_base_*` files.
3. Update `docs/design.md`, `docs/design-docs/architecture.md`, and amend ADR-003 (the tier table loses the `api_base_*` row; add a note pointing to this plan).
4. Delete obsolete model files carried from the main-branch era: `api_active_stream_state.sql`, `api_sla_compliance.sql`, `api_sla_compliance_by_org.sql`, and any other pre-branch serving views no longer referenced.

**Exit criteria:** `warehouse/models/api/` holds exactly the endpoint-table set agreed in the contract doc; no intermediate directory; `make replay-phase-5` green (api only — schema footprint shrinks, rollups unchanged).

---

## Phase 6 — The three-families table set

**Duration:** 3–4 days. **Risk:** medium (broadest handler surface). **User-visible:** every endpoint latency tightens; handler code simplifies significantly.

1. **Collapse `api_hourly_request_demand`:** fix the duplicated UNION CTE (single CTE block, `UNION ALL` the two `SELECT` branches). Promote to a resolver-written store to match the `_hourly` family convention — currently a pure view over `canonical_ai_batch_jobs` + `canonical_byoc_jobs` that rescans canonical per request.

2. **Promote `api_hourly_byoc_auth`** from view → resolver-written store.

3. **New `api_current_orchestrator_store`** — denormalizes identity plus latest reliability plus capability counts into one row per `(org, orch_address)`. Serves `/v1/streaming/orchestrators`, `/v1/requests/orchestrators`, and dashboard-orchestrators in one scan.

4. **`api_fact_*` tables** — already append-only event logs; enforce consistent sort key `(window_start, entity_key)` and `capability_family` column presence.

5. **Handler rewrites:** every handler in `api/internal/runtime/handlers_*.go` reduces to a single-table, single-`SELECT` shape. Delete the multi-query orchestration in `service.go` (the "run four queries and rejoin in Go" pattern).

6. **Keyset cursor helper:** extract per-handler cursor encode/decode into `api/internal/runtime/cursor.go`; one implementation keyed by `(window_start, stable_key)`.

**Exit criteria:** every endpoint's SQL is one `SELECT … FROM api_* WHERE … ORDER BY … LIMIT …`; `service.go` is pure delegation; query plans show 1–2 MergeTree reads per call; `make replay-phase-6` green across all four layer boundaries (the broadest-surface phase exercises the full pipeline); additive-primitives lint passes on every `api_hourly_*` model; core-logic-recalc lint green.

---

## Phase 7 — Grafana cleanup and contract

**Duration:** 2 days. **Risk:** low. **User-visible:** consistent dashboard semantics; internal debug panels clearly isolated.

1. Run `grafana-lint` across six `naap-*.json` dashboards. Rewrite every panel that references `normalized_*`, `canonical_status_hours`, `canonical_status_samples_recent_store`, `canonical_ai_batch_jobs`, `canonical_byoc_jobs`, `canonical_byoc_payments`, `accepted_raw_events`, or `agg_webrtc_hourly` to the equivalent `api_*` table.

2. Where no `api_*` equivalent exists (some `naap-jobs` debugging panels reach into `normalized_*`), create a new `api_fact_*` or `api_hourly_*` table — do not whitelist the leak.

3. Annotate each panel: `"meta": { "backing_table": "naap.api_hourly_streaming_sla" }`. Contract test validates panel → backing_table coverage.

4. Isolate debug panels: remaining `canonical_*`-scoped panels move to `infra/grafana/dashboards/internal/naap-internal-debug.json`, not provisioned for end users. Clear "not a serving contract" signal.

5. Migrate alerting rules in `infra/grafana/provisioning/alerting/11-clickhouse-rules.yml` to `api_*` sources.

**Exit criteria:** `grafana-lint` passes on every `infra/grafana/dashboards/naap-*.json`; every panel has `backing_table` meta; alerting rules source from `api_*`; `make replay-phase-7` green (api only).

---

## Phase 8 — Resolver structure cleanup and legacy pruning

**Duration:** 2–3 days. **Risk:** low (refactor + drops). **User-visible:** none.

Determinism is already enforced by the replay harness from Phase 0 and has been green at every phase boundary by the time this phase starts. This phase is about structural cleanup and pruning the long tail.

1. **Single-responsibility resolver:** every `repo.insert*` function writes exactly one table. Delete incidental multi-target helpers. Resolver surface becomes exactly: one function per `canonical_*_current`, `canonical_*_hourly`, `api_hourly_*_store`, `api_current_*_store`, `api_fact_*`.

2. **Centralize slice metadata:** `warehouse/macros/refresh_slice.sql` plus `api/internal/resolver/slice.go` — one codepath sets `refresh_run_id`, `refreshed_at`, `artifact_checksum` on every writer. Removes ~30 copies of the pattern across `repo.go`.

3. **Extend replay harness fixtures:** add `fixtures/boundary-cases/` (empty windows, single-event windows, late-arriving events, resolver repair path) and `fixtures/week-rollup/` (for SLA benchmark cohort coverage). Wire into CI matrix.

4. **dbt-project-evaluator** in CI — fails on cross-layer skips, un-tested models, missing descriptions, fan-out violations.

5. **Drop legacy artifacts:**
   - Migration `016_drop_legacy_agg.sql` — drops `agg_*` tables unreferenced after Phase 6.
   - Migration `017_drop_unused_normalized_rollups.sql` — audited list of `normalized_*` rollups no longer fed into any `api_*` store.

**Exit criteria:** `make replay` green across the full fixture matrix (golden, boundary-cases, week-rollup); dbt-project-evaluator green; every `normalized_*` and `canonical_*` table is (a) resolver-input, (b) MV-input, or (c) a documented debug surface.

---

## Cross-cutting: final testing stack

Per the determinism mandate:

- **Layer-by-layer replay harness** (`tools/replay/`) — the first-class verification tool. Drives raw events through every boundary, asserts byte-identical rollups at each, localizes divergence to the layer that introduced it. Built in Phase 0, green at every phase's exit criterion.
- **Uniqueness** on every `_store` table's declared grain.
- **Completeness** (row per expected (dim × window) combination) on every `_hourly_*`.
- **Relationships:** every `api_*` foreign dim (orchestrator_address, pipeline_id, capability_id) exists in the corresponding `canonical_*_current`.
- **Golden-fixture + boundary + week-rollup** fixtures under `tools/replay/fixtures/`, all running in CI by Phase 8.
- **Additive-primitives** test — every `api_hourly_*` declares and exposes its additive primitives.
- **Core-logic recalc** grep lint — blocks re-derivation of definitional logic at the API layer.
- **Grafana contract** test — panel → backing_table exists, and backing_table is in the `api_*` family.
- **Layer discipline** test — ref graph, no skips.

## Sequencing and risks

- Phases 1–2 are the biggest user-visible wins and are low-risk (data already exists; rewiring only).
- Phase 3 is the riskiest for bloat; measure bytes added and confirm acceptable (orchestrator count is hundreds; names ~20 bytes).
- Phase 4 is the biggest semantic rewrite — give it a standalone PR with a parity test matrix across `capability_family × hardware_present × supports_stream × supports_request`.
- Phase 6 is the broadest surface-area change. Hold until 1–5 are stable.
- Full-reload from `naap.raw_events` at the end of each phase — no dual-write, no back-compat, no legacy tolerance.

## Estimated duration

Sequential, one engineer: **~3.5 weeks** (Phase 0 expanded by 1–2 days to build the replay harness; that investment pays back immediately in faster Phase 1–6 iteration). Parallelizable to ~2.5 weeks with two engineers if Phase 3 (warehouse-heavy) and Phases 6–7 (handlers plus Grafana) are split. Phase 0 must be first; 1 must precede 2; 3 and 4 can run in parallel after 2; 5–8 sequential.

## Deliverables

- `docs/exec-plans/active/serving-layer-v2.md` — this plan
- `docs/design-docs/api-table-contract.md` — three-families contract
- Amended `docs/design.md`, `docs/design-docs/architecture.md`, ADR-003
- Per-phase PRs to `refactor/medallion-v2`, each self-contained
- `docs/exec-plans/active/serving-layer-v2-phase1-bench.md` and `-phase8-bench.md` — latency before/after reports

## Non-goals

- No schema changes to `stg_*` / `normalized_*` beyond the new request-family topics already present in staging.
- No change to the Kafka contract or ingest path.
- No new business metrics. This plan is latency, clarity, and architectural debt; product features ship separately.
- No migration of Grafana to a different datasource or Prometheus. ClickHouse remains the sole serving store.
