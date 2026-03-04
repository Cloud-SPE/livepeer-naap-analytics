# Testing and Validation

## Quality Layers

1. Flink unit and contract tests ([`flink-jobs/src/test/java`](../../flink-jobs/src/test/java))
2. Schema sync and parser guardrails
3. ClickHouse integration SQL assertions ([`tests/integration/sql`](../../tests/integration/sql))
4. Replay and trace query-pack validation
5. Notebook-assisted investigation and fixture export/load

## Core Commands

- Prerequisite (one-time on each machine/CI runner):
  - `docker volume create livepeer-analytics-flink-maven-cache`
  - This external volume is used by `flink-builder` as `/tmp/.m2` to avoid re-downloading Maven dependencies every run.

- Java tests:
  - `cd flink-jobs && mvn test`
- Scenario integration harness (full):
  - `cd flink-jobs && mvn -Pscenario-it verify`
- Scenario integration harness (smoke):
  - `cd flink-jobs && mvn -Pscenario-it-smoke verify`
- Scenario integration harness (smoke, debug/persistent):
  - `cd flink-jobs && mvn -Pscenario-it-smoke-debug verify`
- Scenario assertions/query-pack only (reuse existing stack and data; no replay/reset):
  - `cd flink-jobs && mvn -Pscenario-it-assert-only validate`
- Query trace pack:
  - `uv run --project tests/python python tests/python/scripts/run_clickhouse_query_pack.py --lookback-hours 24`
- Pipeline assertions:
  - `uv run --project tests/python python tests/python/scripts/run_clickhouse_data_tests.py --sql-file tests/integration/sql/assertions_raw_typed.sql --lookback-hours 24`
  - `uv run --project tests/python python tests/python/scripts/run_clickhouse_data_tests.py --sql-file tests/integration/sql/assertions_pipeline.sql --lookback-hours 24`
- API readiness assertions:
  - `uv run --project tests/python python tests/python/scripts/run_clickhouse_data_tests.py --sql-file tests/integration/sql/assertions_api_readiness.sql --lookback-hours 24`
- Scenario assertions:
  - `uv run --project tests/python python tests/python/scripts/run_clickhouse_data_tests.py --sql-file tests/integration/sql/assertions_scenario_candidates.sql --lookback-hours 720`
  - Note: `scenario_4_success_with_param_updates_exists` is currently informational (non-blocking) until param-update source rows are present in production.
  - Note: if `scenario_3_success_with_swap_exists` fails while replay and raw/typed tests pass, first check fixture coverage ([`tests/integration/fixtures/manifest.json`](../../tests/integration/fixtures/manifest.json)) before debugging parser logic.
- One-shot integration run:
  - `tests/integration/run_all.sh`

## GitHub CI Split

Use a split CI strategy to balance confidence and runtime:

1. PR required checks (`CI PR Smoke`):
   - `cd flink-jobs && mvn test`
   - Reduced docker integration smoke via harness stages:
     - `stack_up`, `schema_apply`, `pipeline_ready`, `replay_events`, `pipeline_wait`
     - `assert_raw_typed`, `assert_pipeline`, `assert_api`
     - `stack_down`
   - Scenario candidate assertions are intentionally excluded from PR smoke.

2. Nightly/full checks (`CI Nightly Full`):
   - Full harness run (`--mode full`) including query pack and scenario assertions.
   - Uploads full artifacts for drift and long-window diagnostics.

3. Manual deep verification (`CI Manual Deep Verify`):
   - `workflow_dispatch` with mode and window controls.
   - Use for on-demand investigation or release validation.

Workflow files:

- [`.github/workflows/ci-pr-smoke.yml`](../../.github/workflows/ci-pr-smoke.yml)
- [`.github/workflows/ci-nightly-full.yml`](../../.github/workflows/ci-nightly-full.yml)
- [`.github/workflows/ci-manual-deep-verify.yml`](../../.github/workflows/ci-manual-deep-verify.yml)

## Pipeline Assertions Requirement Map

Reference SQL: [`tests/integration/sql/assertions_pipeline.sql`](../../tests/integration/sql/assertions_pipeline.sql)

Use this map to understand why each assertion exists and where to look first when it fails.
Maintenance rule: keep this table in sync with [`tests/integration/sql/assertions_pipeline.sql`](../../tests/integration/sql/assertions_pipeline.sql).
Any add/rename/remove of a `-- TEST:` block must co-update this section in the same change.

| Assertion (`-- TEST:`) | Requirement Enforced | First Triage Check |
|---|---|---|
| `raw_events_present` | Core raw/typed ingress objects have data in-window. | Confirm assertion `from_ts`/`to_ts` matches replay window and fixture manifest window. |
| `capability_dimension_mvs_present` | Required capability MVs exist in ClickHouse. | Check `system.tables` for missing MV names and confirm schema init was applied. |
| `capability_dimensions_projecting` | Capability source tables project into dimension snapshot tables. | Compare row counts between `network_capabilities*` and `dim_orchestrator_capability_*` tables in-window. |
| `capability_snapshot_gpu_coverage` | Capability snapshot dimensions preserve per-event GPU coverage without collapse or split relative to typed capabilities. | Compare distinct GPU counts per `(source_event_id, orchestrator, pipeline, model_id)` and inspect both deficits (`loss_rows`) and expansions (`expansion_rows`). |
| `capability_prices_key_multiplicity_guard` | Capability prices remain unique at current key grain. | Check duplicate groups for `(source_event_id, orchestrator, capability_id, event_timestamp)` in `network_capabilities_prices`; duplicates indicate potential collapse risk with current key contract. |
| `session_fact_present` | Sessionization emits `fact_workflow_sessions` rows. | Confirm lifecycle operators are running and replay produced stream trace/status rows. |
| `core_raw_to_silver_gold_nonempty` | Core flow has non-zero accepted raw rows and non-empty silver+gold facts. | Compare core raw distinct IDs vs DLQ/quarantine distinct IDs, then verify status/trace silver rows and session fact rows are non-zero. |
| `network_capabilities_raw_and_typed_present` | Capabilities are present both raw and typed for attribution windows. | Check `streaming_events(type='network_capabilities')` vs `network_capabilities` parse output. |
| `status_raw_to_silver_projection` | Typed status rows are losslessly projected to silver status fact, including per-source-UID multiplicity. | Compare typed vs silver counts by `lower(hex(SHA256(raw_json)))` source UID; inspect `missing_in_silver`, `multiplicity_mismatch`, and `silver_only_keys`. |
| `trace_raw_to_silver_projection` | Typed trace rows are losslessly projected to silver trace edges, including per-source-UID multiplicity. | Compare typed vs silver counts by `lower(hex(SHA256(raw_json)))` source UID; inspect `missing_in_silver`, `multiplicity_mismatch`, and `silver_only_keys`. |
| `ingest_raw_to_silver_projection` | Typed ingest rows are losslessly projected to ingest silver fact, including per-source-UID multiplicity. | Compare typed vs silver counts by `cityHash64(raw_json)` source UID; inspect `missing_in_silver`, `multiplicity_mismatch`, and `silver_only_keys`. |
| `session_final_uniqueness` | Latest-version session rows are unique per `workflow_session_id` under `FINAL`. | Inspect duplicate latest `version` rows in `fact_workflow_sessions`. |
| `workflow_session_has_identifier` | Session rows always carry non-empty `workflow_session_id`. | Inspect lifecycle signal key construction (`stream_id|request_id`) for blanks. |
| `swap_signal_split_consistency` | Compatibility alias `swap_count` must equal explicit swap count (`confirmed_swap_count`); inferred swaps are tracked separately (`inferred_orchestrator_change_count`). | Validate session fact emission contract in Flink state machine and mapper fields. |
| `gold_sessions_use_canonical_orchestrator_identity` | Gold/session orchestrator identity is canonical (not hot-wallet/local). | Compare session `orchestrator_address` to canonical `network_capabilities.orchestrator_address` in-window. |
| `swapped_sessions_have_evidence` | Sessions flagged swapped by canonical semantics (`confirmed_swap_count > 0` or `inferred_orchestrator_change_count > 0`) must have fact-level evidence (explicit swap trace edge or multi-orchestrator segment history). | Check `fact_stream_trace_edges.is_swap_event` and segment orchestrator cardinality by `workflow_session_id`; avoid typed/raw fallback joins for attribution evidence. |
| `param_updates_reference_existing_session` | Param-update facts must reference a known session id (no orphan IDs). | Left join `fact_workflow_param_updates` to all known session ids and inspect orphan IDs. |
| `lifecycle_session_pipeline_model_compatible` | Attributed sessions keep `pipeline` and `model_id` semantically distinct when both set (`pipeline` workflow class, `model_id` model label). | Inspect lifecycle resolver mode, capability attribution selection, and canonical field assignment paths. |
| `lifecycle_unattributed_pipeline_not_model_id` | Unattributed sessions (`gpu_attribution_method = 'none'`) must not persist model labels into `pipeline`; `pipeline` remains workflow-class semantics even without capability attribution. | Inspect lifecycle signal canonicalization in session aggregation and verify unresolved legacy-model hints do not populate session `pipeline`. |
| `session_to_segment_pipeline_model_consistency` | Segment rows keep pipeline/model identity consistent with their parent attributed session. | Compare latest session vs latest segment `pipeline`/`model_id` by `workflow_session_id`/`segment_index`; inspect missing comparable rows. |
| `session_to_latency_pipeline_model_consistency` | Latency samples preserve canonical session `pipeline` and `model_id`. | Compare latest `fact_workflow_latency_samples` fields against latest session fields by `workflow_session_id`. |
| `session_to_status_projection_consistency` | Status/session pipeline-model alignment is enforced with severity tiers: partial drift is WARN, full comparable-set mismatch is FAIL. | Compare `pipeline_mismatch_rows`/`model_mismatch_rows` to comparable row counts and inspect model variant normalization (`streamdiffusion-sdxl` vs `streamdiffusion-sdxl-v2v`) before escalation. |
| `status_vs_segment_identity_consistency` | Status-to-segment identity alignment is monitored with severity tiers: partial drift is WARN, all-row mismatch is FAIL. | Inspect mismatch examples by `workflow_session_id` for canonical orchestrator/model/GPU divergence; use `missing_segment_match_rows` as timing/context diagnostics. |
| `latency_vs_segment_identity_consistency` | Latency-to-segment identity alignment is monitored with severity tiers: partial drift is WARN, all-row mismatch is FAIL. | Inspect latest latency rows lacking valid segment window matches (`segment_start_ts`/`segment_end_ts`) before classifying as blocking drift. |
| `proxy_to_canonical_multiplicity_guard` | Proxy identity fanout is severity-tiered: `(local_address, orch_uri)` multi-canonical is FAIL; proxy-only multi-canonical is WARN. | Check canonical cardinality both per `local_address` and per `(local_address, orch_uri)` in `network_capabilities`; prioritize URI-key collisions as blocking. |
| `session_summary_change_flags_consistency` | Session derived change flags must match distinct segment pipeline/model counts. | Compare `has_model_change`/`has_pipeline_change` from latest sessions against distinct segment values per `workflow_session_id`. |
| `gpu_view_no_perf_only_orphan_rows` | GPU perf-only orphan rows are tracked as non-blocking drift signal (WARN). | Inspect `v_api_gpu_metrics` rows with `status_samples>0` and zero known/latency evidence, then verify segment-hour key coverage for those keys. |
| `trace_edge_pipeline_model_coverage` | Trace-edge pipeline/model coverage is severity-tiered: partial uncovered attributed sessions are WARN, fully uncovered attributed set is FAIL. | Aggregate `fact_stream_trace_edges` by `workflow_session_id`; inspect uncovered sessions for missing pipeline/model projection on edge rows. |
| `latest_sessions_vs_segment_session_ids` | Distinct segmented session ids do not exceed latest session ids. | Compare latest session selection logic vs segment fact time filtering in-window. |
| `raw_session_rows_vs_latest_sessions` | Raw session rows are never fewer than latest-per-session rows. | Inspect `fact_workflow_sessions` versioning/upserts and replay window boundaries. |
| `segment_rows_vs_segment_session_ids` | Segment row count is always >= distinct segment session ids. | Check segment emission completeness and any segment-level dedup/drop behavior. |
| `mixed_known_stream_versions` | Mixed `known_stream` versions are allowed, but regressive `1 -> 0` transitions fail. | Inspect per-session version history ordered by `version` for regressive transitions. |
| `agg_stream_performance_1m_matches_status_samples` | `agg_stream_performance_1m` is lossless/numerically consistent with canonical attributed status facts (`is_attributed=1`, non-empty `orchestrator_address`) at 1-minute grain. | Compare key coverage first (`expected_only_keys`/`rollup_only_keys`), then session/stream/sample and FPS diffs for canonical serving keys. |
| `gpu_view_covers_healthy_attributed_session_keys` | Successful attributable session keys are represented in `v_api_gpu_metrics`. | Check missing key examples and verify fallback attribution fields (`model_id/gpu_id/region`) for those sessions. |
| `demand_has_rows_for_all_session_hours` | Every session hour is represented in `v_api_network_demand`. | Inspect missing `window_start` hours and check demand view filters/materialization lag. |
| `gpu_count_delta_explained_by_key_overlap` | GPU view-vs-rollup row delta must equal net key-overlap delta. | Compare `row_delta` vs `overlap_delta`; investigate unexpected key proliferation/drop. |
| `network_demand_counts_aligned_to_rollup` | Demand view keyspace/counts strictly align with recomputed union(perf,demand) keyspace at `(hour,gateway,region,pipeline,model_id)` grain. | Inspect `rollup_only_keys`/`view_only_keys` first, then demand/perf key derivation and model-key normalization. |
| `network_demand_effective_success_not_above_startup_success` | Effective success must not exceed startup-only success for any demand row. | Investigate rows where `success_ratio > startup_success_ratio`; check effective-failure signal derivation. |
| `network_demand_effective_success_penalizes_failure_indicators` | Demand rows with effective failure indicators must not report perfect effective success. | Inspect rows with unexcused/zero-output/loading-only counters and `success_ratio ~= 1`; transient `last_error_occurred` alone is non-penalizing. |
| `network_demand_model_split_conservation` | Informational guard: model-grain re-aggregation may inflate distinct-count fields (`total_sessions`, `total_streams`), while additive `total_minutes` should remain conserved. | Compare pipeline-baseline recompute versus re-aggregated `v_api_network_demand`; treat distinct-count deltas as WARN and minutes/key drift as blocking only when inconsistent. |
| `network_demand_join_multiplicity_guard` | Demand parity keyspaces are unique at model grain (1:1 joinable). | Check duplicate key counts on both expected rollup keys and API keys for `(hour,gateway,region,pipeline,model_id)`. |
| `network_demand_pipeline_join_inflation_guard` | Informational guard for naive pipeline-only joins against model-grain demand rows. | If `status='WARN'`, consumer joins must include `model_id` or pre-aggregate before joining. |
| `preexisting_grain_drift_baseline_gpu_sla` | Existing model-aware GPU/SLA grain drift is tracked as non-blocking baseline signal (WARN). | Compare pipeline-collapsed view totals and key coverage (`*_only_keys`) with independent rollup/session recomputes and monitor trend direction. |
| `sla_counts_aligned_to_raw_latest_sessions` | SLA view keyspace/counts strictly align with latest-session recompute keyspace for attributed sessions (`orchestrator_address != ''`), including health counters/signals. | Inspect `raw_only_keys`/`view_only_keys`; verify latest-session dedup, attribution filter, hour bucketing, and health-signal aggregate parity. |
| `view_count_grain_ordering` | Informational grain-ordering check across serving views. | Treat WARN as diagnostic unless accompanied by blocking parity failures. |
| `gpu_view_matches_rollup` | GPU API view must match independently recomputed perf aggregates from status facts on both keyspace and numeric values for perf-backed attributable GPU keys. | Check `rollup_only_keys`/`view_only_keys` first (blocking), then inspect `max_abs_diff_fps` and key completeness diagnostics (`*_empty_*_rows`). |
| `network_demand_view_matches_rollup` | Network demand API view must match recomputed hourly union(perf,demand) keys with perf+demand aggregates on both keyspace and numeric values at model-aware demand grain. | Check `rollup_only_keys`/`view_only_keys` first (blocking), then inspect total diff diagnostics (including empty model diagnostics). |
| `sla_view_matches_session_fact` | SLA API counts must match independent recomputation from session facts on both keyspace and numeric values for attributed sessions, including health/error indicators. | Check `raw_only_keys`/`view_only_keys` first (blocking), then compare known/unexcused/swapped and health-signal diff totals. |
| `terminal_tail_artifact_filtered_in_gpu_sla` | True rollover tail artifact hours (rollover end-hour + no-work current/previous hour) are filtered from GPU/SLA views. | Check artifact candidate count and verify GPU/SLA hit counts are zero. |
| `same_hour_failed_no_output_retained_in_gpu_sla` | Same-hour failed no-output sessions (short, attributed, error-indicated) remain visible in GPU/SLA views. | Verify same-hour failure candidates exist in both API views; missing rows are blocking. |
| `active_no_output_rows_retained_in_gpu_sla` | Active no-output hours (material status evidence but zero FPS) remain visible in GPU/SLA views. | Verify candidate keys exist in both API views; missing keys are blocking. |
| `gold_pipeline_empty_hotspots_diagnostic` | Informational hotspot scan for rows where `model_id` exists but canonical `pipeline` is empty in GPU/SLA views. | Use top orchestrator/model/GPU hotspots to debug attribution misses; non-blocking by design. |
| `sla_ratios_in_bounds` | SLA ratios are bounded to valid ranges (`success_ratio`/`no_swap_ratio`/`health_completeness_ratio` in `[0,1]`, `sla_score` in `[0,100]`). | Inspect denominator/guard conditions in `v_api_sla_compliance`, especially health completeness weighting. |
| `serving_views_do_not_reference_typed_raw_tables` | API serving views (`v_api_*`) must not directly reference raw/typed ingestion tables. | Inspect `system.tables.create_table_query` for offending `v_api_*` definitions and refactor to `fact_*`/`agg_*`/`dim_*` inputs. |

### Join Anti-Patterns (Reference Examples)

Use these failures as canonical examples of joins to avoid:

1. Proxy-key attribution joins (`local_address`/hot-wallet as canonical identity)
  - Risk: one proxy can fan out to multiple canonical orchestrators in-window, causing wrong model/GPU attribution.
  - Guardrails: `proxy_to_canonical_multiplicity_guard` and Flink selector ambiguity rejection (return no attribution on multi-canonical in-window candidates).

2. Non-equi `LEFT JOIN ... ON` time-window joins in ClickHouse assertions/views
  - Risk: engine-level failure on stable builds (for example `INVALID_JOIN_ON_EXPRESSION`) and fragile behavior if experimental flags are required.
  - Guardrails: use join-safe patterns (`ASOF JOIN` + explicit interval-validity predicate) for sample-to-segment time alignment checks.

3. Serving joins that bypass fact/agg contracts and touch raw/typed tables
  - Risk: metric drift and identity drift caused by reintroducing ingestion semantics into API surfaces.
  - Guardrails: `serving_views_do_not_reference_typed_raw_tables` and strict serving dependency boundary (`v_api_*` -> `fact_*`/`agg_*`/`dim_*`).

## Scenario Harness Stages

The harness is stage-based and writes a report + logs for every run.
Default behavior is full-stack replay via Kafka -> Flink -> ClickHouse assertions
(not direct ClickHouse fixture insertion).

- Script:
  - [`tests/python/scripts/run_scenario_test_harness.py`](../../tests/python/scripts/run_scenario_test_harness.py)
- Stage list:
  - `stack_up`
  - `schema_apply`
  - `replay_events`
  - `pipeline_wait`
  - `query_pack`
  - `assert_pipeline`
  - `assert_api`
  - `assert_scenarios`
  - `stack_down`
- Run one stage for debugging:
  - `uv run --project tests/python python tests/python/scripts/run_scenario_test_harness.py --stage assert_pipeline`
- Keep stack running after failure:
  - `uv run --project tests/python python tests/python/scripts/run_scenario_test_harness.py --mode full --keep-stack-on-fail`

### Smoke vs Smoke-Debug

- `scenario-it-smoke` (default CI-friendly):
  - Uses `--down-volumes` on teardown.
  - Removes scenario volumes (including ClickHouse scenario data) at run end.
  - Best for clean, repeatable CI runs.

- `scenario-it-smoke-debug` (local investigation):
  - Uses the same smoke validation stages but intentionally omits `stack_down`.
  - Stack and scenario volumes stay up after both successful and failed runs.
  - Keeps scenario database volumes intact, so notebook/manual SQL can inspect resulting data.
  - Manually tear down when done:
    - `docker compose -f docker-compose.yml -f docker-compose.scenario.yml down --remove-orphans`

Recommended workflow:
1. Validate with `scenario-it-smoke`.
2. If deeper analysis is needed, run `scenario-it-smoke-debug`, then inspect with notebook/queries.

### Harness Artifacts

Every run writes to:

- `artifacts/test-runs/<run_id>/harness.log`
- `artifacts/test-runs/<run_id>/stages/<stage>.log`
- `artifacts/test-runs/<run_id>/summary.json`
- `artifacts/test-runs/<run_id>/report.md`

When assertion stages run, JSON outputs are also written under:

- `artifacts/test-runs/<run_id>/stages/assert_pipeline.json`
- `artifacts/test-runs/<run_id>/stages/assert_api.json`
- `artifacts/test-runs/<run_id>/stages/assert_scenarios.json`

## Contract-Critical Tests

- `ClickHouseSchemaSyncTest`: mappers and schema stay aligned.
- `EventParsersTest`: typed extraction behavior.
- Quality gate tests: dedup, validation, and routing semantics.
- Lifecycle state machine tests: session/segment/latency derivation invariants.
- `RefactorDriftGuardTest`: prevents reintroduction of duplicated normalization/fallback helper implementations.

## Refactor Safety Protocol

Use this sequence for shared-helper refactors:

1. Add characterization tests that lock current behavior.
2. Centralize helper logic and migrate call sites in small slices.
3. Run Java unit/contract tests:
   - `cd flink-jobs && mvn test`
4. Run integration assertions:
   - `tests/integration/run_all.sh`
5. Run query-pack validation:
   - `uv run --project tests/python python tests/python/scripts/run_clickhouse_query_pack.py --lookback-hours 24`

## Contract Drift Checklist (Generalized)

Apply this checklist to any refactor/change that can affect schema, lifecycle semantics, API views, assertions, or notebook diagnostics:

1. Keep implementation and contract docs in the same change:
   - [`configs/clickhouse-init/01-schema.sql`](../../configs/clickhouse-init/01-schema.sql) / [`flink-jobs/`](../../flink-jobs)
   - [`docs/data/SCHEMA_AND_METRIC_CONTRACTS.md`](../data/SCHEMA_AND_METRIC_CONTRACTS.md)
2. Keep assertions synchronized with requirement map:
   - [`tests/integration/sql/assertions_pipeline.sql`](../../tests/integration/sql/assertions_pipeline.sql)
   - [`docs/quality/TESTING_AND_VALIDATION.md`](./TESTING_AND_VALIDATION.md) (Pipeline Assertions Requirement Map)
3. Run the minimum validation gate:
   - `cd flink-jobs && mvn test`
   - `tests/integration/run_all.sh`
   - `uv run --project tests/python python tests/python/scripts/run_clickhouse_query_pack.py --lookback-hours 24`
4. If semantics or output fields changed, re-check notebook cells and saved outputs:
   - [`tests/python/notebooks/INTEGRATION_EXEC_SUMMARY.ipynb`](../../tests/python/notebooks/INTEGRATION_EXEC_SUMMARY.ipynb)
   - [`tests/python/notebooks/FLINK_DATA_TRACE_AND_INTEGRATION_TESTS.ipynb`](../../tests/python/notebooks/FLINK_DATA_TRACE_AND_INTEGRATION_TESTS.ipynb)
5. If any step is skipped, document the gap and risk in PR notes.

## Data Quality Contract

- Dedup:
  - primary key: `event.id` when present.
  - fallback: deterministic hash over normalized payload/dimensions.
  - state TTL: `QUALITY_DEDUP_TTL_MINUTES` (default `1440`).
  - duplicates are quarantined (`events.quarantine.streaming_events.v1`, `streaming_events_quarantine`).
- Validation:
  - schema/type/version checks emit DLQ envelopes (`events.dlq.streaming_events.v1`, `streaming_events_dlq`).
  - replay metadata (`__replay=true`) is preserved in failure envelopes.
- Pre-sink row guard:
  - `CLICKHOUSE_SINK_MAX_RECORD_BYTES` (default `1_000_000`) enforced before sink writes.
  - oversize typed rows emit `SINK_GUARD` DLQ failures.
  - oversize DLQ/quarantine envelopes are dropped to avoid recursive failure loops.

## Validation Artifacts

| Artifact | Path | Use |
|---|---|---|
| Metrics validation SQL | [`docs/reports/METRICS_VALIDATION_QUERIES.sql`](../reports/METRICS_VALIDATION_QUERIES.sql) | KPI/contract query pack |
| Ops validation SQL | [`docs/reports/OPS_ACTIVITY_VALIDATION_QUERIES.sql`](../reports/OPS_ACTIVITY_VALIDATION_QUERIES.sql) | Operational quality checks |
| Executive summary notebook | [`tests/python/notebooks/INTEGRATION_EXEC_SUMMARY.ipynb`](../../tests/python/notebooks/INTEGRATION_EXEC_SUMMARY.ipynb) | High-level PASS/FAIL review |
| End-to-end trace notebook | [`tests/python/notebooks/FLINK_DATA_TRACE_AND_INTEGRATION_TESTS.ipynb`](../../tests/python/notebooks/FLINK_DATA_TRACE_AND_INTEGRATION_TESTS.ipynb) | Scenario trace walkthroughs |

Policy:
- Keep `docs/reports/*` as dated evidence artifacts.
- Canonical testing/quality contracts live in `docs/*`; reports remain supporting evidence.

### Raw/Typed Validation Exception Boundary

- Default contract for serving/attribution validation is fact/agg/view based.
- [`tests/integration/sql/assertions_raw_typed.sql`](../../tests/integration/sql/assertions_raw_typed.sql) is an intentional exception used only for ingestion-lineage accounting (raw -> typed -> DLQ/quarantine).
- Do not reuse raw/typed joins from lineage assertions when validating canonical attribution, session identity, or API parity semantics.

## Notebook + Fixture Workflow

- Notebook environment:
  - [`tests/python/README.md`](../../tests/python/README.md)
- Production fixture export:
  - [`tests/python/scripts/export_scenario_fixtures.py`](../../tests/python/scripts/export_scenario_fixtures.py)
- Fixture load for test database:
  - [`tests/python/scripts/load_scenario_fixtures.py`](../../tests/python/scripts/load_scenario_fixtures.py)

### Raw-First Fixture Contract

- Fixture JSONL replay rows must come from canonical raw ingress only:
  - `__table = streaming_events`
- Scenario discovery still uses [`tests/integration/sql/scenario_candidates.sql`](../../tests/integration/sql/scenario_candidates.sql) against typed/fact tables.
- Capability context selection still uses typed capability snapshots to find relevant source ids, but replay payloads are fetched from raw `streaming_events(type='network_capabilities')`.
- [`tests/python/scripts/replay_scenario_events.py`](../../tests/python/scripts/replay_scenario_events.py) replays raw `streaming_events` rows only (no typed `network_capabilities` reconstruction path).

Recommended export command:

```bash
uv run --project tests/python python tests/python/scripts/export_scenario_fixtures.py \
  --host clickhouse.livepeer.cloud \
  --port 8123 \
  --database livepeer_analytics \
  --user analytics_user \
  --password analytics_password \
  --from-ts 2026-02-18T00:00:00Z \
  --to-ts 2026-02-25T23:59:59Z \
  --limit-per-scenario 3 \
  --capability-precede-session \
  --allow-missing-scenarios
```

### Scenario Candidate Discovery

- Notebook section:
  - [`tests/python/notebooks/FLINK_DATA_TRACE_AND_INTEGRATION_TESTS.ipynb`](../../tests/python/notebooks/FLINK_DATA_TRACE_AND_INTEGRATION_TESTS.ipynb) -> `Scenario Candidate Discovery`
- Required review in that section:
  - `scenario_3_success_with_swap` candidate counts (must be non-zero for blocking `assert_scenarios` runs).
  - `scenario_4_success_with_param_updates` candidate counts (track as availability signal; non-blocking until promoted to gate).
  - `scenario_5_out_of_category_baseline` candidate counts (sampled fallback/fallout population; non-blocking).
  - `scenario_5` should be interpreted as a sampled subset of notebook `fallout_df` sessions.
- Pre-run fixture gate:
  - `jq '.scenarios | {scenario_3:(.scenario_3_success_with_swap.sessions|length),scenario_4:(.scenario_4_success_with_param_updates.sessions|length),scenario_5:(.scenario_5_out_of_category_baseline.sessions|length)}' tests/integration/fixtures/manifest.json`
  - If scenario 3 is zero, refresh fixtures from production before running full scenario assertions.
- Additional fixture sanity gate (raw-first):
  - `rg --no-filename '"__table":"[^"]+"' tests/integration/fixtures/prod_snapshot_* | sort -u`
  - Expected replay rows should resolve to `streaming_events` only.

### Trace Pack Validation (Sections 06-09)

- In notebook trace-pack sections `06_rollup_population` through `09_sla_view_parity`, do not expect 1:1 counts vs sections `01-05`.
- Use grain-aligned reconciliation sourced from canonical assertions:
  - [`tests/integration/sql/assertions_pipeline.sql`](../../tests/integration/sql/assertions_pipeline.sql)
  - checks: `gpu_view_matches_rollup`, `network_demand_view_matches_rollup`, `sla_view_matches_session_fact`
- Treat notebook totals as diagnostics; treat assertion SQL parity results as contract verdicts.

## Run Notebook Against Scenario Test Output

Use this when you want notebook results to reflect the same local dataset produced by the scenario harness.

1. Run smoke/full scenario harness and note `run_id`:
   - `cd flink-jobs && mvn -Pscenario-it-smoke verify`
   - Example run artifact: `artifacts/test-runs/<run_id>/summary.json`

2. Keep/query local ClickHouse (not production):
   - `export CH_HOST=localhost`
   - `export CH_PORT=8123`
   - `export CH_DATABASE=livepeer_analytics`
   - `export CH_USER=analytics_user`
   - `export CH_PASSWORD=analytics_password`

3. Reuse the exact assertion window from the run artifacts (recommended):
   - `cat artifacts/test-runs/<run_id>/stages/assert_raw_typed.json`
   - copy `from_ts` and `to_ts`

4. Launch notebook with those env vars:
   - `uv run --project tests/python jupyter lab`
   - Optional startup defaults for display mode:
     - `export NB_SHOW_DEBUG=1`
     - `export NB_SHOW_0609_DEBUG=1`

5. In notebook:
   - restart kernel
   - run config cell first (it should show `CH_HOST=localhost`)
   - set `FROM_TS` / `TO_TS` to values from step 3
   - rerun trace and assertion cells

Notes:
- If notebook still shows old FAIL rows, outputs are stale; rerun cells or run all from top.
- Harness smoke defaults to `stack_down --volumes`, so persisted DB state is removed at end. For reproducible analysis, use run artifact windows and rerun assertions against current local stack.

### Notebook Display Modes

- Button-first controls (recommended):
  - Use top-of-notebook toggle buttons:
    - `Mode: Compact | Debug`
    - `06-09: Auto | Force Debug 06-09`
  - After changing a toggle, rerun the affected section cells.
- Optional startup defaults via env vars:
  - `NB_SHOW_DEBUG=0|1`
  - `NB_SHOW_0609_DEBUG=0|1`
  - Env vars only set initial toggle state; notebook buttons control mode during analysis.

## Quality Gate Signals to Watch

- `quality_gate.dlq`
- `quality_gate.dedup.duplicates`
- `quality_gate.sink_guard.oversize_drops`
- Quarantine and DLQ table growth rates in ClickHouse

## Deep References

- [`tests/python/notebooks/FLINK_DATA_TRACE_AND_INTEGRATION_TESTS.ipynb`](../../tests/python/notebooks/FLINK_DATA_TRACE_AND_INTEGRATION_TESTS.ipynb)
- [`tests/python/notebooks/INTEGRATION_EXEC_SUMMARY.ipynb`](../../tests/python/notebooks/INTEGRATION_EXEC_SUMMARY.ipynb)
- [`docs/quality/DATA_QUALITY.md`](./DATA_QUALITY.md)
- [`docs/reports/JAVA_CODEBASE_ASSESSMENT.md`](../reports/JAVA_CODEBASE_ASSESSMENT.md)
