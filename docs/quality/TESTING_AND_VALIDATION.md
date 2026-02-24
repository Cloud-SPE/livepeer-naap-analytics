# Testing and Validation

## Quality Layers

1. Flink unit and contract tests (`flink-jobs/src/test/java`)
2. Schema sync and parser guardrails
3. ClickHouse integration SQL assertions (`tests/integration/sql`)
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
- Query trace pack:
  - `uv run --project tools/python python scripts/run_clickhouse_query_pack.py --lookback-hours 24`
- Pipeline assertions:
  - `uv run --project tools/python python scripts/run_clickhouse_data_tests.py --sql-file tests/integration/sql/assertions_raw_typed.sql --lookback-hours 24`
  - `uv run --project tools/python python scripts/run_clickhouse_data_tests.py --sql-file tests/integration/sql/assertions_pipeline.sql --lookback-hours 24`
- Scenario assertions:
  - `uv run --project tools/python python scripts/run_clickhouse_data_tests.py --sql-file tests/integration/sql/assertions_scenario_candidates.sql --lookback-hours 720`
  - Note: `scenario_4_success_with_param_updates_exists` is currently informational (non-blocking) until param-update source rows are present in production.
- One-shot integration run:
  - `tests/integration/run_all.sh`

## Pipeline Assertions Requirement Map

Reference SQL: `tests/integration/sql/assertions_pipeline.sql`

Use this map to understand why each assertion exists and where to look first when it fails.
Maintenance rule: keep this table in sync with `tests/integration/sql/assertions_pipeline.sql`.
Any add/rename/remove of a `-- TEST:` block must co-update this section in the same change.

| Assertion (`-- TEST:`) | Requirement Enforced | First Triage Check |
|---|---|---|
| `raw_events_present` | Core raw/typed ingress objects have data in-window. | Confirm assertion `from_ts`/`to_ts` matches replay window and fixture manifest window. |
| `capability_dimension_mvs_present` | Required capability MVs exist in ClickHouse. | Check `system.tables` for missing MV names and confirm schema init was applied. |
| `capability_dimensions_projecting` | Capability source tables project into dimension snapshot tables. | Compare row counts between `network_capabilities*` and `dim_orchestrator_capability_*` tables in-window. |
| `session_fact_present` | Sessionization emits `fact_workflow_sessions` rows. | Confirm lifecycle operators are running and replay produced stream trace/status rows. |
| `network_capabilities_raw_and_typed_present` | Capabilities are present both raw and typed for attribution windows. | Check `streaming_events(type='network_capabilities')` vs `network_capabilities` parse output. |
| `status_raw_to_silver_projection` | Typed status rows are losslessly projected to silver status fact. | Join typed/silver by `cityHash64(raw_json)` source UID and inspect missing rows. |
| `trace_raw_to_silver_projection` | Typed trace rows are losslessly projected to silver trace edges. | Join typed/silver by `cityHash64(raw_json)` source UID and inspect missing rows. |
| `ingest_raw_to_silver_projection` | Typed ingest rows are losslessly projected to ingest silver fact. | Verify `stream_ingest_metrics` exists in fixtures; if present, check projection UID join. |
| `session_final_uniqueness` | Latest-version session rows are unique per `workflow_session_id` under `FINAL`. | Inspect duplicate latest `version` rows in `fact_workflow_sessions`. |
| `workflow_session_has_identifier` | Session rows always carry non-empty `workflow_session_id`. | Inspect lifecycle signal key construction (`stream_id|request_id`) for blanks. |
| `swap_signal_split_consistency` | Legacy `swap_count` must equal confirmed swap count (`confirmed_swap_count`). | Validate session fact emission contract in Flink state machine and mapper fields. |
| `gold_sessions_use_canonical_orchestrator_identity` | Gold/session orchestrator identity is canonical (not hot-wallet/local). | Compare session `orchestrator_address` to canonical `network_capabilities.orchestrator_address` in-window. |
| `swapped_sessions_have_evidence` | `swap_count > 0` (confirmed swaps) has supporting explicit swap evidence from trace/segments. | Check swap evidence via both `fact_stream_trace_edges` and `stream_trace_events` by `(stream_id,request_id)`. |
| `param_updates_reference_existing_session` | Param-update facts must reference a known session in-window. | Left join `fact_workflow_param_updates` to latest sessions and inspect orphan IDs. |
| `lifecycle_session_pipeline_model_compatible` | Session `pipeline` and `model_id` remain contract-compatible when both set. | Inspect attribution selection and model labeling paths for mismatched values. |
| `gpu_view_matches_rollup` | GPU API view numerically matches rollup aggregate source. | Recompute rollup grain join on `(window, orch, pipeline, model, gpu, region)`. |
| `sla_view_matches_session_fact` | SLA API counts match independent recomputation from session facts. | Compare known/unexcused/swapped counts per hour+dimension between raw and API view. |
| `sla_ratios_in_bounds` | SLA ratios are bounded to valid probability range `[0,1]`. | Inspect denominator/guard conditions in `v_api_sla_compliance`. |

## Scenario Harness Stages

The harness is stage-based and writes a report + logs for every run.
Default behavior is full-stack replay via Kafka -> Flink -> ClickHouse assertions
(not direct ClickHouse fixture insertion).

- Script:
  - `scripts/run_scenario_test_harness.py`
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
  - `python scripts/run_scenario_test_harness.py --stage assert_pipeline`
- Keep stack running after failure:
  - `python scripts/run_scenario_test_harness.py --mode full --keep-stack-on-fail`

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
   - `uv run --project tools/python python scripts/run_clickhouse_query_pack.py --lookback-hours 24`

## Contract Drift Checklist (Generalized)

Apply this checklist to any refactor/change that can affect schema, lifecycle semantics, API views, assertions, or notebook diagnostics:

1. Keep implementation and contract docs in the same change:
   - `configs/clickhouse-init/01-schema.sql` / `flink-jobs/*`
   - `docs/data/SCHEMA_AND_METRIC_CONTRACTS.md`
2. Keep assertions synchronized with requirement map:
   - `tests/integration/sql/assertions_pipeline.sql`
   - `docs/quality/TESTING_AND_VALIDATION.md` (Pipeline Assertions Requirement Map)
3. Run the minimum validation gate:
   - `cd flink-jobs && mvn test`
   - `tests/integration/run_all.sh`
   - `uv run --project tools/python python scripts/run_clickhouse_query_pack.py --lookback-hours 24`
4. If semantics or output fields changed, re-check notebook cells and saved outputs:
   - `docs/reports/notebook/FLINK_DATA_TRACE_AND_INTEGRATION_TESTS.ipynb`
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
| Metrics validation SQL | `docs/reports/METRICS_VALIDATION_QUERIES.sql` | KPI/contract query pack |
| Ops validation SQL | `docs/reports/OPS_ACTIVITY_VALIDATION_QUERIES.sql` | Operational quality checks |
| End-to-end trace notebook | `docs/reports/notebook/FLINK_DATA_TRACE_AND_INTEGRATION_TESTS.ipynb` | Scenario trace walkthroughs |
| JSONL validation input | `scripts/livepeer_samples.jsonl` | Replayable sample event inputs |

Policy:
- Keep `docs/reports/*` as dated evidence artifacts.
- Canonical testing/quality contracts live in `docs/*`; reports remain supporting evidence.

## Notebook + Fixture Workflow

- Notebook environment:
  - `tools/python/README.md`
- Production fixture export:
  - `scripts/export_scenario_fixtures.py`
- Fixture load for test database:
  - `scripts/load_scenario_fixtures.py`

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
   - `uv run --project tools/python jupyter lab`

5. In notebook:
   - restart kernel
   - run config cell first (it should show `CH_HOST=localhost`)
   - set `FROM_TS` / `TO_TS` to values from step 3
   - rerun trace and assertion cells

Notes:
- If notebook still shows old FAIL rows, outputs are stale; rerun cells or run all from top.
- Harness smoke defaults to `stack_down --volumes`, so persisted DB state is removed at end. For reproducible analysis, use run artifact windows and rerun assertions against current local stack.

## Quality Gate Signals to Watch

- `quality_gate.dlq`
- `quality_gate.dedup.duplicates`
- `quality_gate.sink_guard.oversize_drops`
- Quarantine and DLQ table growth rates in ClickHouse

## Deep References

- `docs/reports/notebook/FLINK_DATA_TRACE_AND_INTEGRATION_TESTS.ipynb`
- `docs/quality/DATA_QUALITY.md`
- `docs/reports/JAVA_CODEBASE_ASSESSMENT.md`
