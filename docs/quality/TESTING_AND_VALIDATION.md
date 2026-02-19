# Testing and Validation

## Quality Layers

1. Flink unit and contract tests (`flink-jobs/src/test/java`)
2. Schema sync and parser guardrails
3. ClickHouse integration SQL assertions (`tests/integration/sql`)
4. Replay and trace query-pack validation
5. Notebook-assisted investigation and fixture export/load

## Core Commands

- Java tests:
  - `cd flink-jobs && mvn test`
- Query trace pack:
  - `uv run --project tools/python python scripts/run_clickhouse_query_pack.py --lookback-hours 24`
- Pipeline assertions:
  - `uv run --project tools/python python scripts/run_clickhouse_data_tests.py --sql-file tests/integration/sql/assertions_pipeline.sql --lookback-hours 24`
- Scenario assertions:
  - `uv run --project tools/python python scripts/run_clickhouse_data_tests.py --sql-file tests/integration/sql/assertions_scenario_candidates.sql --lookback-hours 720`
- One-shot integration run:
  - `tests/integration/run_all.sh`

## Contract-Critical Tests

- `ClickHouseSchemaSyncTest`: mappers and schema stay aligned.
- `EventParsersTest`: typed extraction behavior.
- Quality gate tests: dedup, validation, and routing semantics.
- Lifecycle state machine tests: session/segment/latency derivation invariants.

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

## Quality Gate Signals to Watch

- `quality_gate.dlq`
- `quality_gate.dedup.duplicates`
- `quality_gate.sink_guard.oversize_drops`
- Quarantine and DLQ table growth rates in ClickHouse

## Deep References

- `docs/reports/notebook/FLINK_DATA_TRACE_AND_INTEGRATION_TESTS.ipynb`
- `docs/quality/DATA_QUALITY.md`
- `docs/reports/JAVA_CODEBASE_ASSESSMENT.md`
