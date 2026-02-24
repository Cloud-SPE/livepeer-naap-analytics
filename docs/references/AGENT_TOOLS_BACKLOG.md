# Agent Tools Backlog

Canonical backlog for agent-facing debugging tools that reduce repeated manual triage work across ClickHouse, Flink, and Kafka.

## Scope and Usage

- Focus on operator and debugging workflows, not product-facing features.
- Keep items implementation-oriented with clear inputs, outputs, and success criteria.
- When closing an item, link the implementation PR and any updated docs/scripts in this file.

## Prioritization Scale

- `P0`: required to unblock recurring incident/debug workflows.
- `P1`: high-value triage acceleration and reliability improvement.
- `P2`: useful enhancement; not required for immediate debugging throughput.
- `P3`: maintenance/documentation/process improvement.

## Canonical Backlog

| ID | Priority | Tool | Item | Why It Matters | Initial Deliverables |
|---|---|---|---|---|---|
| `ATOOL-001` | `P1` | `ch-debug` | Build a ClickHouse diagnostic entry point that accepts `event_id` and/or `stream_id` and returns a compact correlation report (raw event, typed projections, lifecycle facts, DLQ presence, freshness checks). | Collapses frequent multi-query investigation into one deterministic workflow and reduces miss-rate in manual triage. | New script (`scripts/ch_debug_report.py`), reusable SQL pack (`tests/integration/sql/debug_ch_event_trace.sql`), and short usage doc update. |
| `ATOOL-002` | `P1` | `pipeline-trace` | Add a stage-classification trace tool that identifies where a workflow disappeared (`ingest`, `parse`, `dedup`, `sink`, `mv`) using Kafka offsets, Flink indicators, and ClickHouse evidence. | Speeds root-cause isolation by reporting the first failing stage instead of raw, unstructured logs. | New script (`scripts/pipeline_trace.py`) with deterministic stage rubric and machine-readable JSON output. |
| `ATOOL-003` | `P1` | `flink-debug` | Add a Flink triage command that snapshots JobManager/TaskManager health, recent failures, restart/checkpoint counters, and bounded relevant logs in one run. | Standardizes Flink incident collection and avoids ad-hoc log scraping during active failures. | New script (`scripts/flink_debug_snapshot.sh` or `.py`) plus report artifact under `artifacts/debug-runs/<timestamp>/`. |
| `ATOOL-004` | `P2` | `kafka-debug` | Add Kafka inspection tooling for topic/partition lag, offset windows, and bounded message sampling for replay candidates. | Improves confidence in ingestion-path debugging and replay scoping without manual command assembly. | New script (`scripts/kafka_debug_snapshot.sh` or `.py`) and reusable output format for downstream tooling. |
| `ATOOL-005` | `P2` | `debug-agent` | Add a dedicated agent spec in `docs/agents/debug-agent.md` that orchestrates `ch-debug`, `pipeline-trace`, `flink-debug`, and `kafka-debug` with consistent guardrails. | Makes debugging behavior repeatable across humans/agents and reduces context-switch cost during incidents. | New agent doc with command catalog, escalation boundaries, and expected run artifact layout. |
| `ATOOL-006` | `P2` | Validation | Add smoke checks for each debug tool in CI or local validation script (argument validation, dry-run SQL parsing, expected report schema). | Prevents debug tool drift and ensures tools remain usable when schema/workflows evolve. | `scripts/validate_debug_tools.sh` (or Python equivalent) and docs note in testing references. |

## Notes

- Existing building blocks to reuse first: `scripts/run_clickhouse_query_pack.py`, `scripts/run_clickhouse_data_tests.py`, and `scripts/run_scenario_test_harness.py`.
- Prefer thin orchestration layers over duplicate query logic.
