# Issues Backlog

Canonical backlog for unresolved feature ideas, design hardening, and engineering improvements.

## Scope and Usage

- Source of truth for open issues discovered in architecture, data contract, quality, and consolidation docs.
- Keep items implementation-oriented and traceable to source docs.
- When closing an item, update this file and the linked canonical doc in the same PR.

## Prioritization Scale

- `P0`: blocks correctness or release readiness.
- `P1`: high-value reliability or contract hardening.
- `P2`: useful enhancement; not release-blocking.
- `P3`: maintenance/documentation/process improvement.

## Canonical Backlog

| ID | Priority | Area | Item | Why It Matters | Source |
|---|---|---|---|---|---|
| `BACKLOG-001` | `P0` | Metrics telemetry | Add true orchestrator transport bandwidth telemetry path (up/down), then model it in facts/rollups. | Current KPI is blocked; capacity and network analysis remain incomplete. | `docs/data/SCHEMA_AND_METRIC_CONTRACTS.md`, `docs/architecture/SYSTEM_OVERVIEW.md`, `docs/references/METRICS_SCHEMA_DESIGN_SCRATCHPAD.md` |
| `BACKLOG-002` | `P0` | Time semantics | Harden timestamp-domain policy for cross-component latency and edge ordering, explicitly covering (a) runner vs gateway clock drift/skew and (b) late/out-of-order gateway trickle delivery; expose dual-time semantics where required. | Prevents invalid mixed-domain arithmetic, false edge ordering, and misleading latency conclusions under skewed clocks or delayed event arrival. | `docs/data/SCHEMA_AND_METRIC_CONTRACTS.md`, `docs/architecture/SYSTEM_OVERVIEW.md`, `docs/references/METRICS_SCHEMA_DESIGN_SCRATCHPAD.md` |
| `BACKLOG-003` | `P1` | SLA contract | Lock SLA scoring contract: numerators/denominators, window policy, thresholds, and minimum coverage validity rules. | Ensures SLA scores are stable, comparable, and audit-ready. | `docs/data/SCHEMA_AND_METRIC_CONTRACTS.md`, `docs/architecture/SYSTEM_OVERVIEW.md`, `docs/references/METRICS_SCHEMA_DESIGN_SCRATCHPAD.md` |
| `BACKLOG-004` | `P1` | Lifecycle observability | Add Flink lifecycle counters/gauges for matching quality, out-of-order signals, and coverage emission quality. | Improves operational confidence and shortens incident triage. | `docs/data/SCHEMA_AND_METRIC_CONTRACTS.md`, `docs/references/METRICS_SCHEMA_DESIGN_SCRATCHPAD.md` |
| `BACKLOG-005` | `P1` | Data quality gates | Define and enforce release-gate thresholds for unmatched edges, freshness lag, and rollup parity; lock alert execution path. | Makes readiness checks objective and automatable. | `docs/data/SCHEMA_AND_METRIC_CONTRACTS.md`, `docs/references/METRICS_SCHEMA_DESIGN_SCRATCHPAD.md` |
| `BACKLOG-006` | `P1` | Ingest metrics | Add ingest-leg rollups (jitter/latency/bytes) after finalizing API contract fields. | Closes visibility gap between startup path and ingest transport behavior. | `docs/data/SCHEMA_AND_METRIC_CONTRACTS.md`, `docs/references/METRICS_SCHEMA_DESIGN_SCRATCHPAD.md` |
| `BACKLOG-007` | `P2` | Serving model | Add metadata-only GPU inventory serving view (`v_api_gpu_inventory_current`). | Improves inventory visibility without expensive repeated joins. | `docs/data/SCHEMA_AND_METRIC_CONTRACTS.md`, `docs/references/METRICS_SCHEMA_DESIGN_SCRATCHPAD.md` |
| `BACKLOG-008` | `P2` | Serving model | Add optional enriched convenience view (`v_api_gpu_metrics_enriched`). | Simplifies consumer queries when joined metadata is required frequently. | `docs/data/SCHEMA_AND_METRIC_CONTRACTS.md`, `docs/references/METRICS_SCHEMA_DESIGN_SCRATCHPAD.md` |
| `BACKLOG-009` | `P2` | Serving model | Add model-grain demand view (`v_api_network_demand_by_model`) distinct from pipeline-grain demand. | Improves model-capacity planning without overloading current demand contract. | `docs/data/SCHEMA_AND_METRIC_CONTRACTS.md`, `docs/references/METRICS_SCHEMA_DESIGN_SCRATCHPAD.md` |
| `BACKLOG-010` | `P2` | Lineage | Preserve and harden per-session canonical orchestrator lineage queryability by adding explicit swap lineage fields and validation checks (including previous/next orchestrator transitions) in lifecycle facts/views. | Supports deterministic swap forensics and reduces ambiguity in multi-hop session transitions now that baseline session identity exists. | `docs/data/SCHEMA_AND_METRIC_CONTRACTS.md`, `docs/references/METRICS_SCHEMA_DESIGN_SCRATCHPAD.md` |
| `BACKLOG-011` | `P2` | Flink maintainability | Refactor pipeline assembly into focused topology builders while preserving behavior. | Reduces regression risk and improves reviewability of pipeline changes. | `docs/reports/JAVA_CODEBASE_ASSESSMENT.md` |
| `BACKLOG-012` | `P2` | Flink maintainability | Split monolithic sink mapper surface into domain-focused mapper modules. | Lowers schema-drift risk and makes mapping evolution safer. | `docs/reports/JAVA_CODEBASE_ASSESSMENT.md` |
| `BACKLOG-013` | `P1` | Testing | Complete integration enforcement for latency and demand-attribution invariants by running API-readiness assertions in the default integration harness and adding explicit failure thresholds for drift/parity. | Some invariants exist, but they are not fully enforced in the primary run path, leaving regression risk for API-facing metrics. | `docs/reports/JAVA_CODEBASE_ASSESSMENT.md`, `docs/quality/TESTING_AND_VALIDATION.md` |
| `BACKLOG-014` | `P2` | Contract docs | Add semantic Javadocs for API-facing KPI fields and attribution fields (units/nullability/invariants). | Reduces implementation ambiguity and onboarding cost. | `docs/reports/JAVA_CODEBASE_ASSESSMENT.md` |
| `BACKLOG-015` | `P3` | Docs automation | Add CI checks for markdown linting and local-link validation. | Prevents doc drift and broken navigation. | `docs/references/DOC_CONSOLIDATION_PLAN.md` |
| `BACKLOG-016` | `P3` | Docs governance | Add freshness metadata (owner + last-reviewed date) to canonical docs. | Clarifies ownership and review cadence. | `docs/references/DOC_CONSOLIDATION_PLAN.md` |
| `BACKLOG-017` | `P3` | PR guardrails | Add automated check that schema changes co-update mappers, tests, and canonical docs in one PR. | Enforces contract discipline and reduces partial changes. | `docs/references/DOC_CONSOLIDATION_PLAN.md` |
| `BACKLOG-018` | `P1` | Flink observability | Configure and validate a production metrics reporter (for example Prometheus/JMX) so JobManager/TaskManager and pipeline metrics are exported. | Prevents blind spots during incident response and enables SLO-backed alerting. | `docs/operations/RUNBOOKS_AND_RELEASE.md`, `docs/quality/TESTING_AND_VALIDATION.md` |
| `BACKLOG-019` | `P2` | Flink config hygiene | Replace deprecated Flink keys and submission paths (`restart-strategy` -> `restart-strategy.type`, REST query-param submission -> JSON body flow). | Reduces upgrade risk and avoids behavior drift on future Flink releases. | `docs/operations/RUNBOOKS_AND_RELEASE.md`, `docs/workflows/ENGINEERING_WORKFLOW.md` |
| `BACKLOG-020` | `P1` | Flink serialization | Remove `GenericType` fallbacks in hot/stateful paths by introducing explicit Flink type information/serializers for warned fields (for example `KafkaInboundRecord#headers`, `CapabilitySnapshotBucket#byModelKey`). | Improves runtime performance and state schema evolution safety. | `docs/quality/TESTING_AND_VALIDATION.md`, `docs/workflows/ENGINEERING_WORKFLOW.md` |
| `BACKLOG-021` | `P2` | Namespace consistency | Replace `com.livepeer` package names across code and configs with the chosen canonical namespace, including build/runtime references. | Reduces coupling to legacy naming, avoids config/package drift, and simplifies future refactors/releases. | `docs/workflows/ENGINEERING_WORKFLOW.md`, `docs/references/ISSUES_BACKLOG.md` |

## Notes

- This file supersedes scattered backlog sections in working/reference docs.
- Detailed contract semantics remain in `docs/data/SCHEMA_AND_METRIC_CONTRACTS.md`.
