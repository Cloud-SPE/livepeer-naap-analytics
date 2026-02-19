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
| `BACKLOG-002` | `P0` | Time semantics | Harden timestamp-domain policy for cross-component latency and edge ordering; expose dual-time semantics where required. | Prevents invalid mixed-domain arithmetic and misleading latency conclusions. | `docs/data/SCHEMA_AND_METRIC_CONTRACTS.md`, `docs/architecture/SYSTEM_OVERVIEW.md`, `docs/references/METRICS_SCHEMA_DESIGN_SCRATCHPAD.md` |
| `BACKLOG-003` | `P1` | SLA contract | Lock SLA scoring contract: numerators/denominators, window policy, thresholds, and minimum coverage validity rules. | Ensures SLA scores are stable, comparable, and audit-ready. | `docs/data/SCHEMA_AND_METRIC_CONTRACTS.md`, `docs/architecture/SYSTEM_OVERVIEW.md`, `docs/references/METRICS_SCHEMA_DESIGN_SCRATCHPAD.md` |
| `BACKLOG-004` | `P1` | Lifecycle observability | Add Flink lifecycle counters/gauges for matching quality, out-of-order signals, and coverage emission quality. | Improves operational confidence and shortens incident triage. | `docs/data/SCHEMA_AND_METRIC_CONTRACTS.md`, `docs/references/METRICS_SCHEMA_DESIGN_SCRATCHPAD.md` |
| `BACKLOG-005` | `P1` | Data quality gates | Define and enforce release-gate thresholds for unmatched edges, freshness lag, and rollup parity; lock alert execution path. | Makes readiness checks objective and automatable. | `docs/data/SCHEMA_AND_METRIC_CONTRACTS.md`, `docs/references/METRICS_SCHEMA_DESIGN_SCRATCHPAD.md` |
| `BACKLOG-006` | `P1` | Ingest metrics | Add ingest-leg rollups (jitter/latency/bytes) after finalizing API contract fields. | Closes visibility gap between startup path and ingest transport behavior. | `docs/data/SCHEMA_AND_METRIC_CONTRACTS.md`, `docs/references/METRICS_SCHEMA_DESIGN_SCRATCHPAD.md` |
| `BACKLOG-007` | `P2` | Serving model | Add metadata-only GPU inventory serving view (`v_api_gpu_inventory_current`). | Improves inventory visibility without expensive repeated joins. | `docs/data/SCHEMA_AND_METRIC_CONTRACTS.md`, `docs/references/METRICS_SCHEMA_DESIGN_SCRATCHPAD.md` |
| `BACKLOG-008` | `P2` | Serving model | Add optional enriched convenience view (`v_api_gpu_metrics_enriched`). | Simplifies consumer queries when joined metadata is required frequently. | `docs/data/SCHEMA_AND_METRIC_CONTRACTS.md`, `docs/references/METRICS_SCHEMA_DESIGN_SCRATCHPAD.md` |
| `BACKLOG-009` | `P2` | Serving model | Add model-grain demand view (`v_api_network_demand_by_model`) distinct from pipeline-grain demand. | Improves model-capacity planning without overloading current demand contract. | `docs/data/SCHEMA_AND_METRIC_CONTRACTS.md`, `docs/references/METRICS_SCHEMA_DESIGN_SCRATCHPAD.md` |
| `BACKLOG-010` | `P2` | Lineage | Preserve/expand per-session canonical orchestrator lineage queryability in lifecycle facts. | Supports swap debugging, attribution audits, and historical forensics. | `docs/data/SCHEMA_AND_METRIC_CONTRACTS.md`, `docs/references/METRICS_SCHEMA_DESIGN_SCRATCHPAD.md` |
| `BACKLOG-011` | `P2` | Flink maintainability | Refactor pipeline assembly into focused topology builders while preserving behavior. | Reduces regression risk and improves reviewability of pipeline changes. | `docs/reports/JAVA_CODEBASE_ASSESSMENT.md` |
| `BACKLOG-012` | `P2` | Flink maintainability | Split monolithic sink mapper surface into domain-focused mapper modules. | Lowers schema-drift risk and makes mapping evolution safer. | `docs/reports/JAVA_CODEBASE_ASSESSMENT.md` |
| `BACKLOG-013` | `P1` | Testing | Add integration tests for latency derivation and demand attribution invariants. | Improves end-to-end confidence for API-facing facts and rollups. | `docs/reports/JAVA_CODEBASE_ASSESSMENT.md` |
| `BACKLOG-014` | `P2` | Contract docs | Add semantic Javadocs for API-facing KPI fields and attribution fields (units/nullability/invariants). | Reduces implementation ambiguity and onboarding cost. | `docs/reports/JAVA_CODEBASE_ASSESSMENT.md` |
| `BACKLOG-015` | `P3` | Docs automation | Add CI checks for markdown linting and local-link validation. | Prevents doc drift and broken navigation. | `docs/references/DOC_CONSOLIDATION_PLAN.md` |
| `BACKLOG-016` | `P3` | Docs governance | Add freshness metadata (owner + last-reviewed date) to canonical docs. | Clarifies ownership and review cadence. | `docs/references/DOC_CONSOLIDATION_PLAN.md` |
| `BACKLOG-017` | `P3` | PR guardrails | Add automated check that schema changes co-update mappers, tests, and canonical docs in one PR. | Enforces contract discipline and reduces partial changes. | `docs/references/DOC_CONSOLIDATION_PLAN.md` |

## Notes

- This file supersedes scattered backlog sections in working/reference docs.
- Detailed contract semantics remain in `docs/data/SCHEMA_AND_METRIC_CONTRACTS.md`.
