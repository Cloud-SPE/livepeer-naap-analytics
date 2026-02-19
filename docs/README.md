# Documentation System of Record

This repository uses a map-first documentation model for human and agent collaboration:

- `AGENTS.md` is the fast entry point.
- `docs/` is the canonical knowledge base.

## Canonical Doc Set

- Architecture: `docs/architecture/SYSTEM_OVERVIEW.md`
- Data and metric contracts: `docs/data/SCHEMA_AND_METRIC_CONTRACTS.md`
- Operations and release: `docs/operations/RUNBOOKS_AND_RELEASE.md`
- Testing and validation: `docs/quality/TESTING_AND_VALIDATION.md`
- Engineering workflow: `docs/workflows/ENGINEERING_WORKFLOW.md`
- Doc inventory and migration map: `docs/references/DOC_INVENTORY_AND_MIGRATION_MAP.md`
- Consolidation review and next steps: `docs/references/DOC_CONSOLIDATION_PLAN.md`
- Canonical issues backlog: `docs/references/ISSUES_BACKLOG.md`
- Canonical glossary: `docs/references/GLOSSARY.md`
- Docs automation helpers: `docs/automation/DOCS_TOOLING.md`
- Scripts reference: `docs/automation/SCRIPTS_REFERENCE.md`

## Quick Start by Goal

| Goal | Read first | Then use |
|---|---|---|
| Understand system flow end to end | `docs/architecture/SYSTEM_OVERVIEW.md` | `docs/data/SCHEMA_AND_METRIC_CONTRACTS.md` |
| Change schema/metrics safely | `docs/data/SCHEMA_AND_METRIC_CONTRACTS.md` | `docs/quality/TESTING_AND_VALIDATION.md`, `docs/workflows/ENGINEERING_WORKFLOW.md` |
| Deploy/redeploy/replay jobs | `docs/operations/RUNBOOKS_AND_RELEASE.md` | `docs/operations/FLINK_DEPLOYMENT.md`, `docs/operations/REPLAY_RUNBOOK.md` |
| Investigate data quality issues | `docs/quality/TESTING_AND_VALIDATION.md` | `docs/quality/DATA_QUALITY.md` |
| Find unresolved improvements | `docs/references/ISSUES_BACKLOG.md` | `docs/references/METRICS_SCHEMA_DESIGN_SCRATCHPAD.md` (working context) |

## Working Rules

- Keep canonical docs concise and implementation-focused.
- Any schema or metric contract change must update both code and canonical docs in the same PR.
