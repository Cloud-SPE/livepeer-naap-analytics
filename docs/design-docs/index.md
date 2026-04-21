# Design Documents Index

| Document | Status | Last verified | Description |
|----------|--------|--------------|-------------|
| [`core-beliefs.md`](core-beliefs.md) | ✅ current | 2026-03-24 | Project tenants and agent-first principles |
| [`architecture.md`](architecture.md) | ✅ current | 2026-04-18 | Current Go layer rules, warehouse tier contract, and rollup-safety rules |
| [`api-table-contract.md`](api-table-contract.md) | ✅ current | 2026-04-18 | Three-families serving-layer contract (`api_hourly_*`, `api_current_*`, `api_fact_*`); endpoint-to-table mapping |
| [`attribution-rollup-coding-rules.md`](attribution-rollup-coding-rules.md) | ✅ current | 2026-04-10 | Merge-blocking rules for attribution authority, one-row-per-job facts, inflation safety, and dashboard math |
| [`selection-centered-attribution.md`](selection-centered-attribution.md) | ✅ current | 2026-04-18 | Attribution rules for V2V, AI batch, and BYOC; resolver-owned job-store patterns |
| [`system-visuals.md`](system-visuals.md) | ✅ current | 2026-04-18 | Mermaid diagrams for ingest, resolver publication, and deployment topology |
| [`adr-001-storage-architecture.md`](adr-001-storage-architecture.md) | ✅ current | 2026-03-24 | ClickHouse + Kafka engine decision, compression, retention |
| [`adr-002-api-design.md`](adr-002-api-design.md) | ✅ current | 2026-03-24 | REST/JSON, auth model, org model, rate limiting |
| [`adr-003-tiered-serving-contract.md`](adr-003-tiered-serving-contract.md) | ✅ current (amended) | 2026-04-18 | Tier semantics; amended by `api-table-contract.md` after Phase 5 retired the `api_base_*` tier |
| [`adr-004-quality-aware-sla.md`](adr-004-quality-aware-sla.md) | ✅ current | 2026-04-09 | Why `sla_score` is quality-aware, why benchmark-based cohort scoring is used, and what future changes require explicit signoff |
| [`data-validation-rules.md`](data-validation-rules.md) | ✅ current | 2026-03-25 | Architecture-independent behavioral contract for all 17 data validation rules; maps 1:1 to the 31-test validation harness |

## Community documentation

| Document | Status | Last verified | Description |
|----------|--------|--------------|-------------|
| [`../metrics-and-sla-reference.md`](../metrics-and-sla-reference.md) | ✅ current | 2026-04-02 | Community-facing metrics reference: formulas, SLA targets, scoring models, session taxonomy, glossary |
| [`../references/inbound-kafka-contract.md`](../references/inbound-kafka-contract.md) | ✅ current | 2026-04-09 | Repo-authoritative inbound Kafka event contract |

## Operations docs

| Document | Status | Last verified | Description |
|----------|--------|--------------|-------------|
| [`../operations/data-retention-policy.md`](../operations/data-retention-policy.md) | ✅ current | 2026-04-02 | Kafka topic retention windows, ClickHouse TTL inventory, replay strategy, and known gaps |
| [`../operations/devops-environment-guide.md`](../operations/devops-environment-guide.md) | ✅ current | 2026-04-02 | Monitoring, local and production environment setup, Kafka offset reset, ClickHouse reload and replay |
| [`../operations/infra-hardening-runbook.md`](../operations/infra-hardening-runbook.md) | ✅ current | 2026-04-02 | Security posture, Kafka listener architecture, open hardening action items (ports 9001, 8443, 8123) |
| [`../operations/operations-runbook.md`](../operations/operations-runbook.md) | ✅ current | 2026-04-02 | Deployment from scratch, alerting, troubleshooting, maintenance, and backups |
| [`../operations/incident-response.md`](../operations/incident-response.md) | ✅ current | 2026-04-02 | Severity definitions (P0–P3), response times, escalation contacts, post-mortem template |

## Status legend

- ✅ current — reflects real code/decisions
- ⚠️ stale — review needed
- 🚧 draft — not yet authoritative
