# Design Documents Index

| Document | Status | Last verified | Description |
|----------|--------|--------------|-------------|
| [`core-beliefs.md`](core-beliefs.md) | ✅ current | 2026-03-24 | Project tenants and agent-first principles |
| [`architecture.md`](architecture.md) | ✅ current | 2026-03-24 | Layer rules, enforcement model, DI pattern |
| [`system-visuals.md`](system-visuals.md) | ✅ current | 2026-03-30 | Mermaid diagrams for ingest, resolver publication, and deployment topology |
| [`adr-001-storage-architecture.md`](adr-001-storage-architecture.md) | ✅ current | 2026-03-24 | ClickHouse + Kafka engine decision, compression, retention |
| [`adr-002-api-design.md`](adr-002-api-design.md) | ✅ current | 2026-03-24 | REST/JSON, auth model, org model, rate limiting |
| [`adr-003-tiered-serving-contract.md`](adr-003-tiered-serving-contract.md) | ✅ current | 2026-03-27 | Explicit analytics tier semantics, canonical derivation rules, and future consumer-surface governance |
| [`data-validation-rules.md`](data-validation-rules.md) | ✅ current | 2026-03-25 | Architecture-independent behavioral contract for all 17 data validation rules; maps 1:1 to the 31-test validation harness |

## Operations docs

| Document | Status | Last verified | Description |
|----------|--------|--------------|-------------|
| [`../operations/data-retention-policy.md`](../operations/data-retention-policy.md) | ✅ current | 2026-04-02 | Kafka topic retention windows, ClickHouse TTL inventory, replay strategy, and known gaps |

## Status legend

- ✅ current — reflects real code/decisions
- ⚠️ stale — review needed
- 🚧 draft — not yet authoritative
