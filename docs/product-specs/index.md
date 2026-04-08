# Product Specifications Index

All specifications are P0 / required for launch unless noted otherwise.

## API cross-cutting

| Spec | Req IDs | Status |
|------|---------|--------|
| [`api-overview.md`](api-overview.md) | API-001 – API-010 | approved |

## Feature specifications

| Spec | Req IDs | Status | Description |
|------|---------|--------|-------------|
| [`r1-network-state.md`](r1-network-state.md) | NET-001 – NET-012 | approved | Network summary, orch list, GPU inventory, model availability |
| [`r3-performance-quality.md`](r3-performance-quality.md) | PERF-001 – PERF-008 | approved | Inference FPS by model |

## Community documentation

| Document | Status | Description |
|----------|--------|-------------|
| [`../metrics-and-sla-reference.md`](../metrics-and-sla-reference.md) | Active | Community-facing metrics reference: formulas, SLA targets, scoring models, session taxonomy, glossary |

## Architecture decisions

| ADR | Status | Summary |
|-----|--------|---------|
| [`adr-001-storage-architecture.md`](../design-docs/adr-001-storage-architecture.md) | Accepted | ClickHouse + Kafka engine, no cache layer, LZ4/ZSTD compression, configurable TTL |
| [`adr-002-api-design.md`](../design-docs/adr-002-api-design.md) | Accepted | REST/JSON, open + IP rate-limited, org filter, RFC 7807 errors |

## Requirement traceability

| Req ID | Endpoint | Spec |
|--------|---------|------|
| NET-001 | `GET /v1/net/orchestrators` | r1-network-state.md |
| NET-002 | `GET /v1/net/models` | r1-network-state.md |
| NET-003 | `GET /v1/net/capacity` | r1-network-state.md |
| PERF-001 | `GET /v1/perf/stream/by-model` | r3-performance-quality.md |
