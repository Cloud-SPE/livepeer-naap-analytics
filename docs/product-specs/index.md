# Product Specifications Index

All active specs in this directory are expected to match the live OpenAPI
surface.

## API Cross-Cutting

| Spec | Req IDs | Status |
|------|---------|--------|
| [`api-overview.md`](api-overview.md) | API-001 – API-010 | approved |

## Feature Specifications

| Spec | Req IDs | Status | Description |
|------|---------|--------|-------------|
| [`r1-network-state.md`](r1-network-state.md) | NET-001 – NET-003 | approved | Current orchestrator, model, request-supply, and discover inventory |
| [`r3-performance-quality.md`](r3-performance-quality.md) | PERF-001 – PERF-003 | approved | Streaming performance, SLA, demand, and GPU quality surfaces |

## Community Documentation

| Document | Status | Description |
|----------|--------|-------------|
| [`../metrics-and-sla-reference.md`](../metrics-and-sla-reference.md) | Active | Community-facing metrics, SLA targets, scoring models, and glossary |
| [`../references/performance-quality-reference.md`](../references/performance-quality-reference.md) | Active | Current performance-route semantics and lower-layer implementation notes |

## Architecture Decisions

| ADR | Status | Summary |
|-----|--------|---------|
| [`../design-docs/adr-001-storage-architecture.md`](../design-docs/adr-001-storage-architecture.md) | Accepted | ClickHouse + Kafka engine, no cache layer, configurable retention |
| [`../design-docs/adr-002-api-design.md`](../design-docs/adr-002-api-design.md) | Accepted | REST/JSON, open + rate-limited access, RFC 7807 errors |

## Requirement Traceability

| Req ID | Endpoint | Spec |
|--------|----------|------|
| NET-001 | `GET /v1/dashboard/orchestrators` | `r1-network-state.md` |
| NET-002 | `GET /v1/streaming/models` | `r1-network-state.md` |
| NET-003 | `GET /v1/requests/orchestrators` | `r1-network-state.md` |
| PERF-001 | `GET /v1/streaming/models` | `r3-performance-quality.md` |
| PERF-002 | `GET /v1/streaming/sla` | `r3-performance-quality.md` |
| PERF-003 | `GET /v1/streaming/demand` | `r3-performance-quality.md` |
