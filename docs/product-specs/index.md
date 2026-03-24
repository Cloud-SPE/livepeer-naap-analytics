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
| [`r2-stream-activity.md`](r2-stream-activity.md) | STR-001 – STR-010 | approved | Active streams, stream history, time-series |
| [`r3-performance-quality.md`](r3-performance-quality.md) | PERF-001 – PERF-008 | approved | Inference FPS, discovery latency, WebRTC quality |
| [`r4-payments-economics.md`](r4-payments-economics.md) | PAY-001 – PAY-008 | approved | Payment summary, history, by pipeline, by orch |
| [`r5-reliability-failures.md`](r5-reliability-failures.md) | REL-001 – REL-008 | approved | Failure rates, swap rates, failure log |
| [`r6-orch-leaderboard.md`](r6-orch-leaderboard.md) | LDR-001 – LDR-005 | approved | Public orch ranking, orch profile |

## Architecture decisions

| ADR | Status | Summary |
|-----|--------|---------|
| [`adr-001-storage-architecture.md`](../design-docs/adr-001-storage-architecture.md) | Accepted | ClickHouse + Kafka engine, no cache layer, LZ4/ZSTD compression, configurable TTL |
| [`adr-002-api-design.md`](../design-docs/adr-002-api-design.md) | Accepted | REST/JSON, open + IP rate-limited, org filter, RFC 7807 errors |

## Requirement traceability

| Req ID | Endpoint | Spec |
|--------|---------|------|
| NET-001 | `GET /v1/network/summary` | r1-network-state.md |
| NET-002 | `GET /v1/network/orchestrators` | r1-network-state.md |
| NET-003 | `GET /v1/network/gpus` | r1-network-state.md |
| NET-004 | `GET /v1/network/models` | r1-network-state.md |
| STR-001 | `GET /v1/streams/active` | r2-stream-activity.md |
| STR-002 | `GET /v1/streams/summary` | r2-stream-activity.md |
| STR-003 | `GET /v1/streams/history` | r2-stream-activity.md |
| PERF-001 | `GET /v1/performance/fps` | r3-performance-quality.md |
| PERF-002 | `GET /v1/performance/fps/history` | r3-performance-quality.md |
| PERF-003 | `GET /v1/performance/latency` | r3-performance-quality.md |
| PERF-004 | `GET /v1/performance/quality` | r3-performance-quality.md |
| PAY-001 | `GET /v1/payments/summary` | r4-payments-economics.md |
| PAY-002 | `GET /v1/payments/history` | r4-payments-economics.md |
| PAY-003 | `GET /v1/payments/by-pipeline` | r4-payments-economics.md |
| PAY-004 | `GET /v1/payments/by-orchestrator` | r4-payments-economics.md |
| REL-001 | `GET /v1/reliability/summary` | r5-reliability-failures.md |
| REL-002 | `GET /v1/reliability/history` | r5-reliability-failures.md |
| REL-003 | `GET /v1/reliability/orchestrators` | r5-reliability-failures.md |
| REL-004 | `GET /v1/reliability/failures` | r5-reliability-failures.md |
| LDR-001 | `GET /v1/leaderboard/orchestrators` | r6-orch-leaderboard.md |
| LDR-002 | `GET /v1/leaderboard/orchestrators/{address}` | r6-orch-leaderboard.md |
