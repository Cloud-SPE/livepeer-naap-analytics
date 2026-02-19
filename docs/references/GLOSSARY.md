# Glossary

Canonical terminology for consistent communication across architecture, schema, operations, and testing docs.

## Core Data Model Terms

| Term | Definition |
|---|---|
| Raw tables | Directly ingested event surfaces with minimal reshaping (for replay/audit), such as `streaming_events` and DLQ/quarantine tables. |
| Typed tables | Parsed event-family tables with explicit columns (for example `ai_stream_status`, `stream_trace_events`, `network_capabilities`). |
| Fact tables (`fact_*`) | Curated analytical tables at explicit business grain. |
| Rollups (`agg_*`) | Pre-aggregated tables optimized for common read patterns and time windows. |
| API views (`v_api_*`) | Stable serving interfaces consumed by APIs and dashboards. |
| Snapshot dimensions (`*_snapshots`) | Historical enrichment state used for event-time correctness. |
| Current dimensions (`*_current`) | Latest-state projections used for present-time inventory/labels. |

## Identity and Join Terms

| Term | Definition |
|---|---|
| Canonical wallet | Official orchestrator identity (`orchestrator_address`) used in silver/gold/API layers. |
| Local/proxy wallet | Non-canonical wallet identity (`local_address`) kept for traceability in capability data. |
| Wallet normalization | Mapping local/proxy wallet values to canonical orchestrator wallet before downstream joins/persistence. |
| Raw-to-downstream join | Join path that maps raw wallet values through `network_capabilities` to canonical wallet identity first. |

## Lifecycle and Session Terms

| Term | Definition |
|---|---|
| Workflow session | Deterministic lifecycle unit used for startup/swap/failure semantics. |
| `workflow_session_id` | Canonical session key composed from `stream_id` and `request_id` with deterministic fallback rules. |
| Known session | Session included in startup outcome denominator. |
| Startup success | Session with playable startup signal observed in lifecycle edges. |
| Excused failure | Startup failure matching no-orchestrator signal or approved excusable error taxonomy. |
| Unexcused failure | Known startup failure not classified as success or excused. |
| Swap | Session with explicit swap edge or multi-orchestrator lineage in one session. |
| Session segment | Contiguous portion of a session bounded by orchestrator identity changes. |

## Quality and Replay Terms

| Term | Definition |
|---|---|
| Quality gate | Flink stage that validates schema/version, derives dedup key, and routes accepted vs failed events. |
| DLQ | Dead letter queue for schema/validation failures (`events.dlq.streaming_events.v1`). |
| Quarantine | Stream for duplicates/expected rejects (`events.quarantine.streaming_events.v1`). |
| Dedup TTL | State retention window (`QUALITY_DEDUP_TTL_MINUTES`) used to suppress duplicates safely. |
| Replay window | Bounded time range (`REPLAY_START_EPOCH_MS` to `REPLAY_END_EPOCH_MS`) replayed from DLQ to input topic. |
| Sink guard | Pre-sink record-size guard preventing oversize writes and recursive DLQ failures. |

## Metrics and Serving Terms

| Term | Definition |
|---|---|
| Additive fields | Numerators/counts/sums that can be safely re-aggregated across larger windows. |
| Derived fields | Ratios/scores recomputed from additive fields at query/serving time. |
| SLA score | Compliance composite derived from declared numerator/denominator contract and thresholds. |
| Attribution method | Enum describing how model/GPU attribution was assigned for a session/sample. |
| Attribution confidence | Confidence indicator paired with attribution method to expose enrichment certainty. |

## Runtime and Infrastructure Terms

| Term | Definition |
|---|---|
| Kafka Connect fan-out | Dual raw sinks from Kafka to ClickHouse raw ingest and MinIO object archive. |
| Savepoint | Flink state snapshot used for safe stop/restart and versioned upgrades. |
| Compose replay profile | Docker Compose profile that runs replay submitter components for bounded DLQ replay. |
