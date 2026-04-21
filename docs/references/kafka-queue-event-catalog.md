# Kafka Queue Event Catalog

Producer-side reference for Kafka events emitted by the upstream Go-livepeer
implementation through `monitor.SendQueueEventAsync`.

This file is retained because it helps explain how upstream producers shape the
messages this repo receives. It is not the consumer contract for this repo.
For the repo-authoritative inbound contract, see
[`inbound-kafka-contract.md`](inbound-kafka-contract.md).

## Common Envelope

Every queued event is wrapped in a common envelope before being written to Kafka:

- `id`: generated UUID for the message key
- `type`: top-level event family
- `timestamp`: enqueue time
- `gateway`: gateway address configured for the producer
- `data`: event payload

## Event Families

| Event type | Purpose | Key fields in `data` |
|---|---|---|
| `stream_trace` | Live stream lifecycle tracing | `type`, `stream_id`, `pipeline_id`, `request_id`, `orchestrator_info{address,url}`, optional `timestamp`, optional `pipeline`, optional `message` |
| `stream_ingest_metrics` | Periodic ingest quality metrics | `timestamp`, `stream_id`, `pipeline_id`, `request_id`, `stats` |
| `ai_stream_events` | Generic AI stream event channel | event-specific payload plus `stream_id`, `request_id`, `pipeline_id`, `orchestrator_info` |
| `ai_stream_status` | AI stream status snapshots | `type=status`, `inference_status`, plus `stream_id`, `request_id`, `pipeline_id`, `orchestrator_info` |
| `discovery_results` | Orchestrator discovery output | array of `{address, url, latency_ms}` |
| `create_new_payment` | Payment observability | `clientIP`, `requestID`, `capability`, `manifestID`, `sessionID`, `recipient`, `faceValue`, `winProb`, `price`, `numTickets`, `sender`, `orchestrator` |
| `network_capabilities` | Network capability snapshot | array of orchestrator capabilities and optional hardware metadata |
| `<dynamic queue_event_type>` | Forwarded custom event streams | arbitrary forwarded event shape with gateway-added context |

## `stream_trace` Subtypes

These are the upstream subtype names relevant to the current consumer contract:

| Subtype | Purpose |
|---|---|
| `gateway_receive_stream_request` | Stream request accepted at gateway |
| `gateway_ingest_stream_closed` | Ingest connection closed |
| `gateway_send_first_ingest_segment` | First segment sent from gateway |
| `gateway_receive_first_processed_segment` | First processed segment received |
| `gateway_receive_few_processed_segments` | Readiness signal after a few processed segments |
| `gateway_receive_first_data_segment` | First BYOC data-channel segment |
| `gateway_no_orchestrators_available` | Selection failed because no orchestrators were available |
| `orchestrator_swap` | Mid-stream orchestrator swap |
| `runner_receive_first_ingest_segment` | Runner observed first ingest segment |
| `runner_send_first_processed_segment` | Runner emitted first processed segment |

## Forwarding Notes

- Some upstream subscriptions can forward arbitrary custom `queue_event_type` values.
- This repo does not treat arbitrary forwarded families as a stable semantic contract.
- Unsupported or non-core families are accepted only if they match the current inbound contract; otherwise they are routed to `ignored_raw_events`.

## Upstream Ownership Note

The producer implementation lives outside this repository. File-level upstream
producer locations are therefore intentionally not maintained here.
