# Kafka Queue Event Catalog (`monitor.SendQueueEventAsync`)

## Scope
This document catalogs all Kafka events emitted through `monitor.SendQueueEventAsync()` in this repository.

## Common Kafka Envelope
Every queued event is wrapped in a common envelope before being written to Kafka:

- `id` (string): generated UUID for the message key.
- `type` (string): top-level event type (for example `stream_trace`).
- `timestamp` (string): enqueue time in Unix milliseconds.
- `gateway` (string): gateway address configured for the Kafka producer.
- `data` (object/array): event payload described below.

## Event Types

| Event type | Purpose | Definition | Key fields in `data` | Source |
|---|---|---|---|---|
| `stream_trace` | Live-stream lifecycle tracing | Gateway trace events for ingest/processing/orchestrator lifecycle | `type`, `stream_id`, `pipeline_id`, `request_id`, `orchestrator_info{address,url}`, optional `timestamp`, optional `pipeline`, optional `message` | `gateway` |
| `stream_ingest_metrics` | Periodic ingest quality metrics | WHIP ingest stats snapshots emitted every 5s while stream is active | `timestamp`, `stream_id`, `pipeline_id`, `request_id`, `stats` | `gateway` |
| `ai_stream_events` | Generic AI stream event channel | Mix of gateway-authored events and forwarded event payloads from orchestrator/runner | Gateway-authored examples: `type=params_update` with `params`; `type=error` with `message`. Common context fields are added: `stream_id`, `request_id`, `pipeline_id`, `orchestrator_info` | `gateway`, `orchestrator`, `runner` |
| `ai_stream_status` | AI stream status snapshots | Special routing of forwarded events where `event.type == "status"` | `type=status`, `inference_status` (runner status object), plus added context fields: `stream_id`, `request_id`, `pipeline_id`, `orchestrator_info` | `runner` (forwarded via orchestrator/gateway) |
| `discovery_results` | Orchestrator discovery visibility | Discovery output list for selected orchestrators | Array of `{address, url, latency_ms}` | `gateway` |
| `create_new_payment` | Payment observability | Ticket batch/payment creation metadata | `clientIP`, `requestID`, `capability`, `manifestID`, `sessionID`, `recipient`, `faceValue`, `winProb`, `price`, `numTickets`, `sender`, `orchestrator` | `gateway` |
| `network_capabilities` | Network capability snapshot | Stored network capabilities for orchestrators | Array of `OrchNetworkCapabilities`: `address`, `local_address`, `orch_uri`, `capabilities`, `capabilities_prices`, `hardware` | `gateway` |
| `<dynamic queue_event_type>` | Forwarded custom event streams | In event subscriptions, `queue_event_type` can pass through arbitrary Kafka event types | Arbitrary `event` object, with gateway-added `stream_id`, `request_id`, `pipeline_id`, `orchestrator_info` | `orchestrator`, `runner` |

## `stream_trace` Subtypes (`data.type`)

| Subtype | Purpose | Key fields |
|---|---|---|
| `gateway_receive_stream_request` | Stream request accepted at gateway | `timestamp`, `stream_id`, `pipeline_id`, `request_id`, empty `orchestrator_info` |
| `gateway_ingest_stream_closed` | Ingest connection closed/disconnected | `timestamp`, `stream_id`, `pipeline_id`, `request_id`, usually empty `orchestrator_info` |
| `gateway_send_first_ingest_segment` | First segment sent from gateway to orchestrator | `timestamp`, `stream_id`, `pipeline_id`, `request_id`, `orchestrator_info` |
| `gateway_receive_first_processed_segment` | First processed segment received back | `timestamp`, `stream_id`, `pipeline_id`, `request_id`, `orchestrator_info` |
| `gateway_receive_few_processed_segments` | Third processed segment received (readiness signal) | `timestamp`, `stream_id`, `pipeline_id`, `request_id`, `orchestrator_info` |
| `gateway_receive_first_data_segment` | First data-channel segment received (BYOC event) | `timestamp`, `stream_id`, `pipeline_id`, `request_id`, `orchestrator_info` |
| `gateway_no_orchestrators_available` | Selection failed because no orchestrators available | `timestamp`, `stream_id`, `pipeline_id`, `request_id`, empty `orchestrator_info` |
| `orchestrator_swap` | Mid-stream orchestrator swap event | `stream_id`, `request_id`, `pipeline`, `pipeline_id`, `message`, `orchestrator_info` |

## `stream_ingest_metrics.stats` Key Nested Fields

`stats` is a `media.MediaStats` object with:

- `peer_conn_stats`: `id`, `bytes_received`, `bytes_sent`
- `track_stats[]`: `type`, `jitter`, `packets_lost`, `packets_received`, `packet_loss_pct`, `rtt`, `last_input_ts`, `last_output_ts`, `latency`, optional `warnings`
- `conn_quality`

## Notes on Status/Event Forwarding

- Event subscriptions parse payloads as either:
  - `{ "queue_event_type": "...", "event": { ... } }`, or
  - direct event JSON (fallback), which defaults Kafka type to `ai_stream_events`.
- For forwarded status events (`type=status`), gateway rewrites queue type to `ai_stream_status`.
- For status snapshots, gateway coalesces `inference_status.last_restart_logs` and `inference_status.last_params` from prior status state when omitted in incremental updates.

## Primary Producer Locations

- `server/ai_live_video.go`
- `byoc/trickle.go`
- `server/ai_mediaserver.go`
- `byoc/stream_gateway.go`
- `server/ai_process.go`
- `discovery/discovery.go`
- `server/segment_rpc.go`
- `core/livepeernode.go`
- envelope wrapper: `monitor/kafka.go`
