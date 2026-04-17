# Livepeer Network Events Reference

This document catalogs every call to `monitor.SendQueueEventAsync` in the codebase. Each call publishes an event to a Kafka topic. Events are wrapped in a `GatewayEvent` envelope before dispatch.

## Envelope

Every event is wrapped in a `GatewayEvent` struct (`monitor/kafka.go`) before being written to Kafka:

```json
{
  "id":        "3f2e1a0b-4c5d-6e7f-8a9b-0c1d2e3f4a5b",
  "type":      "<topic-name>",
  "timestamp": "1743500000000",
  "gateway":   "10.0.1.42:8935",
  "data":      { ... }
}
```

| Field       | Source                          | Notes                         |
|-------------|---------------------------------|-------------------------------|
| `id`        | `uuid.New().String()`           | Random UUID per event         |
| `type`      | `eventType` argument            | Matches the Kafka topic name  |
| `timestamp` | `time.Now().UnixMilli()`        | Gateway wall-clock at publish |
| `gateway`   | `kafkaProducer.gatewayAddress`  | Address of the sending node   |
| `data`      | `data` argument                 | Event-specific payload        |

The channel buffer holds up to 100 events (`KafkaChannelSize`). If full, the event is dropped with a warning log.

---

## Topics

| Topic                  | # of call sites | Description                                      |
|------------------------|-----------------|--------------------------------------------------|
| `stream_trace`         | 18              | Stream lifecycle and timing milestones           |
| `ai_stream_events`     | 4 + 2 dynamic   | AI pipeline events (param updates, errors)       |
| `ai_stream_status`     | 2 dynamic       | AI runner status updates (routed dynamically)    |
| `stream_ingest_metrics`| 2               | Periodic WHIP/WebRTC transport statistics        |
| `discovery_results`    | 1               | Orchestrator discovery results                   |
| `network_capabilities` | 1               | Gateway's view of available orch capabilities    |
| `create_new_payment`   | 1               | Payment ticket creation on the orchestrator      |
| `create_signed_ticket` | 1               | Remote-signer ticket signing on the orchestrator |
| `job_gateway`          | 11              | BYOC job submit / discovery / token fetch        |
| `job_payment`          | 4 + relayed     | BYOC payment balance sync and ticket creation    |
| `job_auth`             | 2 + relayed     | BYOC auth webhook result (orch-relayed creds)    |
| `job_orchestrator`     | dynamic         | Orch-side job lifecycle, relayed via header      |
| `worker_lifecycle`     | dynamic         | Orch worker register/unregister/capacity drained |
| `ai_llm_request`       | 3               | LLM request start / completion / SSE completion  |
| `ai_batch_request`     | 2               | Batch AI pipeline received / completed           |

---

## Topic: `stream_trace`

Lifecycle and timing milestones for live AI streams. All 16 call sites emit the same top-level shape; the `type` field distinguishes the event.

### Common fields

| Field               | Type     | Description                                         |
|---------------------|----------|-----------------------------------------------------|
| `type`              | string   | Event subtype (see table below)                     |
| `timestamp`         | int64    | Unix milliseconds (from `time.Now().UnixMilli()`)   |
| `stream_id`         | string   | Stream identifier                                   |
| `pipeline_id`       | string   | Pipeline instance identifier                        |
| `request_id`        | string   | Request identifier                                  |
| `orchestrator_info` | object   | `{ "address": string, "url": string }`              |

Additional fields appear on specific subtypes (noted per event below).

---

### `gateway_receive_stream_request`

Fired when the gateway accepts a new live AI stream request, before orchestrator selection. `orchestrator_info` is empty (`""`) at this point.

**Call sites:**
- `server/ai_mediaserver.go:599` — RTMP ingest path
- `server/ai_mediaserver.go:1082` — WHIP ingest path
- `byoc/stream_gateway.go:587` — BYOC gateway path

```json
{
  "type":        "gateway_receive_stream_request",
  "timestamp":   1743500000000,
  "stream_id":   "stream-abc123",
  "pipeline_id": "pipe-xyz789",
  "request_id":  "req-001",
  "orchestrator_info": {
    "address": "",
    "url":     ""
  }
}
```

---

### `gateway_ingest_stream_closed`

Fired when the ingest side of a stream disconnects (MediaMTX segmenter exits or WHIP connection closes). `orchestrator_info` is empty at this stage.

**Call sites:**
- `server/ai_mediaserver.go:675` — RTMP ingest segmenter exits
- `server/ai_mediaserver.go:1109` — WHIP connection closes
- `byoc/stream_gateway.go:756` — BYOC MediaMTX ingest disconnects

```json
{
  "type":        "gateway_ingest_stream_closed",
  "timestamp":   1743500300000,
  "stream_id":   "stream-abc123",
  "pipeline_id": "pipe-xyz789",
  "request_id":  "req-001",
  "orchestrator_info": {
    "address": "",
    "url":     ""
  }
}
```

---

### `orchestrator_swap`

Fired when the gateway is about to retry the stream with a different orchestrator. Includes the reason for the swap and the address/URL of the orchestrator being abandoned.

**Extra fields:**
| Field      | Type   | Description                              |
|------------|--------|------------------------------------------|
| `pipeline` | string | Pipeline name (e.g. `"live-video-to-video"`) |
| `message`  | string | Swap reason / error description          |

**Call sites:**
- `server/ai_mediaserver.go:754`
- `byoc/stream_gateway.go:266`

```json
{
  "type":        "orchestrator_swap",
  "stream_id":   "stream-abc123",
  "request_id":  "req-001",
  "pipeline":    "live-video-to-video",
  "pipeline_id": "pipe-xyz789",
  "message":     "trickle subscribe error, swapping: EOF",
  "orchestrator_info": {
    "address": "0x4a9b6c3d2e1f0a8b7c6d5e4f3a2b1c0d9e8f7a6b",
    "url":     "https://orch-01.example.com:8935"
  }
}
```

---

### `gateway_no_orchestrators_available`

Fired when all orchestrators have been exhausted and no response was obtained. `orchestrator_info` is empty.

**Call site:** `server/ai_process.go:1604`

```json
{
  "type":        "gateway_no_orchestrators_available",
  "timestamp":   1743500120000,
  "stream_id":   "stream-abc123",
  "pipeline_id": "pipe-xyz789",
  "request_id":  "req-001",
  "orchestrator_info": {
    "address": "",
    "url":     ""
  }
}
```

---

### `gateway_send_first_ingest_segment`

Fired once when the first ingest segment is successfully written to the orchestrator's trickle endpoint. `orchestrator_info` is now populated.

**Call sites:**
- `server/ai_live_video.go:160`
- `byoc/trickle.go:133`

```json
{
  "type":        "gateway_send_first_ingest_segment",
  "timestamp":   1743500005000,
  "stream_id":   "stream-abc123",
  "pipeline_id": "pipe-xyz789",
  "request_id":  "req-001",
  "orchestrator_info": {
    "address": "0x4a9b6c3d2e1f0a8b7c6d5e4f3a2b1c0d9e8f7a6b",
    "url":     "https://orch-01.example.com:8935"
  }
}
```

---

### `gateway_receive_first_processed_segment`

Fired once when the gateway receives the first processed (AI-transformed) video segment back from the orchestrator. Used to measure end-to-end first-frame latency.

**Call sites:**
- `server/ai_live_video.go:344`
- `byoc/trickle.go:298`

```json
{
  "type":        "gateway_receive_first_processed_segment",
  "timestamp":   1743500008500,
  "stream_id":   "stream-abc123",
  "pipeline_id": "pipe-xyz789",
  "request_id":  "req-001",
  "orchestrator_info": {
    "address": "0x4a9b6c3d2e1f0a8b7c6d5e4f3a2b1c0d9e8f7a6b",
    "url":     "https://orch-01.example.com:8935"
  }
}
```

---

### `gateway_receive_few_processed_segments`

Fired once when 3 processed segments have been received. Used as a proxy for "runner started successfully and playback can begin".

**Call sites:**
- `server/ai_live_video.go:362`
- `byoc/trickle.go:316`

```json
{
  "type":        "gateway_receive_few_processed_segments",
  "timestamp":   1743500010000,
  "stream_id":   "stream-abc123",
  "pipeline_id": "pipe-xyz789",
  "request_id":  "req-001",
  "orchestrator_info": {
    "address": "0x4a9b6c3d2e1f0a8b7c6d5e4f3a2b1c0d9e8f7a6b",
    "url":     "https://orch-01.example.com:8935"
  }
}
```

---

### `gateway_receive_first_data_segment`

Fired once when the first non-video data segment (e.g. JSON output from the AI pipeline) is received back from the orchestrator.

**Call site:** `byoc/trickle.go:893`

```json
{
  "type":        "gateway_receive_first_data_segment",
  "timestamp":   1743500009000,
  "stream_id":   "stream-abc123",
  "pipeline_id": "pipe-xyz789",
  "request_id":  "req-001",
  "orchestrator_info": {
    "address": "0x4a9b6c3d2e1f0a8b7c6d5e4f3a2b1c0d9e8f7a6b",
    "url":     "https://orch-01.example.com:8935"
  }
}
```

---

## Topic: `ai_stream_events`

Events from the AI processing pipeline. Covers three distinct scenarios: parameter updates, errors, and passthrough events from the orchestrator that are not status updates.

---

### `params_update`

Fired when updated inference parameters are forwarded from the client to the orchestrator via the control channel. The `params` field contains the raw JSON parameter object as sent by the client.

**Call sites:**
- `server/ai_live_video.go:619`
- `byoc/trickle.go:492`

```json
{
  "type":        "params_update",
  "stream_id":   "stream-abc123",
  "request_id":  "req-001",
  "pipeline":    "live-video-to-video",
  "pipeline_id": "pipe-xyz789",
  "params": {
    "prompt":         "a cinematic oil painting",
    "strength":       0.75,
    "guidance_scale": 7.5
  },
  "orchestrator_info": {
    "address": "0x4a9b6c3d2e1f0a8b7c6d5e4f3a2b1c0d9e8f7a6b",
    "url":     "https://orch-01.example.com:8935"
  }
}
```

---

### Error event (`type: "error"`)

Fired when the live pipeline hits a fatal error. The base event map is cloned from the `sendErrorEvent` closure (which includes `type`, `request_id`, `stream_id`, `pipeline_id`, `pipeline`), then `capability` and `message` are appended.

**Call sites:**
- `server/ai_live_video.go:951` (via `LiveErrorEventSender`)
- `byoc/trickle.go:924` (via `LiveErrorEventSender`)

```json
{
  "type":        "error",
  "stream_id":   "stream-abc123",
  "request_id":  "req-001",
  "pipeline_id": "pipe-xyz789",
  "pipeline":    "live-video-to-video",
  "capability":  "LiveVideoToVideo",
  "message":     "trickle subscribe: connection reset by peer"
}
```

---

### Passthrough orchestrator events

The gateway subscribes to an event stream from the orchestrator over trickle. Each segment is parsed as a JSON object with an optional `queue_event_type` wrapper field. If the event type is not `"status"`, it is forwarded as-is to `ai_stream_events`. The gateway appends `stream_id`, `request_id`, `pipeline_id`, and `orchestrator_info` to every event.

**Call sites:**
- `server/ai_live_video.go:840`
- `byoc/trickle.go:747`

Example — an inference completion notification from the runner:

```json
{
  "type":        "inference_complete",
  "stream_id":   "stream-abc123",
  "request_id":  "req-001",
  "pipeline_id": "pipe-xyz789",
  "seq":         42,
  "latency_ms":  320,
  "orchestrator_info": {
    "address": "0x4a9b6c3d2e1f0a8b7c6d5e4f3a2b1c0d9e8f7a6b",
    "url":     "https://orch-01.example.com:8935"
  }
}
```

---

## Topic: `ai_stream_status`

Routed from the same two call sites as the `ai_stream_events` passthrough above. When the parsed event has `"type": "status"`, `queueEventType` is set to `"ai_stream_status"` instead of `"ai_stream_events"`.

The gateway also merges `inference_status.last_restart_logs` and `inference_status.last_params` from the previous cached status if those fields are absent in the new event (to preserve the last non-null values for the `/status` API).

**Call sites:**
- `server/ai_live_video.go:840`
- `byoc/trickle.go:747`

The payload is a `PipelineStatus`-shaped object (`monitor/kafka.go`):

```json
{
  "type": "status",
  "stream_id":   "stream-abc123",
  "request_id":  "req-001",
  "pipeline_id": "pipe-xyz789",
  "orchestrator_info": {
    "address": "0x4a9b6c3d2e1f0a8b7c6d5e4f3a2b1c0d9e8f7a6b",
    "url":     "https://orch-01.example.com:8935"
  },
  "inference_status": {
    "pipeline":               "live-video-to-video",
    "start_time":             1743500000.123,
    "last_params_update_time":1743500060.000,
    "last_params": {
      "prompt":    "oil painting",
      "strength":  0.8
    },
    "last_params_hash":   "a1b2c3d4",
    "input_fps":          25.0,
    "output_fps":         24.8,
    "last_input_time":    1743500119.500,
    "last_output_time":   1743500119.800,
    "restart_count":      1,
    "last_restart_time":  1743500030.000,
    "last_restart_logs":  ["CUDA OOM on init", "retrying with lower batch size"],
    "last_error":         null,
    "stream_id":          "stream-abc123"
  }
}
```

---

## Topic: `stream_ingest_metrics`

Periodic WebRTC transport statistics for the WHIP ingest connection. Emitted on a ticker inside the WHIP stats loop. The `stats` field is a `media.MediaStats` struct.

**Call sites:**
- `server/ai_mediaserver.go:1248`
- `byoc/stream_gateway.go:1117`

```json
{
  "timestamp":   1743500060000,
  "stream_id":   "stream-abc123",
  "pipeline_id": "pipe-xyz789",
  "request_id":  "req-001",
  "stats": {
    "peer_conn_stats": {
      "ID":            "transport-001",
      "BytesReceived": 4823042,
      "BytesSent":     1024
    },
    "track_stats": [
      {
        "type":             "video",
        "jitter":           0.003,
        "packets_lost":     2,
        "packets_received": 3780,
        "packet_loss_pct":  0.053,
        "rtt":              18000000,
        "last_input_ts":    90000.123,
        "last_output_ts":   90000.456,
        "latency":          0.023,
        "warnings":         []
      },
      {
        "type":             "audio",
        "jitter":           0.001,
        "packets_lost":     0,
        "packets_received": 1890,
        "packet_loss_pct":  0.0,
        "rtt":              17000000,
        "last_input_ts":    48000.000,
        "last_output_ts":   48000.010,
        "latency":          0.010,
        "warnings":         []
      }
    ],
    "conn_quality": "good"
  }
}
```

---

## Topic: `discovery_results`

Fired once per orchestrator discovery cycle after sorting and filtering. The data is an array (not an object) — one entry per selected orchestrator, in preference order (ascending latency).

**Call site:** `discovery/discovery.go:325`

```json
[
  {
    "address":    "0x4a9b6c3d2e1f0a8b7c6d5e4f3a2b1c0d9e8f7a6b",
    "url":        "https://orch-01.example.com:8935",
    "latency_ms": "12"
  },
  {
    "address":    "0x1d2c3b4a5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b",
    "url":        "https://orch-02.example.com:8935",
    "latency_ms": "34"
  },
  {
    "address":    "0x9f8e7d6c5b4a3b2c1d0e9f8a7b6c5d4e3f2a1b0c",
    "url":        "https://orch-03.example.com:8935",
    "latency_ms": "71"
  }
]
```

---

## Topic: `network_capabilities`

Fired whenever the gateway refreshes its cached view of orchestrator capabilities (e.g. after polling `/getNetworkCapabilities`). The data is an array of `common.OrchNetworkCapabilities` structs.

**Call site:** `core/livepeernode.go:354`

```json
[
  {
    "address":       "0x4a9b6c3d2e1f0a8b7c6d5e4f3a2b1c0d9e8f7a6b",
    "local_address": "10.0.1.10:8935",
    "orch_uri":      "https://orch-01.example.com:8935",
    "capabilities": {
      "bitstring": [512, 0, 0, 0],
      "mandatory": null
    },
    "capabilities_prices": [
      {
        "pricePerUnit":  1,
        "pixelsPerUnit": 1
      }
    ],
    "hardware": [
      {
        "gpu_info": {
          "0": {
            "name":        "NVIDIA A10G",
            "memory_total": 23028,
            "memory_free":  18432
          }
        },
        "model_id":  "stabilityai/sd-turbo",
        "pipeline":  "live-video-to-video"
      }
    ],
    "capability_options": {
      "LiveVideoToVideo": [
        {
          "model_id": "stabilityai/sd-turbo",
          "vram":     "20GB"
        }
      ]
    }
  }
]
```

---

## Topic: `create_new_payment`

Fired on the orchestrator each time a new probabilistic payment ticket batch is created for a transcoding session. All values are strings.

**Call site:** `server/segment_rpc.go:877`

```json
{
  "clientIP":     "192.168.1.100",
  "requestID":    "req-001",
  "capability":   "LiveVideoToVideo",
  "manifestID":   "Qm1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0",
  "sessionID":    "sess-deadbeef-1234-5678-abcd-ef0123456789",
  "recipient":    "0x4a9b6c3d2e1f0a8b7c6d5e4f3a2b1c0d9e8f7a6b",
  "faceValue":    "0.000000100000000000",
  "winProb":      "0.0010000000",
  "price":        "100.000 wei/pixel",
  "numTickets":   "1",
  "sender":       "0x1d2c3b4a5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b",
  "orchestrator": "https://orch-01.example.com:8935"
}
```

---

## Topic: `create_signed_ticket`

Fired on the orchestrator's remote signer each time a ticket is signed for a pending billable session. Used as the source of truth for billable compute accounting on the orchestrator side.

**Call site:** `server/remote_signer.go:518`

```json
{
  "session_id":         "state-abc",
  "session_status":     "continuing",
  "pipeline":           "live-video-to-video",
  "request_id":         "req-001",
  "orch_address":       "0x4a9b6c3d2e1f0a8b7c6d5e4f3a2b1c0d9e8f7a6b",
  "orch_url":           "https://orch-01.example.com:8935",
  "manifest_id":        "Qm1a2b3c4d5e...",
  "pm_session_id":      "pm-sess-xyz",
  "current_time":       "2026-04-15T12:00:00Z",
  "current_time_unix":  1776384000000,
  "previous_time":      "2026-04-15T11:59:00Z",
  "previous_time_unix": 1776383940000,
  "billable_secs":      60,
  "pixels":             124416000,
  "session_balance":    "1000",
  "computed_fee":       "100",
  "cost_per_pixel":     "0.0000000008",
  "sequence_number":    42,
  "num_tickets":        1
}
```

`session_status` is `"new"` if `sequence_number == 0`, otherwise `"continuing"`.

---

## Topic: `job_gateway`

BYOC job submission lifecycle on the gateway. All events use `data.type` as the subtype discriminator.

### Subtypes

| Subtype                              | Call site                         | Fires when                                                       |
|--------------------------------------|-----------------------------------|------------------------------------------------------------------|
| `job_gateway_submitted`              | `byoc/job_gateway.go:78`          | Gateway begins forwarding job request to one orch (per attempt) |
| `job_gateway_completed` (success)    | `byoc/job_gateway.go:151, 232`    | Non-streaming or streaming response finished successfully       |
| `job_gateway_completed` (failure)    | `byoc/job_gateway.go:91`          | `sendJobToOrch` returned error on this attempt                  |
| `job_orchestrator_discovery_result`  | `byoc/job_gateway.go:384, 395, 405` | Final orch-pool selection outcome for the job                   |
| `job_orchestrator_token_fetch_result`| `byoc/job_gateway.go:492, 508, 527, 542, 554` | Per-orch token fetch result during discovery         |

### `job_gateway_submitted`

```json
{
  "type":          "job_gateway_submitted",
  "request_id":    "req-001",
  "attempt_index": 0,
  "capability":    "LLM",
  "orchestrator_info": {
    "address": "0x4a9b6c3d2e1f0a8b7c6d5e4f3a2b1c0d9e8f7a6b",
    "url":     "https://orch-01.example.com:8935"
  }
}
```

### `job_gateway_completed`

`success: true` path also includes `http_status` and (for SSE) `streaming: true`. Error path has `error: "<msg>"` and no `http_status`.

```json
{
  "type":          "job_gateway_completed",
  "request_id":    "req-001",
  "attempt_index": 0,
  "capability":    "LLM",
  "success":       true,
  "error":         null,
  "duration_ms":   2345,
  "completed_at":  1776384002345,
  "http_status":   200,
  "streaming":     true,
  "orchestrator_info": { "address": "0x...", "url": "https://..." }
}
```

### `job_orchestrator_discovery_result`

`orchestrator_info` is absent. `selected` is the count of orchs that passed capacity + options filter.

```json
{
  "type":       "job_orchestrator_discovery_result",
  "capability": "LLM",
  "success":    true,
  "selected":   3,
  "error":      null
}
```

### `job_orchestrator_token_fetch_result`

Per-orch probe result during fan-out discovery. On success includes `available_capacity` and populated `orchestrator_info`; on failure includes `error` and optionally `http_status`.

```json
{
  "type":               "job_orchestrator_token_fetch_result",
  "orch_url":           "https://orch-01.example.com:8935",
  "capability":         "LLM",
  "success":            true,
  "latency_ms":         42,
  "available_capacity": 5,
  "error":              null,
  "orchestrator_info":  { "address": "0x...", "url": "https://..." }
}
```

---

## Topic: `job_payment`

BYOC payment accounting. Populated by both gateway-direct events (balance sync, ticket creation, stream cycle) and orchestrator-relayed events forwarded through the `X-Livepeer-Events` header (`byoc/job_gateway.go:310` via `orchEventTopic` mapping `payment_*` → `job_payment`).

### Subtypes

| Subtype                | Origin      | Call site                              |
|------------------------|-------------|----------------------------------------|
| `payment_balance_sync` | Gateway     | `byoc/payment.go:101`                  |
| `payment_created`      | Gateway     | `byoc/payment.go:134, 211`             |
| `payment_stream_cycle` | Gateway     | `byoc/stream_gateway.go:391`           |
| `payment_*` (orch)     | Orch-relayed| `byoc/job_gateway.go:310`              |

### `payment_balance_sync`

Emitted once per job submission before deciding whether to create tickets.

```json
{
  "type":              "payment_balance_sync",
  "request_id":        "req-001",
  "capability":        "LLM",
  "sender":            "0x1d2c...",
  "orchestrator_addr": "0x4a9b...",
  "balance":           "120.000",
  "diff":              "10.000",
  "min_bal_covered":   true,
  "reset_to_zero":     false,
  "orchestrator_info": { "address": "0x4a9b...", "url": "https://..." }
}
```

### `payment_created`

`with_tickets: false` means min balance already covered (no new tickets issued); `true` means a fresh ticket batch was created.

```json
{
  "type":         "payment_created",
  "request_id":   "req-001",
  "capability":   "LLM",
  "sender":       "0x1d2c...",
  "balance":      "120.000",
  "with_tickets": true,
  "orchestrator_info": { "address": "0x4a9b...", "url": "https://..." }
}
```

### `payment_stream_cycle`

Fires every ~50s inside `monitorStream` per active BYOC stream.

```json
{
  "type":       "payment_stream_cycle",
  "stream_id":  "stream-abc123",
  "request_id": "req-001",
  "success":    true,
  "error":      "",
  "orchestrator_info": { "address": "0x4a9b...", "url": "https://..." }
}
```

---

## Topic: `job_auth`

BYOC live-stream authentication webhook results.

### Subtypes

| Subtype                         | Origin      | Call site                         |
|---------------------------------|-------------|-----------------------------------|
| `job_auth_webhook_result`       | Gateway     | `byoc/stream_gateway.go:588, 624` |
| `job_credential_verify_result`  | Orch-relayed| `byoc/job_gateway.go:310` (via `orchEventTopic`) |

### `job_auth_webhook_result`

`success: true` variant includes `pipeline_id`; failure variant omits it and populates `error`.

```json
{
  "type":         "job_auth_webhook_result",
  "stream_name":  "stream-abc",
  "stream_id":    "stream-abc123",
  "pipeline":     "live-video-to-video",
  "pipeline_id":  "pipe-xyz789",
  "success":      true,
  "error":        null,
  "gateway_host": "gw.example.com"
}
```

---

## Topic: `job_orchestrator`

Orchestrator-side job lifecycle events forwarded by the gateway. Produced by the orchestrator and shipped back via the `X-Livepeer-Events` response header; the gateway re-publishes each entry on a Kafka topic derived from the event type (prefix `job_orchestrator_` → `job_orchestrator`) and enriches with `orchestrator_info` (`byoc/job_gateway.go:297–310`).

Payload schema is defined by the orchestrator, not the gateway. A replay engine should treat this as a loose `map[string]interface{}` keyed on `data.type`, with `orchestrator_info` always present.

```json
{
  "type":       "job_orchestrator_<subtype>",
  "request_id": "req-001",
  /* orchestrator-supplied fields */
  "orchestrator_info": { "address": "0x...", "url": "https://..." }
}
```

---

## Topic: `worker_lifecycle`

Orchestrator worker fleet events, drained by the gateway from each orchestrator's `/events/drain` endpoint (`discovery/db_discovery.go:554`). Events carry an orch-assigned UUID in `data.id` — use that for dedup, not the envelope UUID.

### Subtypes

`lifecycleEventTopic` (`discovery/db_discovery.go:612`) maps:

| Event type                  | Routed to topic     |
|-----------------------------|---------------------|
| `worker_registered`         | `worker_lifecycle`  |
| `worker_unregistered`       | `worker_lifecycle`  |
| `worker_capacity_exhausted` | `worker_lifecycle`  |

```json
{
  "id":   "uuid-assigned-by-orch",
  "type": "worker_registered",
  /* orch-supplied fields (capability, worker id, capacity, etc) */
  "orchestrator_info": { "address": "0x...", "url": "https://..." }
}
```

Note: the `data.id` is assigned by the orchestrator at insert time and preserved across the relay so downstream consumers can deduplicate across re-drains.

---

## Topic: `ai_llm_request`

LLM pipeline request/response events emitted by the gateway.

### Subtypes

| Subtype                  | Call site                  | Fires when                                  |
|--------------------------|----------------------------|---------------------------------------------|
| `llm_request_started`    | `server/ai_process.go:1200`| Before HTTP call to orch worker             |
| `llm_request_completed`  | `server/ai_process.go:1376`| Non-streaming response fully parsed         |
| `llm_stream_completed`   | `server/ai_process.go:1308`| SSE stream closed (final chunk or error)    |

### `llm_request_started`

```json
{
  "type":           "llm_request_started",
  "timestamp":      1776384000000,
  "request_id":     "req-001",
  "model":          "meta-llama/Llama-3.1-8B-Instruct",
  "streaming":      true,
  "max_tokens":     512,
  "message_count":  4,
  "system_prompt":  true,
  "temperature":    0.7,
  "orch_url":       "https://orch-01.example.com:8935",
  "price_per_unit": 0.0001,
  "client_ip":      "10.0.1.5"
}
```

### `llm_stream_completed`

Includes TTFT, token usage, utilisation, tokens-per-second, and `finish_reason` (`stop`, `length`, `error`, `unknown`). `error` is an empty string on success.

```json
{
  "type":                 "llm_stream_completed",
  "timestamp":            1776384002500,
  "request_id":           "req-001",
  "model":                "meta-llama/Llama-3.1-8B-Instruct",
  "completion_id":        "cmpl-abc",
  "orch_url":             "https://orch-01.example.com:8935",
  "prompt_tokens":        120,
  "completion_tokens":    200,
  "total_tokens":         320,
  "max_tokens_requested": 512,
  "token_utilization":    0.625,
  "finish_reason":        "stop",
  "total_duration_ms":    2500,
  "tokens_per_second":    80.0,
  "latency_score":        0.72,
  "price_per_unit":       0.0001,
  "ttft_ms":              180,
  "error":                ""
}
```

### `llm_request_completed`

Same shape as `llm_stream_completed` minus `ttft_ms` and `error`.

---

## Topic: `ai_batch_request`

Non-live AI pipeline lifecycle (image-to-image, audio-to-text, text-to-image, etc). Fires only when `params.liveParams == nil`.

### Subtypes

| Subtype                        | Call site                  |
|--------------------------------|----------------------------|
| `ai_batch_request_received`    | `server/ai_process.go:1628`|
| `ai_batch_request_completed`   | `server/ai_process.go:1652`|

### `ai_batch_request_received`

```json
{
  "type":         "ai_batch_request_received",
  "timestamp":    1776384000000,
  "request_id":   "req-001",
  "pipeline":     "text-to-image",
  "model_id":     "stabilityai/sd-turbo",
  "client_ip":    "10.0.1.5",
  "orchestrator": ""
}
```

### `ai_batch_request_completed`

Fired via `defer` after the retry loop exits. `success` reflects whether a non-nil response was returned. `error_type` is one of a small classifier set (`batchAIRequestErrorType`).

```json
{
  "type":           "ai_batch_request_completed",
  "timestamp":      1776384004000,
  "request_id":     "req-001",
  "pipeline":       "text-to-image",
  "model_id":       "stabilityai/sd-turbo",
  "success":        true,
  "tries":          1,
  "duration_ms":    4000,
  "orch_url":       "https://orch-01.example.com:8935",
  "latency_score":  0.9,
  "price_per_unit": 0.0001,
  "error_type":     "",
  "error":          ""
}
```

---

## Additional `stream_trace` subtypes

Beyond the subtypes documented earlier, these were added for BYOC streams:

### `stream_stopped`

Emitted via `defer` inside `runStream` when a BYOC stream exits (normal or error). Includes cumulative `duration_ms` and `swap_count`. No `orchestrator_info`.

**Call site:** `byoc/stream_gateway.go:167`

```json
{
  "type":        "stream_stopped",
  "stream_id":   "stream-abc123",
  "duration_ms": 360000,
  "swap_count":  2,
  "error":       ""
}
```

### `stream_orchestrator_token_refresh`

Emitted during BYOC orch swap when refreshing a job token against the new orch.

**Call sites:** `byoc/stream_gateway.go:312` (failure), `byoc/stream_gateway.go:325` (success)

```json
{
  "type":      "stream_orchestrator_token_refresh",
  "stream_id": "stream-abc123",
  "success":   true,
  "error":     null,
  "orchestrator_info": { "address": "0x...", "url": "https://..." }
}
```

---

## Delivery semantics (important for replay)

`SendQueueEventAsync` (`monitor/kafka.go:164`) is best-effort, **at-most-once**:

- Buffered channel of size `KafkaChannelSize = 100` (`monitor/kafka.go:22`).
- Batch flush every `KafkaBatchInterval = 1s` or when `KafkaBatchSize = 100` accumulates.
- If the channel is full at publish, the event is **silently dropped** with only a warning log (`monitor/kafka.go:181`). A counter `KafkaEventSendError` is bumped.
- Batch write retries 3 times; after that the whole batch is lost (`monitor/kafka.go:148`).
- Kafka message key = envelope `id` (random UUID) → **no partition-level per-entity ordering**. Replay engines must sort by `data.timestamp` (or `data.completed_at`, `data.current_time_unix`) within an entity key.

### Two event origins

1. **Gateway-direct** — stamped with gateway wall clock at publish.
2. **Orchestrator-relayed** — produced on orch, transported either via the `X-Livepeer-Events` response header (`byoc/job_gateway.go:290–313`, routed by `orchEventTopic`) or polled from `/events/drain` (`discovery/db_discovery.go:554`, routed by `lifecycleEventTopic`). Gateway enriches with `orchestrator_info`; for `worker_lifecycle` it preserves the orch-assigned `data.id` for consumer dedup.

### Replay rules

1. Join on `request_id` for per-request projections, `stream_id` for stream timelines, `orchestrator_info.address` for orch-fleet analytics.
2. Dedup on envelope `id` generally; on `data.id` for `worker_lifecycle`; on `(request_id, data.type, data.timestamp)` to guard against re-emission from orch drains.
3. Sort by `data.timestamp` within an entity key — Kafka offset is not trustworthy.
4. Treat the stream as lossy: build projections that tolerate missing events, not strict state machines.
5. Topic-specific subtype dispatchers should key on `(topic, data.type)`; topics without a subtype discriminator are `discovery_results`, `network_capabilities`, `stream_ingest_metrics`, `create_new_payment`, `create_signed_ticket`.
6. Numeric fields on `create_new_payment` are all strings — parse on the consumer.
7. Orch-relayed topics (`job_orchestrator`, `job_payment` `payment_*` relays, `job_auth` `job_credential_verify_result`, `worker_lifecycle`) carry schemas defined by the orchestrator, not the gateway. Expect drift; consume defensively.

---

## Complete call site index

| # | File | Line | Topic | Subtype / Notes |
|---|------|------|-------|-----------------|
| 1 | `server/ai_mediaserver.go` | 599 | `stream_trace` | `gateway_receive_stream_request` (RTMP) |
| 2 | `server/ai_mediaserver.go` | 675 | `stream_trace` | `gateway_ingest_stream_closed` (RTMP) |
| 3 | `server/ai_mediaserver.go` | 754 | `stream_trace` | `orchestrator_swap` |
| 4 | `server/ai_mediaserver.go` | 1082 | `stream_trace` | `gateway_receive_stream_request` (WHIP) |
| 5 | `server/ai_mediaserver.go` | 1109 | `stream_trace` | `gateway_ingest_stream_closed` (WHIP) |
| 6 | `server/ai_mediaserver.go` | 1248 | `stream_ingest_metrics` | WHIP WebRTC stats |
| 7 | `server/ai_live_video.go` | 160 | `stream_trace` | `gateway_send_first_ingest_segment` |
| 8 | `server/ai_live_video.go` | 344 | `stream_trace` | `gateway_receive_first_processed_segment` |
| 9 | `server/ai_live_video.go` | 362 | `stream_trace` | `gateway_receive_few_processed_segments` |
| 10 | `server/ai_live_video.go` | 619 | `ai_stream_events` | `params_update` |
| 11 | `server/ai_live_video.go` | 840 | `ai_stream_events` or `ai_stream_status` | Passthrough from orchestrator event stream |
| 12 | `server/ai_live_video.go` | 951 | `ai_stream_events` | Error event |
| 13 | `server/ai_process.go` | 1604 | `stream_trace` | `gateway_no_orchestrators_available` |
| 14 | `server/segment_rpc.go` | 877 | `create_new_payment` | Payment ticket creation |
| 15 | `byoc/stream_gateway.go` | 266 | `stream_trace` | `orchestrator_swap` |
| 16 | `byoc/stream_gateway.go` | 587 | `stream_trace` | `gateway_receive_stream_request` |
| 17 | `byoc/stream_gateway.go` | 756 | `stream_trace` | `gateway_ingest_stream_closed` |
| 18 | `byoc/stream_gateway.go` | 1117 | `stream_ingest_metrics` | WHIP WebRTC stats |
| 19 | `byoc/trickle.go` | 133 | `stream_trace` | `gateway_send_first_ingest_segment` |
| 20 | `byoc/trickle.go` | 298 | `stream_trace` | `gateway_receive_first_processed_segment` |
| 21 | `byoc/trickle.go` | 316 | `stream_trace` | `gateway_receive_few_processed_segments` |
| 22 | `byoc/trickle.go` | 492 | `ai_stream_events` | `params_update` |
| 23 | `byoc/trickle.go` | 747 | `ai_stream_events` or `ai_stream_status` | Passthrough from orchestrator event stream |
| 24 | `byoc/trickle.go` | 893 | `stream_trace` | `gateway_receive_first_data_segment` |
| 25 | `byoc/trickle.go` | 924 | `ai_stream_events` | Error event |
| 26 | `discovery/discovery.go` | 325 | `discovery_results` | Sorted orch list after discovery |
| 27 | `core/livepeernode.go` | 356 | `network_capabilities` | Orch capability snapshot |
| 28 | `server/remote_signer.go` | 518 | `create_signed_ticket` | Remote signer ticket signing |
| 29 | `server/ai_process.go` | 1200 | `ai_llm_request` | `llm_request_started` |
| 30 | `server/ai_process.go` | 1308 | `ai_llm_request` | `llm_stream_completed` |
| 31 | `server/ai_process.go` | 1376 | `ai_llm_request` | `llm_request_completed` |
| 32 | `server/ai_process.go` | 1628 | `ai_batch_request` | `ai_batch_request_received` |
| 33 | `server/ai_process.go` | 1652 | `ai_batch_request` | `ai_batch_request_completed` |
| 34 | `byoc/job_gateway.go` | 78 | `job_gateway` | `job_gateway_submitted` |
| 35 | `byoc/job_gateway.go` | 91 | `job_gateway` | `job_gateway_completed` (error) |
| 36 | `byoc/job_gateway.go` | 151 | `job_gateway` | `job_gateway_completed` (non-streaming success) |
| 37 | `byoc/job_gateway.go` | 232 | `job_gateway` | `job_gateway_completed` (streaming success) |
| 38 | `byoc/job_gateway.go` | 310 | dynamic (`job_auth` / `job_orchestrator` / `job_payment`) | Orch-relayed via `X-Livepeer-Events` header |
| 39 | `byoc/job_gateway.go` | 384 | `job_gateway` | `job_orchestrator_discovery_result` (error) |
| 40 | `byoc/job_gateway.go` | 395 | `job_gateway` | `job_orchestrator_discovery_result` (empty) |
| 41 | `byoc/job_gateway.go` | 405 | `job_gateway` | `job_orchestrator_discovery_result` (success) |
| 42 | `byoc/job_gateway.go` | 492 | `job_gateway` | `job_orchestrator_token_fetch_result` (net error) |
| 43 | `byoc/job_gateway.go` | 508 | `job_gateway` | `job_orchestrator_token_fetch_result` (non-200) |
| 44 | `byoc/job_gateway.go` | 527 | `job_gateway` | `job_orchestrator_token_fetch_result` (read error) |
| 45 | `byoc/job_gateway.go` | 542 | `job_gateway` | `job_orchestrator_token_fetch_result` (unmarshal error) |
| 46 | `byoc/job_gateway.go` | 554 | `job_gateway` | `job_orchestrator_token_fetch_result` (success) |
| 47 | `byoc/payment.go` | 101 | `job_payment` | `payment_balance_sync` |
| 48 | `byoc/payment.go` | 134 | `job_payment` | `payment_created` (no tickets) |
| 49 | `byoc/payment.go` | 211 | `job_payment` | `payment_created` (with tickets) |
| 50 | `byoc/stream_gateway.go` | 167 | `stream_trace` | `stream_stopped` |
| 51 | `byoc/stream_gateway.go` | 312 | `stream_trace` | `stream_orchestrator_token_refresh` (failure) |
| 52 | `byoc/stream_gateway.go` | 325 | `stream_trace` | `stream_orchestrator_token_refresh` (success) |
| 53 | `byoc/stream_gateway.go` | 391 | `job_payment` | `payment_stream_cycle` |
| 54 | `byoc/stream_gateway.go` | 588 | `job_auth` | `job_auth_webhook_result` (failure) |
| 55 | `byoc/stream_gateway.go` | 624 | `job_auth` | `job_auth_webhook_result` (success) |
| 56 | `discovery/db_discovery.go` | 569 | `worker_lifecycle` (via `lifecycleEventTopic`) | Orch worker register/unregister/capacity drained |
