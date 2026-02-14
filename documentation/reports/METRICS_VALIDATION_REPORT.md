# Metrics Validation Report (JSONL)

- input file: `scripts/livepeer_samples.jsonl`
- generated at: `2026-02-11T16:58:55.158530+00:00`

## Table Coverage

### `ai_stream_status`
- row count: 150
- time span: 2026-01-23T15:59:58.979000 to 2026-02-10T22:51:13.627000 (18 days, 6:51:14.648000)
- field coverage: `stream_id` 114/150 nonempty (150/150 present), `request_id` 150/150 nonempty (150/150 present), `pipeline` 150/150 nonempty (150/150 present), `pipeline_id` 0/150 nonempty (150/150 present), `gateway` 114/150 nonempty (150/150 present), `orchestrator_address` 150/150 nonempty (150/150 present), `orchestrator_url` 150/150 nonempty (150/150 present), `output_fps` 150/150 nonempty (150/150 present), `input_fps` 150/150 nonempty (150/150 present), `start_time` 150/150 nonempty (150/150 present), `state` 150/150 nonempty (150/150 present)

### `stream_ingest_metrics`
- row count: 100
- time span: 2026-02-10T18:30:03.494000 to 2026-02-10T18:37:08.494000 (0:07:05)
- field coverage: `stream_id` 100/100 nonempty (100/100 present), `request_id` 100/100 nonempty (100/100 present), `pipeline_id` 0/100 nonempty (100/100 present), `connection_quality` 100/100 nonempty (100/100 present), `bytes_received` 100/100 nonempty (100/100 present), `bytes_sent` 100/100 nonempty (100/100 present), `video_jitter` 100/100 nonempty (100/100 present), `audio_jitter` 100/100 nonempty (100/100 present), `video_latency` 100/100 nonempty (100/100 present), `audio_latency` 100/100 nonempty (100/100 present)

### `stream_trace_events`
- row count: 150
- time span: 2026-01-23T18:13:07.969000 to 2026-02-10T22:51:49.594000 (18 days, 4:38:41.625000)
- field coverage: `stream_id` 146/150 nonempty (150/150 present), `request_id` 150/150 nonempty (150/150 present), `pipeline_id` 0/150 nonempty (150/150 present), `orchestrator_address` 36/150 nonempty (150/150 present), `orchestrator_url` 36/150 nonempty (150/150 present), `trace_type` 150/150 nonempty (150/150 present), `data_timestamp` 150/150 nonempty (150/150 present)

### `ai_stream_events`
- row count: 150
- time span: 2026-01-23T15:59:50.430000 to 2026-02-10T22:51:49.594000 (18 days, 6:51:59.164000)
- field coverage: `stream_id` 112/150 nonempty (150/150 present), `request_id` 150/150 nonempty (150/150 present), `pipeline` 150/150 nonempty (150/150 present), `pipeline_id` 0/150 nonempty (150/150 present), `event_type` 150/150 nonempty (150/150 present), `message` 150/150 nonempty (150/150 present), `capability` 0/150 nonempty (150/150 present)

### `payment_events`
- row count: 150
- time span: 2026-01-23T15:59:55.557000 to 2026-02-10T22:50:52.605000 (18 days, 6:50:57.048000)
- field coverage: `request_id` 52/150 nonempty (150/150 present), `session_id` 150/150 nonempty (150/150 present), `manifest_id` 150/150 nonempty (150/150 present), `sender` 150/150 nonempty (150/150 present), `recipient` 150/150 nonempty (150/150 present), `orchestrator` 150/150 nonempty (150/150 present), `capability` 0/150 nonempty (150/150 present)

### `network_capabilities`
- row count: 100
- time span: 2026-02-03T22:59:36.807000 to 2026-02-10T22:55:12.295000 (6 days, 23:55:35.488000)
- field coverage: `orchestrator_address` 100/100 nonempty (100/100 present), `orch_uri` 100/100 nonempty (100/100 present), `gpu_id` 85/100 nonempty (100/100 present), `gpu_name` 85/100 nonempty (100/100 present), `pipeline` 100/100 nonempty (100/100 present), `model_id` 100/100 nonempty (100/100 present), `runner_version` 80/100 nonempty (100/100 present)

### `discovery_results`
- row count: 100
- time span: 2026-02-03T23:01:05.345000 to 2026-02-10T22:55:33.414000 (6 days, 23:54:28.069000)
- field coverage: `orchestrator_address` 100/100 nonempty (100/100 present), `orchestrator_url` 100/100 nonempty (100/100 present), `latency_ms` 100/100 nonempty (100/100 present)

## Metric Feasibility

### Output FPS
- rows with `output_fps`: 150/150
- avg `output_fps`: 11.049
- p95 `output_fps`: 29.174
- avg `output_fps / input_fps`: 0.614
- top orchestrators by sample volume (count, avg_fps, p95_fps):
- `0x52cf2968b3dc6016778742d3539449a6597d5954`: 56, 9.355, 17.213
- `0x22d4da4b5ce66601a7fca3cee56070d2deaa6072`: 17, 12.235, 26.593
- `0xd66e5c00725e0d87d57172191ebd5e865fd71cff`: 12, 17.574, 28.620
- `0x0074780feff1fd0277fad6ccdb5a29908df6051f`: 12, 21.347, 53.184
- `0x3bbe84023c11c4874f493d70b370d26390e3c580`: 8, 3.971, 11.590

### Jitter
- FPS jitter coefficient (`stddevPop(output_fps)/avg(output_fps)`): 1.034
- avg network `video_jitter`: 100.907
- avg network `audio_jitter`: 20.091
- note: FPS jitter and network jitter are different KPIs and should be reported separately.

### Up/Down Bandwidth (Inferred)
- streams sampled: 1
- valid delta pairs: 49
- negative delta pairs skipped: 0
- avg down Mbps: 0.051
- p95 down Mbps: 0.057
- avg up Mbps: 0.005
- p95 up Mbps: 0.005
- formula: `((delta_bytes * 8) / delta_seconds) / 1_000_000`.

### Startup / E2E / Prompt Latency (Trace Proxies)
- trace requests analyzed: 38
- startup proxy pairs found: 4 (missing: 34)
- startup proxy avg ms: 13086.250
- e2e proxy pairs found: 1 (missing: 35)
- e2e proxy avg ms: 171.000
- prompt-to-playable proxy pairs found: 5 (missing: 33)
- prompt-to-playable proxy avg ms: 14514.000
- negative-latency pairs rejected: 2
- required before finalization: lock canonical edge dictionary and authoritative timestamp field.

### Failure Rate / Swap Rate (Current Proxies)
- distinct `ai_stream_events` error requests: 74
- distinct requests in `ai_stream_status`: 33
- distinct requests in `stream_trace_events`: 38
- error requests overlapping status denominator: 10
- error requests overlapping trace denominator: 38
- failure rate vs status denominator: 30.303%
- failure rate vs trace denominator: 100.000%
- state-based failure requests (non-ONLINE seen): 28
- state-based failure rate: 84.848%
- requests with non-empty orchestrator in trace: 5
- swap requests (>1 orchestrator): 0
- swap rate proxy: 0.000%
- required before finalization: classify error taxonomy and lock denominator contract (session vs request).

## Edge Mapping Gaps

- Lock canonical edge pairs for startup/e2e/prompt metrics.
- Decide authoritative time field for trace math: `data_timestamp` vs `event_timestamp`.
- Finalize failure denominator contract (`session` or `request`).
- Improve orchestrator coverage in trace to stabilize swap metric.
- Populate `pipeline_id` in curated tables for inference workflow segmentation.
