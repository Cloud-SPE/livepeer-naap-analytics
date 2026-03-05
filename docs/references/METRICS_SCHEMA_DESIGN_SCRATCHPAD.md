> **Status:** Working backlog document. Canonical contracts are in [`docs/data/SCHEMA_AND_METRIC_CONTRACTS.md`](../data/SCHEMA_AND_METRIC_CONTRACTS.md).

# Metrics Schema Design Scratchpad

## Key Metrics to Enable

| Metric Name | Category | Dimensions | Measure | Status | Notes |
|---|---|---|---|---|---|
| Up/Down Bandwidth | Network | wallet, gpu_id, workflow, region, time | Mbps | Blocked | Requires dedicated telemetry source. |
| Startup Time | Performance | wallet, gpu_id, workflow, region, time | s/ms | Live | Proxy from lifecycle edge pairs. |
| E2E Stream Latency | Performance | wallet, gpu_id, workflow, region, time | ms | Live/Derived | Proxy from lifecycle edge pairs. |
| Prompt-to-First-Frame / Playable | Performance | wallet, gpu_id, workflow, region, time | ms | Live/Derived | Derived from status start + playable edge. |
| Jitter Coefficient | Performance | wallet, gpu_id, workflow, region, time | `stddev(fps)/avg(fps)` | Live | Based on output FPS samples. |
| Output FPS | Performance | wallet, gpu_id, workflow, region, time | FPS | Live | Base KPI from status samples. |
| Failure Rate | Reliability | wallet, gpu_id, workflow, region, time | % | Live | Uses startup classification contract. |
| Swap Rate | Reliability | wallet, gpu_id, workflow, region, time | % | Live | Uses explicit swap + unique orchestrator fallback. |

## Key APIs to Enable

| API Endpoint | Backing View | Canonical Grain | Core Additive Fields | Core Derived Fields | Status |
|---|---|---|---|---|---|
| `/gpu/metrics` | `v_api_gpu_metrics` | 1h (serving rollups may expose finer windows) | `status_samples`, `known_sessions_count`, `startup_success_sessions`, `excused_sessions`, `startup_unexcused_sessions`, `total_swapped_sessions` | `avg_output_fps`, `p95_output_fps`, `fps_jitter_coefficient`, `startup_unexcused_rate`, `swap_rate` | In progress |
| `/network/demand` | `v_api_network_demand` | 1h `(gateway, region, pipeline_id, model_id)` | `sessions_count`, `total_minutes`, `known_sessions_count`, `startup_unexcused_sessions`, `total_swapped_sessions`, `sessions_with_errors`, `loading_only_sessions`, `zero_output_fps_sessions`, `ticket_face_value_eth` | `avg_output_fps`, `startup_success_rate`, `effective_success_rate` | Live |
| `/sla/compliance` | `v_api_sla_compliance` | 1h `(orchestrator, pipeline_id, model_id, gpu_id, region)` | `known_sessions_count`, `startup_unexcused_sessions`, `total_swapped_sessions`, `sessions_with_errors`, `loading_only_sessions`, `zero_output_fps_sessions`, `health_signal_count`, `health_expected_signal_count` | `startup_success_rate`, `effective_success_rate`, `no_swap_rate`, `health_signal_coverage_ratio`, `sla_score` | Live |

Notes:
- API payloads should remain rollup-safe: recompute ratios/scores from additive fields for larger windows.
- `v_api_network_demand` is now model-aware; downstream consumers must include `model_id` in joins or pre-aggregate to `pipeline_id` grain before joining to pipeline-level datasets.
- Hard-cutover lineage contract: raw typed tables now use Flink-owned `raw_event_uid`; downstream lineage joins use persisted UID fields (`raw_event_uid` <-> `source_event_uid`) and no longer depend on ClickHouse-side hashing.
- `/datasets` remains out of scope.

## Transformation Mapping (Trace -> Status -> Metrics)

### Session Identity
- Primary validation/session key: `session_key = stream_id + '|' + request_id` (with deterministic missing-side fallback).
- Reason: `request_id` alone is reusable; `stream_id` alone can mask multiple attempts.

### Trace Subtype Mapping (Current)
- `gateway_receive_stream_request` -> known-stream denominator membership.
- `gateway_receive_few_processed_segments` -> startup success signal.
- `gateway_no_orchestrators_available` -> excused startup failure signal.
- `gateway_ingest_stream_closed` -> terminal/supporting lifecycle signal.
- `orchestrator_swap` -> explicit swap signal.

### Error Mapping (Current)
- Error source: `ai_stream_events` where `event_type = 'error'`.
- Excusable taxonomy seed (substring match):
  - `no orchestrators available`
  - `mediamtx ingest disconnected`
  - `whip disconnected`
  - `missing video`
  - `ice connection state failed`
  - `user disconnected`

### Status Classification Rules (Current Proxy)
- `success`: startup success signal present.
- `excused`: no-orchestrator signal present OR all session errors are excusable.
- `unexcused`: known stream and not classified as success/excused.

## Outstanding Areas for Improvement (BACKLOG)

- Canonical backlog is tracked in [`docs/references/ISSUES_BACKLOG.md`](./ISSUES_BACKLOG.md).
- Keep this scratchpad as working context only; add and update open items in the canonical backlog file.
