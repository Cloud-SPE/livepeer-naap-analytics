# Workflow Lifecycle Edge Spec (Implementation Source of Truth)

## Purpose
Define the canonical lifecycle edge mapping, session identity rules, and classification logic used to build stateful facts:
- `fact_workflow_sessions`
- `fact_workflow_session_segments`

This spec is the implementation contract for Flink sessionization/classification code.

## Status
- `Locked`: sections that are implementation-ready.
- `Needs Clarification`: sections with `TODO (User Clarification)`.

---

## 1) Session Identity Contract

### 1.1 Canonical Session Key (`workflow_session_id`) (`Locked`)
Use deterministic composite identity to avoid `request_id` reuse collisions:

```text
if stream_id != '' and request_id != '' => stream_id + '|' + request_id
else if stream_id != ''                 => stream_id + '|_missing_request'
else if request_id != ''                => '_missing_stream|' + request_id
else                                    => '_missing_stream|_missing_request|' + hash(raw_json or event_id)
```

### 1.2 Workflow Type (`Needs Clarification`)
Default currently assumed:
- `ai_live_video_stream` for Live Video AI stream paths.
- future values: `inference`, `transform`, `batch`, `transcode`, `unknown`.

Mapping rules (v1):
- If session is built from live AI stream event family (`stream_trace`, `stream_ingest_metrics`, `ai_stream_events`, `ai_stream_status`), set `workflow_type = ai_live_video_stream`.
- `discovery_results` and `create_new_payment` are supporting facts and do not define workflow type directly.

USER CLARIFICATION:

Here are the mappings:

ai_stream events -> stream_trace, stream_ingest_metrics, ai_stream_events, and ai_stream_status

The following events are facts moreso than workflows in of themselves.  Workflows do instantiate them in order to proceed.

discovery_results -> used to find the available orchestrators that can support a given workflow/job type on the network
create_new_payment - used by the network to faciliate payments for workflows and jobs being conducted

---

## 2) Trace Edge Dictionary

### 2.1 Raw `trace_type` -> Normalized Category (`Locked`)
Initial normalization:
- `gateway_*` -> `gateway`
- `orchestrator_*` -> `orchestrator`
- `runner_*` -> `runner`
- `app_*` -> `app`
- else -> `other`

### 2.2 Lifecycle Signals (`Locked`)
- `gateway_receive_stream_request` -> known-stream denominator membership.
- `gateway_receive_first_processed_segment` -> first processed output at gateway (startup edge candidate).
- `gateway_receive_few_processed_segments` -> playable/stable startup success signal.
- `gateway_send_first_ingest_segment` -> first segment sent from gateway toward processing path.
- `runner_send_first_processed_segment` -> first processed segment emitted by runner.
- `gateway_ingest_stream_closed` -> terminal/supporting lifecycle signal.
- `gateway_no_orchestrators_available` -> explicit excused startup failure signal.
- `orchestrator_swap` -> explicit swap signal.

TODO (User Clarification):
- Confirm whether any additional `trace_type` values should be treated as terminal success/failure signals.

USER CLARIFICATION:

- at this time, no other type values should be treated as signals.  this may change as new types are added to the eventing model in go-livepeer

---

## 3) Metric Edge Pair Definitions

All formulas are computed at session grain (`workflow_session_id`) using first occurrence of each edge unless noted.

### 3.1 Startup Time Proxy (`Locked`)
- Start edge: `gateway_receive_stream_request`
- End edge: `gateway_receive_first_processed_segment`
- Formula:
```text
startup_ms = ts(first gateway_receive_first_processed_segment)
           - ts(first gateway_receive_stream_request)
```

### 3.2 E2E Latency Proxy (`Locked`)
- Start edge: `gateway_send_first_ingest_segment`
- End edge: `runner_send_first_processed_segment`
- Formula:
```text
e2e_proxy_ms = ts(first runner_send_first_processed_segment)
             - ts(first gateway_send_first_ingest_segment)
```

TODO (User Clarification):
- Confirm whether E2E should instead terminate at gateway receive (`gateway_receive_first_processed_segment`) for product reporting.

USER CLARIFICATION:

No, these edges are fine, but this conceptually e2e latency and startup time proxy are the same.  So this is more a measurement of the time it takes for the runner to produce the first usable response.  The difference
between this and the startup time proxy then allows us to identify the lag between when the runner first responded to when the gateway got the response (aka gateway <> runner latency).

### 3.3 Prompt-to-Playable Proxy (`Locked`)
- Prompt/start timestamp: `ai_stream_status.start_time` (minimum per session)
- End edge: `gateway_receive_few_processed_segments`
- Formula:
```text
prompt_to_playable_ms = ts(first gateway_receive_few_processed_segments)
                      - ts(min ai_stream_status.start_time)
```

TODO (User Clarification):
- Confirm whether prompt start should later move to explicit prompt-change event when available.

USER CLARIFICATION:

- ai_stream_events with a type field of 'params_update' indicates when the live video inference call received new input parameter (prompts, controlnets, etc.).  This might be worthy of tracking in the lifecycle so we can create metrics.

---

## 4) Startup Outcome Classification

Classification applies only to known streams (sessions containing `gateway_receive_stream_request`).

### 4.1 Classes (`Locked`)
- `success`
- `excused`
- `unexcused`
- `not_in_denominator` (operational/debug bucket only)

### 4.2 Rule Order (`Locked`)
Evaluate in order:
1. if `known_stream = 0` -> `not_in_denominator`
2. if startup success signal exists (`gateway_receive_few_processed_segments`) -> `success`
3. if `gateway_no_orchestrators_available` exists -> `excused`
4. if session has errors and all are excusable -> `excused`
5. else -> `unexcused`

### 4.3 Excusable Error Taxonomy (`Needs Clarification`)
Current draft patterns (case-insensitive contains):
- `no orchestrators available`
- `mediamtx ingest disconnected`
- `whip disconnected`
- `missing video`
- `ice connection state failed`
- `user disconnected`

TODO (User Clarification):
- Confirm approved excusable taxonomy list.
- Confirm whether taxonomy should be strict exact codes (preferred) vs message substring matching.

USER CLARIFICATION:

- These are confirmed.  The string is present as a substring in the event message so I don't think we can use exact string matches.  Perhaps I misunderstand what you mean by exact codes?  Please clarify.

---

## 5) Swap Detection Rules

### 5.1 Session Swap Flag (`Locked`)
A session is marked swapped if either:
- explicit swap event exists: `trace_type = orchestrator_swap`, or
- fallback condition: `uniq(orchestrator_address) > 1` within same `workflow_session_id`.

Formula:
```text
session_has_swap = (explicit_swap_events > 0) OR (orch_count > 1)
swap_rate = sessions_with_swaps / total_sessions
```

TODO (User Clarification):
- Confirm whether fallback should require ordered change over time (A -> B) rather than unique count only.

USER CLARIFICATION:

- Unique count should suffice.  We may to make sure the model normalizes between hot wallet and cold wallet addresses.  Most tables use the hot wallet rather than the authoritative wallet address for the orchestrator.  The authoritative one that maps to ENS names and the addresss most uses will recognize it found in the capabilities event data in the `local_address` field.  We should use this value in the facts and rollups to make querying easier.

Identity normalization rule (v1):
- Use canonical orchestrator identity in facts/rollups:
  - `canonical_orchestrator_address = network_capabilities.local_address` when mapping exists for observed hot wallet.
  - Fallback to observed event `orchestrator_address` when no mapping is available.

### 5.2 Segment Boundaries (`Needs Clarification`)
Draft:
- New segment starts when orchestrator/worker/gpu identity changes.
- `segment_index` increments per change in ordered edge stream.

TODO (User Clarification):
- Confirm exact identity-change fields that create a new segment:
  - `orchestrator_address` only?
  - `orchestrator_address + worker_id + gpu_id`?

USER CLARIFICATION:

  - `orchestrator_address` is the most definitive signal.  However, we want to understand how gpu id, worker id, and parameter updates might change the perofmrance of the stream as well.  We should consider how to add these dimensions.

Boundary policy (v1):
- Segment boundary is triggered by orchestrator identity change only.
- Worker/GPU/parameter changes do not open a new segment in v1.
- Worker/GPU values are tracked as segment/session attributes and enrichment fields.
- Parameter changes are tracked as lifecycle markers from `ai_stream_events` where `event_type = 'params_update'`.
- This enables before/after analysis for FPS/jitter relative to parameter update timestamps.

---

## 6) Attribution and Enrichment

### 6.1 Model/GPU Attribution (`Locked`)
For stream/session facts, enrich using nearest valid capability snapshot by orchestrator identity + time.

Fields:
- `model_id`
- `gpu_id`
- `gpu_attribution_method`
- `gpu_attribution_confidence`

### 6.2 Attribution Method Enum (`Needs Clarification`)
Draft values:
- `exact_orchestrator_time_match`
- `nearest_prior_snapshot`
- `nearest_snapshot_within_ttl`
- `proxy_address_join`
- `none`

TODO (User Clarification):
- Confirm enum set and acceptable max lag TTL for nearest snapshot attribution.

USER CLARIFICATION:
- This is fine.  For the max lag TTL, we can use a configurable window of 24 hours.

TTL rule (v1):
- Use configurable nearest-snapshot attribution window with default `24h`.

---

## 7) Grain and Fact Output Contract

### 7.1 `fact_workflow_sessions` (`Locked`)
Grain: `1 row / workflow_session_id`

Required outputs:
- identity: `workflow_session_id`, `stream_id`, `request_id`, `workflow_type`, `workflow_id`, `pipeline_id`
- timing: `session_start_ts`, `session_end_ts`, `first_stream_request_ts`, `first_processed_ts`, `first_playable_ts`
- classification: `known_stream`, `startup_success`, `startup_excused`, `startup_unexcused`
- reliability: `swap_count`, `error_count`, `excusable_error_count`
- attribution: `orchestrator_address`, `gateway`, `model_id`, `gpu_id`, `gpu_attribution_method`, `gpu_attribution_confidence`
- lineage/idempotency: `source_first_event_uid`, `source_last_event_uid`, `version`

### 7.2 `fact_workflow_session_segments` (`Needs Clarification`)
Grain: `1 row / (workflow_session_id, segment_index)`

Required outputs:
- `segment_start_ts`, `segment_end_ts`
- `gateway`, `orchestrator_address`, `orchestrator_url`, `worker_id`, `gpu_id`, `model_id`, `region`
- `reason`, `source_trace_type`, `source_event_uid`, `version`

TODO (User Clarification):
- Confirm whether `segment_end_ts` should be closed on next segment start or only explicit close edge.

USER CLARIFICATION:

- We can close the segment when the next segment starts or if no other segment start, the explicit edge close.

---

## 8) Open Questions Checklist (Fill Before Implementation)

1. Confirm `workflow_type` mapping rules.
2. Confirm final E2E endpoint edge (`runner_send_first_processed_segment` vs gateway receive edge).
3. Confirm prompt start source (`start_time` now, prompt-event later).
4. Confirm approved excusable error taxonomy and matching mode.
5. Confirm swap fallback rule strictness (unique count vs ordered transition).
6. Confirm segment boundary identity fields.
7. Confirm attribution TTL and final `gpu_attribution_method` enum.
8. Confirm segment close behavior (`segment_end_ts` policy).



Resolution status:
- Closed for v1. See sections 1.2, 5.1, 5.2, and 6.2.

---

## 9) Implementation Note

After this checklist is filled:
- Flink sessionization/classification operators should be implemented exactly against this spec.
- Any changes must update this doc and validation queries together:
  - `documentation/reports/METRICS_VALIDATION_QUERIES.sql`
