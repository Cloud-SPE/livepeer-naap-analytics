# Flink Lifecycle State Machines

This document explains how lifecycle state machines in Flink consume signals, mutate state, and emit facts.
It is implementation-oriented and intended to make the lifecycle codebase easier to reason about during debugging and changes.

## Scope and Ownership

- Source topology: `flink-jobs/src/main/java/com/livepeer/analytics/pipeline/StreamingEventsToClickHouse.java`
- Stateful operators:
  - `WorkflowSessionAggregatorFunction`
  - `WorkflowSessionSegmentAggregatorFunction`
  - `WorkflowParamUpdateAggregatorFunction`
- State logic:
  - `WorkflowSessionStateMachine`
  - `WorkflowSessionSegmentStateMachine`
  - `WorkflowLatencyDerivation`
- Shared signal model:
  - `LifecycleSignal`
- Shared capability enrichment state:
  - `CapabilityBroadcastState`
  - `CapabilitySnapshotRef`

## Lifecycle Inputs and Signal Construction

Lifecycle signals are built from three parsed streams and then unioned:

1. `ai_stream_status` -> `SignalType.STREAM_STATUS`
2. `stream_trace_events` -> `SignalType.STREAM_TRACE`
3. `ai_stream_events` -> `SignalType.AI_STREAM_EVENT`

Signal constructors:

- `toLifecycleSignalFromStatus(...)`
- `toLifecycleSignalFromTrace(...)`
- `toLifecycleSignalFromAiEvent(...)`

Common signal fields include:

- identity: `workflowSessionId`, `streamId`, `requestId`
- time: `signalTimestamp`, `ingestTimestamp`
- context: `pipeline`, `gateway`, `orchestratorAddress`, `orchestratorUrl`
- event-specific: `traceType`, `aiEventType`, `message`, `startTimeMs`

## Processing Order (Per Lifecycle Signal)

For each keyed lifecycle signal (`key = workflow_session_id`):

1. Read/create keyed accumulator state.
2. Lookup capability snapshot from broadcast state using observed hot wallet (`signal.orchestratorAddress`).
3. Apply state transition logic.
4. Apply capability attribution logic (when snapshot is available and compatible).
5. Increment/update versioned state.
6. Emit upsert row(s) to fact table(s).

Note: each lifecycle fact table uses `ReplacingMergeTree(version)` semantics in ClickHouse, so higher `version` is the latest row for a given logical key.

## Broadcast Capability Enrichment

Capability events (`network_capabilities`) are broadcast and cached by hot wallet key:

- map key: normalized `local_address`
- value: `CapabilitySnapshotRef` containing:
  - `snapshotTs`
  - `canonicalOrchestratorAddress`
  - `orchestratorUrl`
  - `modelId`
  - `gpuId`

Important behavior:

- cache is one snapshot per hot wallet key (last write wins).
- attribution applies only when signal has non-empty observed wallet key.

## Session State Machine (`fact_workflow_sessions`)

Primary logic class: `WorkflowSessionStateMachine`.

### What it tracks

- session boundaries/time windows (`sessionStartTs`, `sessionEndTs`)
- lifecycle evidence (`firstStreamRequestTs`, `firstProcessedTs`, `firstPlayableTs`)
- startup classification counters/flags
- swap evidence (`explicitSwapCount`, canonical orchestrator set)
- attribution fields (`modelId`, `gpuId`, `gpu_attribution_method`, `gpu_attribution_confidence`)
- monotonic `version`

### Signal effects

- `STREAM_TRACE`:
  - updates edge markers by `traceType`
  - controls known-stream membership and startup success evidence
  - records explicit swap signal when `traceType = orchestrator_swap`
- `AI_STREAM_EVENT`:
  - increments error counters
  - classifies error as excusable via substring taxonomy
- `STREAM_STATUS`:
  - updates identity/context fields and timestamps
  - does not directly drive startup classification edges

### Startup classification order

1. success (`first playable` observed)
2. excused (no orchestrators signal or all errors excusable)
3. unexcused (known stream, not success, not excused)

### Swap classification

- swapped if explicit swap signal exists OR unique canonical orchestrator count > 1.

## Segment State Machine (`fact_workflow_session_segments`)

Primary logic class: `WorkflowSessionSegmentStateMachine`.

### Segment policy (v1)

- segment boundary only on orchestrator identity change.
- parameter updates and non-boundary context changes stay in same segment.

### Output behavior

- can emit two rows on boundary event:
  1. close previous segment
  2. open new segment
- otherwise emits one updated segment row.

## Param Update Aggregator (`fact_workflow_param_updates`)

Primary logic class: `WorkflowParamUpdateAggregatorFunction`.

Behavior:

- tracks lightweight per-session context.
- emits rows only for `AI_STREAM_EVENT` where `eventType = params_update`.
- reuses capability enrichment path for canonical orchestrator + model/gpu context.

## Latency Samples (`fact_workflow_latency_samples`)

Latency samples are derived from latest session snapshots via `WorkflowLatencyDerivation`.

Key points:

- emitted from session fact stream (not directly from raw edges).
- carries pipeline/model/gpu context present on session snapshot at emission time.
- includes presence flags (`has_*`) to distinguish missing metric vs zero value.

## GPU Attribution: Decision Flow

Attribution is snapshot-based with TTL bands:

1. exact timestamp match -> method `exact_orchestrator_time_match`, confidence `1.0`
2. prior snapshot within TTL -> `nearest_prior_snapshot`, confidence `0.9`
3. any snapshot within TTL -> `nearest_snapshot_within_ttl`, confidence `0.7`
4. stale mapping only -> `proxy_address_join`, confidence `0.4` (no model/gpu overwrite)
5. no snapshot -> `none`, confidence `0.0`

Compatibility guard (current behavior):

- capability snapshot can overwrite `model_id` / `gpu_id` only when:
  - session `pipeline` is empty, or
  - snapshot `modelId` is empty, or
  - `pipeline == modelId` (case-insensitive misnomer-compatible match)

If not compatible:

- canonical orchestrator normalization may still apply,
- but `model_id` / `gpu_id` attribution overwrite is skipped.

This prevents contradictory `(pipeline, model_id)` pairs in lifecycle facts.

## Outputs and Table Mapping

- `WorkflowSessionAggregatorFunction` -> `fact_workflow_sessions`
- `WorkflowSessionSegmentAggregatorFunction` -> `fact_workflow_session_segments`
- `WorkflowParamUpdateAggregatorFunction` -> `fact_workflow_param_updates`
- `WorkflowLatencyDerivation` output stream -> `fact_workflow_latency_samples`
- `WorkflowLifecycleCoverageAggregatorFunction` -> `fact_lifecycle_edge_coverage`

## Debugging Playbook

For one suspicious `workflow_session_id`:

1. Inspect raw timeline:
   - `ai_stream_status`
   - `stream_trace_events`
   - `ai_stream_events`
2. Inspect version history:
   - `fact_workflow_sessions` ordered by `version`
   - `fact_workflow_session_segments` ordered by `(segment_index, version)`
3. Inspect nearby capability snapshots by hot wallet:
   - `network_capabilities` by `local_address`
4. Verify attribution fields:
   - `gpu_attribution_method`
   - `gpu_attribution_confidence`
   - `model_id`, `gpu_id`
5. Verify API projection impact:
   - `v_api_gpu_metrics`
   - `v_api_sla_compliance`

## Change Safety Checklist

When changing lifecycle behavior:

1. Update this document if state transition or attribution semantics change.
2. Update `docs/data/SCHEMA_AND_METRIC_CONTRACTS.md` for contract-level semantics.
3. Add/adjust unit tests under `flink-jobs/src/test/java/com/livepeer/analytics/lifecycle`.
4. Re-run:
   - `cd flink-jobs && mvn test`
   - `tests/integration/run_all.sh`
5. Re-check parity between lifecycle facts and API views using integration SQL packs.
