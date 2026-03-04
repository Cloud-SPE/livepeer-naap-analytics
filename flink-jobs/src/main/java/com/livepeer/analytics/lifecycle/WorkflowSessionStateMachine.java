package com.livepeer.analytics.lifecycle;

import com.livepeer.analytics.model.EventPayloads;
import com.livepeer.analytics.util.AddressNormalizer;
import com.livepeer.analytics.util.StringSemantics;

import java.util.Locale;

/**
 * Deterministic lifecycle state transition logic for workflow sessions.
 *
 * <p>Invariant:
 * - Classification precedence is fixed as:
 *   success > excused > unexcused (for known streams).
 * - Swap signals are split into:
 *   confirmed swap count (explicit trace evidence) and
 *   inferred orchestrator-change count (canonical orchestrator set cardinality).
 * - Compatibility alias `swap_count` is retained as confirmed-only for backward-compatible consumers.
 * </p>
 */
public final class WorkflowSessionStateMachine {
    private static final String[] EXCUSABLE_SUBSTRINGS = new String[] {
            "no orchestrators available",
            "mediamtx ingest disconnected",
            "whip disconnected",
            "missing video",
            "ice connection state failed",
            "user disconnected"
    };

    private WorkflowSessionStateMachine() {}

    /**
     * Apply one normalized lifecycle signal to a mutable workflow-session accumulator.
     */
    public static void applySignal(WorkflowSessionAccumulator state, LifecycleSignal signal) {
        state.workflowSessionId = StringSemantics.firstNonBlank(state.workflowSessionId, signal.workflowSessionId);
        state.streamId = StringSemantics.firstNonBlank(state.streamId, signal.streamId);
        state.requestId = StringSemantics.firstNonBlank(state.requestId, signal.requestId);
        // Session pipeline is intentionally first-write-wins within a session. Upstream operators
        // are responsible for canonicalizing signal.pipeline before invoking the state machine.
        state.pipeline = StringSemantics.firstNonBlank(state.pipeline, signal.pipeline);
        state.modelId = StringSemantics.blankToNull(StringSemantics.firstNonBlank(state.modelId, signal.modelHint));
        if (!StringSemantics.isBlank(state.pipeline)) {
            state.pipelinesSeen.add(state.pipeline);
        }
        if (!StringSemantics.isBlank(state.modelId)) {
            state.modelIdsSeen.add(state.modelId);
        }
        state.workflowId = StringSemantics.firstNonBlank(state.workflowId, state.pipeline, "ai_live_video_stream");
        state.gateway = StringSemantics.firstNonBlank(state.gateway, signal.gateway);
        if (!StringSemantics.isBlank(signal.orchestratorUrl)) {
            state.orchestratorUrl = signal.orchestratorUrl;
        }

        long ts = signal.signalTimestamp;
        state.sessionStartTs = Math.min(state.sessionStartTs, ts);
        state.sessionEndTs = Math.max(state.sessionEndTs, ts);
        if (signal.startTimeMs != null) {
            state.sessionStartTs = Math.min(state.sessionStartTs, signal.startTimeMs);
        }
        state.eventCount++;
        state.version++;

        if (StringSemantics.isBlank(state.sourceFirstEventUid)) {
            state.sourceFirstEventUid = signal.sourceEventUid;
        }
        state.sourceLastEventUid = signal.sourceEventUid;

        if (signal.signalType == LifecycleSignal.SignalType.STREAM_TRACE) {
            applyTrace(state, signal);
        } else if (signal.signalType == LifecycleSignal.SignalType.AI_STREAM_EVENT) {
            applyAiEvent(state, signal);
        } else if (signal.signalType == LifecycleSignal.SignalType.STREAM_STATUS) {
            applyStatus(state, signal);
        }
    }

    public static void applyCapabilityAttribution(
            WorkflowSessionAccumulator state,
            CapabilitySnapshotRef snapshot,
            long signalTs,
            long attributionTtlMs) {
        if (snapshot == null) {
            return;
        }

        // Canonical-only contract: never persist signal hot-wallet addresses into lifecycle facts.
        if (!StringSemantics.isBlank(snapshot.canonicalOrchestratorAddress)) {
            state.orchestratorAddress = snapshot.canonicalOrchestratorAddress;
            state.orchestratorsSeen.add(AddressNormalizer.normalizeOrEmpty(snapshot.canonicalOrchestratorAddress));
            state.canonicalOrchestratorsSeen.add(AddressNormalizer.normalizeOrEmpty(snapshot.canonicalOrchestratorAddress));
        }
        state.orchestratorUrl = StringSemantics.firstNonBlank(state.orchestratorUrl, snapshot.orchestratorUrl);
        AttributionSemantics.Decision decision =
                AttributionSemantics.fromSnapshot(signalTs, snapshot.snapshotTs, attributionTtlMs);
        state.attributionMethod = decision.method;
        state.attributionConfidence = decision.confidence;
        // When capability evidence exists, model/gpu attribution is authoritative for lifecycle facts
        // and intentionally overrides prior signal-derived hints.
        state.modelId = StringSemantics.blankToNull(snapshot.modelId);
        state.gpuId = StringSemantics.blankToNull(snapshot.gpuId);
        if (!StringSemantics.isBlank(state.modelId)) {
            state.modelIdsSeen.add(state.modelId);
        }
        if (!StringSemantics.isBlank(state.pipeline)) {
            state.pipelinesSeen.add(state.pipeline);
        }
    }

    /**
     * Materializes the latest accumulator state into the canonical session fact.
     *
     * <p>Key invariants:
     * - classification precedence remains success > excused > unexcused;
     * - `swap_count` is compatibility-only and mirrors confirmed swap evidence;
     * - health counters are derived from emitted lifecycle signals, not inferred at query time.</p>
     */
    public static EventPayloads.FactWorkflowSession toFact(WorkflowSessionAccumulator state) {
        EventPayloads.FactWorkflowSession fact = new EventPayloads.FactWorkflowSession();

        fact.workflowSessionId = state.workflowSessionId;
        fact.workflowType = state.workflowType;
        fact.workflowId = state.workflowId;
        fact.streamId = state.streamId;
        fact.requestId = state.requestId;
        fact.sessionId = state.sessionId;
        fact.pipeline = state.pipeline;
        fact.gateway = state.gateway;
        fact.orchestratorAddress = state.orchestratorAddress;
        fact.orchestratorUrl = state.orchestratorUrl;
        fact.modelId = state.modelId;
        fact.gpuId = state.gpuId;
        fact.region = state.region;
        fact.hasModelChange = state.modelIdsSeen.size() > 1 ? 1 : 0;
        fact.hasPipelineChange = state.pipelinesSeen.size() > 1 ? 1 : 0;
        fact.attributionMethod = state.attributionMethod;
        fact.attributionConfidence = state.attributionConfidence;

        fact.sessionStartTs = state.sessionStartTs == Long.MAX_VALUE ? 0L : state.sessionStartTs;
        fact.sessionEndTs = state.sessionEndTs == Long.MIN_VALUE ? null : state.sessionEndTs;
        fact.firstStreamRequestTs = state.firstStreamRequestTs;
        fact.firstProcessedTs = state.firstProcessedTs;
        fact.firstPlayableTs = state.firstPlayableTs;

        fact.knownStream = state.knownStream ? 1 : 0;
        fact.startupSuccess = state.startupSuccess ? 1 : 0;
        // Classification precedence:
        // 1) startup_success
        // 2) explicit excused conditions
        // 3) unexcused fallback for known streams
        boolean startupExcused = state.knownStream && !state.startupSuccess
                && (state.noOrchestratorsAvailable
                || (state.errorCount > 0 && state.errorCount == state.excusableErrorCount));
        boolean startupUnexcused = state.knownStream && !state.startupSuccess && !startupExcused;
        fact.startupExcused = startupExcused ? 1 : 0;
        fact.startupUnexcused = startupUnexcused ? 1 : 0;

        long canonicalCount = state.canonicalOrchestratorsSeen.size();
        long inferredOrchestratorChangeCount = Math.max(0L, canonicalCount - 1L);
        // `swap_count` remains for backwards compatibility, but now tracks confirmed swaps only.
        // Inferred multi-orchestrator evidence is emitted separately.
        fact.confirmedSwapCount = (int) Math.max(0L, state.explicitSwapCount);
        fact.inferredOrchestratorChangeCount = (int) inferredOrchestratorChangeCount;
        fact.swapCount = fact.confirmedSwapCount;
        fact.errorCount = state.errorCount;
        fact.excusableErrorCount = state.excusableErrorCount;
        fact.lastErrorOccurred = state.lastErrorOccurred ? 1 : 0;
        fact.loadingOnlySession = state.statusSampleCount > 0 && state.statusNonLoadingSampleCount == 0 ? 1 : 0;
        fact.zeroOutputFpsSession = state.statusSampleCount > 0 && state.statusPositiveOutputSampleCount == 0 ? 1 : 0;
        fact.statusSampleCount = state.statusSampleCount;
        fact.statusErrorSampleCount = state.statusErrorSampleCount;
        long healthExpectedSignalCount = state.knownStream ? 3L : 0L;
        long healthSignalCount = 0L;
        if (state.statusSampleCount > 0) {
            healthSignalCount++;
        }
        if (state.firstProcessedTs != null) {
            healthSignalCount++;
        }
        if (state.firstPlayableTs != null) {
            healthSignalCount++;
        }
        fact.healthSignalCount = healthSignalCount;
        fact.healthExpectedSignalCount = healthExpectedSignalCount;
        fact.healthCompletenessRatio = healthExpectedSignalCount > 0
                ? (float) ((double) healthSignalCount / (double) healthExpectedSignalCount)
                : 1.0f;
        fact.eventCount = state.eventCount;
        fact.version = state.version;
        fact.sourceFirstEventUid = state.sourceFirstEventUid;
        fact.sourceLastEventUid = state.sourceLastEventUid;
        return fact;
    }

    private static void applyTrace(WorkflowSessionAccumulator state, LifecycleSignal signal) {
        String traceType = StringSemantics.firstNonBlank(signal.traceType, "");
        long ts = signal.signalTimestamp;
        if ("gateway_receive_stream_request".equals(traceType)) {
            state.knownStream = true;
            state.firstStreamRequestTs = minNullable(state.firstStreamRequestTs, ts);
        } else if ("gateway_receive_first_processed_segment".equals(traceType)) {
            state.firstProcessedTs = minNullable(state.firstProcessedTs, ts);
        } else if ("gateway_receive_few_processed_segments".equals(traceType)) {
            state.startupSuccess = true;
            state.firstPlayableTs = minNullable(state.firstPlayableTs, ts);
        } else if ("gateway_no_orchestrators_available".equals(traceType)) {
            state.noOrchestratorsAvailable = true;
        } else if ("orchestrator_swap".equals(traceType)) {
            state.explicitSwapCount++;
        }
    }

    /**
     * Applies AI stream event semantics that affect session health/classification only.
     *
     * <p>Note: this does not mutate attribution identity fields.</p>
     */
    private static void applyAiEvent(WorkflowSessionAccumulator state, LifecycleSignal signal) {
        String eventType = StringSemantics.firstNonBlank(signal.aiEventType, "");
        if ("error".equalsIgnoreCase(eventType)) {
            state.errorCount++;
            if (isExcusableError(signal.message)) {
                state.excusableErrorCount++;
            }
        } else if ("params_update".equalsIgnoreCase(eventType)) {
            state.paramsUpdateCount++;
        }
    }

    private static void applyStatus(WorkflowSessionAccumulator state, LifecycleSignal signal) {
        state.statusSampleCount++;
        String statusState = StringSemantics.firstNonBlank(signal.statusState, "");
        // LOADING-only sessions are treated as weak health signal and are tracked explicitly
        // for SLA/forensics, rather than being collapsed into generic startup failure buckets.
        if (!"LOADING".equalsIgnoreCase(statusState)) {
            state.statusNonLoadingSampleCount++;
        }
        float outputFps = signal.statusOutputFps == null ? 0.0f : signal.statusOutputFps;
        if (outputFps > 0.0f) {
            state.statusPositiveOutputSampleCount++;
        }
        if (!StringSemantics.isBlank(signal.statusLastError)) {
            state.lastErrorOccurred = true;
            state.statusErrorSampleCount++;
        }
    }

    private static boolean isExcusableError(String message) {
        if (StringSemantics.isBlank(message)) {
            return false;
        }
        String normalized = message.toLowerCase(Locale.ROOT);
        for (String pattern : EXCUSABLE_SUBSTRINGS) {
            if (normalized.contains(pattern)) {
                return true;
            }
        }
        return false;
    }

    private static Long minNullable(Long existing, long candidate) {
        if (existing == null) {
            return candidate;
        }
        return Math.min(existing, candidate);
    }
}
