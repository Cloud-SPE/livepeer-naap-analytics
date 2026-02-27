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
 * - Legacy `swap_count` is retained as confirmed-only for backward-compatible consumers.
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
        state.pipeline = StringSemantics.firstNonBlank(state.pipeline, signal.pipeline);
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
        state.modelId = StringSemantics.blankToNull(snapshot.modelId);
        state.gpuId = StringSemantics.blankToNull(snapshot.gpuId);
    }

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
