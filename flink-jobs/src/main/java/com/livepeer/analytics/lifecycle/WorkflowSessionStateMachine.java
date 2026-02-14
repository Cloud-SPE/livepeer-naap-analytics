package com.livepeer.analytics.lifecycle;

import com.livepeer.analytics.model.EventPayloads;

import java.util.Locale;

/**
 * Deterministic lifecycle state transition logic for workflow sessions.
 *
 * <p>Invariant:
 * - Classification precedence is fixed as:
 *   success > excused > unexcused (for known streams).
 * - Swap fallback count is derived from canonical orchestrator identity when available
 *   to avoid hot/cold wallet address drift.
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
        state.workflowSessionId = firstNonEmpty(state.workflowSessionId, signal.workflowSessionId);
        state.streamId = firstNonEmpty(state.streamId, signal.streamId);
        state.requestId = firstNonEmpty(state.requestId, signal.requestId);
        state.pipeline = firstNonEmpty(state.pipeline, signal.pipeline);
        state.pipelineId = firstNonEmpty(state.pipelineId, signal.pipelineId);
        state.workflowId = !isEmpty(state.pipelineId)
                ? state.pipelineId
                : firstNonEmpty(state.workflowId, state.pipeline, "ai_live_video_stream");
        state.gateway = firstNonEmpty(state.gateway, signal.gateway);
        if (!isEmpty(signal.orchestratorAddress)) {
            state.orchestratorAddress = signal.orchestratorAddress;
        }
        if (!isEmpty(signal.orchestratorUrl)) {
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

        if (isEmpty(state.sourceFirstEventUid)) {
            state.sourceFirstEventUid = signal.sourceEventUid;
        }
        state.sourceLastEventUid = signal.sourceEventUid;

        if (!isEmpty(signal.orchestratorAddress)) {
            state.orchestratorsSeen.add(normalizeAddress(signal.orchestratorAddress));
        } else if (!isEmpty(state.orchestratorAddress)) {
            state.orchestratorsSeen.add(normalizeAddress(state.orchestratorAddress));
        }

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

        if (!isEmpty(snapshot.canonicalOrchestratorAddress)) {
            state.orchestratorAddress = snapshot.canonicalOrchestratorAddress;
            state.orchestratorsSeen.add(normalizeAddress(snapshot.canonicalOrchestratorAddress));
            state.canonicalOrchestratorsSeen.add(normalizeAddress(snapshot.canonicalOrchestratorAddress));
        }
        state.orchestratorUrl = firstNonEmpty(state.orchestratorUrl, snapshot.orchestratorUrl);

        long delta = Math.abs(signalTs - snapshot.snapshotTs);
        if (delta == 0) {
            state.attributionMethod = "exact_orchestrator_time_match";
            state.attributionConfidence = 1.0f;
            state.modelId = emptyToNull(snapshot.modelId);
            state.gpuId = emptyToNull(snapshot.gpuId);
            return;
        }

        if (delta <= attributionTtlMs && snapshot.snapshotTs <= signalTs) {
            state.attributionMethod = "nearest_prior_snapshot";
            state.attributionConfidence = 0.9f;
            state.modelId = emptyToNull(snapshot.modelId);
            state.gpuId = emptyToNull(snapshot.gpuId);
            return;
        }

        if (delta <= attributionTtlMs) {
            state.attributionMethod = "nearest_snapshot_within_ttl";
            state.attributionConfidence = 0.7f;
            state.modelId = emptyToNull(snapshot.modelId);
            state.gpuId = emptyToNull(snapshot.gpuId);
            return;
        }

        // Mapping is known, but snapshot is stale for model/GPU attribution.
        state.attributionMethod = "proxy_address_join";
        state.attributionConfidence = 0.4f;
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
        fact.pipelineId = state.pipelineId;
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

        long canonicalCount = state.canonicalOrchestratorsSeen.isEmpty()
                ? state.orchestratorsSeen.size()
                : state.canonicalOrchestratorsSeen.size();
        long fallbackSwapCount = Math.max(0L, canonicalCount - 1L);
        fact.swapCount = (int) Math.max(state.explicitSwapCount, fallbackSwapCount);
        fact.errorCount = state.errorCount;
        fact.excusableErrorCount = state.excusableErrorCount;
        fact.eventCount = state.eventCount;
        fact.version = state.version;
        fact.sourceFirstEventUid = state.sourceFirstEventUid;
        fact.sourceLastEventUid = state.sourceLastEventUid;
        return fact;
    }

    private static void applyTrace(WorkflowSessionAccumulator state, LifecycleSignal signal) {
        String traceType = firstNonEmpty(signal.traceType, "");
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
        String eventType = firstNonEmpty(signal.aiEventType, "");
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
        if (isEmpty(message)) {
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

    private static String normalizeAddress(String address) {
        return isEmpty(address) ? "" : address.trim().toLowerCase(Locale.ROOT);
    }

    private static Long minNullable(Long existing, long candidate) {
        if (existing == null) {
            return candidate;
        }
        return Math.min(existing, candidate);
    }

    private static String emptyToNull(String value) {
        return isEmpty(value) ? null : value;
    }

    private static String firstNonEmpty(String... values) {
        for (String value : values) {
            if (!isEmpty(value)) {
                return value;
            }
        }
        return "";
    }

    private static boolean isEmpty(String value) {
        return value == null || value.trim().isEmpty();
    }
}
