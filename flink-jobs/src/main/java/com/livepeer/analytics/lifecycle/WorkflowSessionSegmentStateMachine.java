package com.livepeer.analytics.lifecycle;

import com.livepeer.analytics.model.EventPayloads;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Segment lifecycle transitions.
 *
 * <p>V1 boundary policy: open a new segment only when orchestrator identity changes.</p>
 *
 * <p>Non-boundary changes (v1): worker/gpu/parameter updates. These are tracked as attributes/markers,
 * not segment boundaries, to keep lifecycle segmentation semantically stable.</p>
 */
public final class WorkflowSessionSegmentStateMachine {
    private WorkflowSessionSegmentStateMachine() {}

    public static List<EventPayloads.FactWorkflowSessionSegment> applySignal(
            WorkflowSessionSegmentAccumulator state,
            LifecycleSignal signal,
            CapabilitySnapshotRef snapshot,
            long attributionTtlMs) {
        List<EventPayloads.FactWorkflowSessionSegment> out = new ArrayList<>(2);
        String canonicalFromSnapshot = snapshot == null ? "" : normalizeAddress(snapshot.canonicalOrchestratorAddress);
        String effectiveOrchestrator = canonicalFromSnapshot;

        if (!state.openSegment) {
            openSegment(state, signal, effectiveOrchestrator, "initial", signal.traceType);
            applySnapshot(state, snapshot, signal.signalTimestamp, attributionTtlMs);
            out.add(toFact(state));
            return out;
        }

        // Boundary rule (v1): non-empty orchestrator changed from one value to another.
        if (!isEmpty(effectiveOrchestrator)
                && !isEmpty(state.orchestratorAddress)
                && !normalizeAddress(state.orchestratorAddress).equals(effectiveOrchestrator)) {
            state.segmentEndTs = signal.signalTimestamp;
            state.reason = "orchestrator_change";
            state.sourceTraceType = firstNonEmpty(signal.traceType, state.sourceTraceType);
            state.sourceEventUid = firstNonEmpty(signal.sourceEventUid, state.sourceEventUid);
            state.version++;
            out.add(toFact(state));

            state.segmentIndex++;
            openSegment(state, signal, effectiveOrchestrator, "orchestrator_change", signal.traceType);
            applySnapshot(state, snapshot, signal.signalTimestamp, attributionTtlMs);
            out.add(toFact(state));
            return out;
        }

        // In-place updates (no boundary).
        state.gateway = firstNonEmpty(state.gateway, signal.gateway);
        if (!isEmpty(effectiveOrchestrator)) {
            state.orchestratorAddress = effectiveOrchestrator;
        }
        state.orchestratorUrl = firstNonEmpty(state.orchestratorUrl, signal.orchestratorUrl);
        state.sourceTraceType = firstNonEmpty(signal.traceType, state.sourceTraceType);
        state.sourceEventUid = firstNonEmpty(signal.sourceEventUid, state.sourceEventUid);
        applySnapshot(state, snapshot, signal.signalTimestamp, attributionTtlMs);

        // Explicit close edge finalizes current segment window.
        if ("gateway_ingest_stream_closed".equals(signal.traceType)) {
            state.segmentEndTs = signal.signalTimestamp;
            state.reason = "stream_closed";
        }

        state.version++;
        out.add(toFact(state));
        return out;
    }

    private static void openSegment(
            WorkflowSessionSegmentAccumulator state,
            LifecycleSignal signal,
            String effectiveOrchestrator,
            String reason,
            String sourceTraceType) {
        state.openSegment = true;
        state.workflowSessionId = signal.workflowSessionId;
        state.segmentStartTs = signal.signalTimestamp;
        state.segmentEndTs = null;
        state.gateway = firstNonEmpty(state.gateway, signal.gateway);
        state.orchestratorAddress = firstNonEmpty(effectiveOrchestrator, state.orchestratorAddress);
        state.orchestratorUrl = firstNonEmpty(signal.orchestratorUrl, state.orchestratorUrl);
        state.reason = reason;
        state.sourceTraceType = firstNonEmpty(sourceTraceType, "");
        state.sourceEventUid = firstNonEmpty(signal.sourceEventUid, state.sourceEventUid);
        state.version++;
    }

    private static void applySnapshot(
            WorkflowSessionSegmentAccumulator state,
            CapabilitySnapshotRef snapshot,
            long signalTs,
            long attributionTtlMs) {
        if (snapshot == null) {
            return;
        }

        long delta = Math.abs(signalTs - snapshot.snapshotTs);
        if (delta == 0) {
            state.attributionMethod = "exact_orchestrator_time_match";
            state.attributionConfidence = 1.0f;
        } else if (snapshot.snapshotTs <= signalTs && delta <= attributionTtlMs) {
            state.attributionMethod = "nearest_prior_snapshot";
            state.attributionConfidence = 0.9f;
        } else if (delta <= attributionTtlMs) {
            state.attributionMethod = "nearest_snapshot_within_ttl";
            state.attributionConfidence = 0.7f;
        } else {
            state.attributionMethod = "proxy_address_join";
            state.attributionConfidence = 0.4f;
        }

        if (!isEmpty(snapshot.canonicalOrchestratorAddress)) {
            state.orchestratorAddress = snapshot.canonicalOrchestratorAddress;
        }
        state.orchestratorUrl = firstNonEmpty(state.orchestratorUrl, snapshot.orchestratorUrl);
        state.modelId = emptyToNull(snapshot.modelId);
        state.gpuId = emptyToNull(snapshot.gpuId);
    }

    private static EventPayloads.FactWorkflowSessionSegment toFact(WorkflowSessionSegmentAccumulator state) {
        EventPayloads.FactWorkflowSessionSegment fact = new EventPayloads.FactWorkflowSessionSegment();
        fact.workflowSessionId = state.workflowSessionId;
        fact.segmentIndex = state.segmentIndex;
        fact.segmentStartTs = state.segmentStartTs;
        fact.segmentEndTs = state.segmentEndTs;
        fact.gateway = state.gateway;
        fact.orchestratorAddress = state.orchestratorAddress;
        fact.orchestratorUrl = state.orchestratorUrl;
        fact.workerId = state.workerId;
        fact.gpuId = state.gpuId;
        fact.modelId = state.modelId;
        fact.region = state.region;
        fact.attributionMethod = state.attributionMethod;
        fact.attributionConfidence = state.attributionConfidence;
        fact.reason = state.reason;
        fact.sourceTraceType = state.sourceTraceType;
        fact.sourceEventUid = state.sourceEventUid;
        fact.version = state.version;
        return fact;
    }

    private static String firstNonEmpty(String... values) {
        for (String value : values) {
            if (!isEmpty(value)) {
                return value;
            }
        }
        return "";
    }

    private static String emptyToNull(String value) {
        return isEmpty(value) ? null : value;
    }

    private static boolean isEmpty(String value) {
        return value == null || value.trim().isEmpty();
    }

    private static String normalizeAddress(String address) {
        return isEmpty(address) ? "" : address.trim().toLowerCase(Locale.ROOT);
    }
}
