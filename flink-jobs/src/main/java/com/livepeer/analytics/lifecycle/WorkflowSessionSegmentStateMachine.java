package com.livepeer.analytics.lifecycle;

import com.livepeer.analytics.model.EventPayloads;
import com.livepeer.analytics.util.AddressNormalizer;
import com.livepeer.analytics.util.StringSemantics;

import java.util.ArrayList;
import java.util.List;

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
        String canonicalFromSnapshot = snapshot == null ? "" : AddressNormalizer.normalizeOrEmpty(snapshot.canonicalOrchestratorAddress);
        String effectiveOrchestrator = canonicalFromSnapshot;

        if (!state.openSegment) {
            openSegment(state, signal, effectiveOrchestrator, "initial", signal.traceType);
            applySnapshot(state, snapshot, signal.signalTimestamp, attributionTtlMs);
            out.add(toFact(state));
            return out;
        }

        // Boundary rule (v1): non-empty orchestrator changed from one value to another.
        if (!StringSemantics.isBlank(effectiveOrchestrator)
                && !StringSemantics.isBlank(state.orchestratorAddress)
                && !AddressNormalizer.normalizeOrEmpty(state.orchestratorAddress).equals(effectiveOrchestrator)) {
            state.segmentEndTs = signal.signalTimestamp;
            state.reason = "orchestrator_change";
            state.sourceTraceType = StringSemantics.firstNonBlank(signal.traceType, state.sourceTraceType);
            state.sourceEventUid = StringSemantics.firstNonBlank(signal.sourceEventUid, state.sourceEventUid);
            state.version++;
            out.add(toFact(state));

            state.segmentIndex++;
            openSegment(state, signal, effectiveOrchestrator, "orchestrator_change", signal.traceType);
            applySnapshot(state, snapshot, signal.signalTimestamp, attributionTtlMs);
            out.add(toFact(state));
            return out;
        }

        // In-place updates (no boundary).
        state.gateway = StringSemantics.firstNonBlank(state.gateway, signal.gateway);
        state.pipeline = StringSemantics.firstNonBlank(state.pipeline, signal.pipeline);
        if (!StringSemantics.isBlank(effectiveOrchestrator)) {
            state.orchestratorAddress = effectiveOrchestrator;
        }
        state.orchestratorUrl = StringSemantics.firstNonBlank(signal.orchestratorUrl, state.orchestratorUrl);
        state.sourceTraceType = StringSemantics.firstNonBlank(signal.traceType, state.sourceTraceType);
        state.sourceEventUid = StringSemantics.firstNonBlank(signal.sourceEventUid, state.sourceEventUid);
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
        state.gateway = StringSemantics.firstNonBlank(state.gateway, signal.gateway);
        state.pipeline = StringSemantics.firstNonBlank(state.pipeline, signal.pipeline);
        state.orchestratorAddress = StringSemantics.firstNonBlank(effectiveOrchestrator, state.orchestratorAddress);
        state.orchestratorUrl = StringSemantics.firstNonBlank(signal.orchestratorUrl, state.orchestratorUrl);
        state.reason = reason;
        state.sourceTraceType = StringSemantics.firstNonBlank(sourceTraceType, "");
        state.sourceEventUid = StringSemantics.firstNonBlank(signal.sourceEventUid, state.sourceEventUid);
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

        AttributionSemantics.Decision decision =
                AttributionSemantics.fromSnapshot(signalTs, snapshot.snapshotTs, attributionTtlMs);
        state.attributionMethod = decision.method;
        state.attributionConfidence = decision.confidence;

        // Canonical-only contract: segment attribution never falls back to signal hot wallet.
        if (!StringSemantics.isBlank(snapshot.canonicalOrchestratorAddress)) {
            state.orchestratorAddress = snapshot.canonicalOrchestratorAddress;
        }
        state.orchestratorUrl = StringSemantics.firstNonBlank(snapshot.orchestratorUrl, state.orchestratorUrl);
        state.modelId = StringSemantics.blankToNull(snapshot.modelId);
        state.gpuId = StringSemantics.blankToNull(snapshot.gpuId);
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

}
