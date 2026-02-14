package com.livepeer.analytics.lifecycle;

import com.livepeer.analytics.model.EventPayloads;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkflowSessionStateMachineTest {

    @Test
    void classifiesKnownStreamSuccessAndSwapFallback() {
        WorkflowSessionAccumulator state = new WorkflowSessionAccumulator();

        LifecycleSignal streamRequest = signal(LifecycleSignal.SignalType.STREAM_TRACE, 1000L);
        streamRequest.workflowSessionId = "s|r";
        streamRequest.streamId = "s";
        streamRequest.requestId = "r";
        streamRequest.traceType = "gateway_receive_stream_request";
        streamRequest.orchestratorAddress = "0xhot1";
        streamRequest.sourceEventUid = "e1";

        WorkflowSessionStateMachine.applySignal(state, streamRequest);

        LifecycleSignal firstPlayable = signal(LifecycleSignal.SignalType.STREAM_TRACE, 2000L);
        firstPlayable.workflowSessionId = "s|r";
        firstPlayable.streamId = "s";
        firstPlayable.requestId = "r";
        firstPlayable.traceType = "gateway_receive_few_processed_segments";
        firstPlayable.orchestratorAddress = "0xhot2";
        firstPlayable.sourceEventUid = "e2";

        WorkflowSessionStateMachine.applySignal(state, firstPlayable);

        EventPayloads.FactWorkflowSession fact = WorkflowSessionStateMachine.toFact(state);
        assertEquals(1, fact.knownStream);
        assertEquals(1, fact.startupSuccess);
        assertEquals(0, fact.startupUnexcused);
        assertEquals(1, fact.swapCount); // fallback orch_count > 1
        assertEquals("e1", fact.sourceFirstEventUid);
        assertEquals("e2", fact.sourceLastEventUid);
    }

    @Test
    void classifiesExcusedByErrorTaxonomy() {
        WorkflowSessionAccumulator state = new WorkflowSessionAccumulator();

        LifecycleSignal streamRequest = signal(LifecycleSignal.SignalType.STREAM_TRACE, 1000L);
        streamRequest.workflowSessionId = "s|r";
        streamRequest.traceType = "gateway_receive_stream_request";
        streamRequest.sourceEventUid = "e1";
        WorkflowSessionStateMachine.applySignal(state, streamRequest);

        LifecycleSignal error = signal(LifecycleSignal.SignalType.AI_STREAM_EVENT, 2000L);
        error.workflowSessionId = "s|r";
        error.aiEventType = "error";
        error.message = "WHIP disconnected unexpectedly";
        error.sourceEventUid = "e2";
        WorkflowSessionStateMachine.applySignal(state, error);

        EventPayloads.FactWorkflowSession fact = WorkflowSessionStateMachine.toFact(state);
        assertEquals(1, fact.knownStream);
        assertEquals(0, fact.startupSuccess);
        assertEquals(1, fact.startupExcused);
        assertEquals(0, fact.startupUnexcused);
        assertEquals(1L, fact.errorCount);
        assertEquals(1L, fact.excusableErrorCount);
    }

    @Test
    void classificationPrecedenceKeepsSuccessOverExcusedSignals() {
        WorkflowSessionAccumulator state = new WorkflowSessionAccumulator();

        LifecycleSignal streamRequest = signal(LifecycleSignal.SignalType.STREAM_TRACE, 1000L);
        streamRequest.workflowSessionId = "s|r";
        streamRequest.traceType = "gateway_receive_stream_request";
        streamRequest.sourceEventUid = "e1";
        WorkflowSessionStateMachine.applySignal(state, streamRequest);

        LifecycleSignal success = signal(LifecycleSignal.SignalType.STREAM_TRACE, 1100L);
        success.workflowSessionId = "s|r";
        success.traceType = "gateway_receive_few_processed_segments";
        success.sourceEventUid = "e2";
        WorkflowSessionStateMachine.applySignal(state, success);

        LifecycleSignal excused = signal(LifecycleSignal.SignalType.STREAM_TRACE, 1200L);
        excused.workflowSessionId = "s|r";
        excused.traceType = "gateway_no_orchestrators_available";
        excused.sourceEventUid = "e3";
        WorkflowSessionStateMachine.applySignal(state, excused);

        EventPayloads.FactWorkflowSession fact = WorkflowSessionStateMachine.toFact(state);
        assertEquals(1, fact.startupSuccess);
        assertEquals(0, fact.startupExcused);
        assertEquals(0, fact.startupUnexcused);
    }

    @Test
    void appliesCapabilityAttributionWithTtl() {
        WorkflowSessionAccumulator state = new WorkflowSessionAccumulator();
        state.workflowSessionId = "s|r";
        state.orchestratorAddress = "0xhot";

        CapabilitySnapshotRef ref = new CapabilitySnapshotRef();
        ref.snapshotTs = 10_000L;
        ref.canonicalOrchestratorAddress = "0xcanonical";
        ref.modelId = "streamdiffusion-sdxl";
        ref.gpuId = "GPU-1";

        WorkflowSessionStateMachine.applyCapabilityAttribution(state, ref, 20_000L, 24L * 60L * 60L * 1000L);
        assertEquals("0xcanonical", state.orchestratorAddress);
        assertEquals("streamdiffusion-sdxl", state.modelId);
        assertEquals("GPU-1", state.gpuId);
        assertEquals("nearest_prior_snapshot", state.attributionMethod);
        assertTrue(state.attributionConfidence > 0.0f);
    }

    @Test
    void canonicalAttributionPreventsFalseSwapFromHotWalletChanges() {
        WorkflowSessionAccumulator state = new WorkflowSessionAccumulator();

        LifecycleSignal first = signal(LifecycleSignal.SignalType.STREAM_TRACE, 1000L);
        first.workflowSessionId = "s|r";
        first.traceType = "gateway_receive_stream_request";
        first.orchestratorAddress = "0xhotA";
        first.sourceEventUid = "e1";
        WorkflowSessionStateMachine.applySignal(state, first);

        CapabilitySnapshotRef sameCanonicalA = new CapabilitySnapshotRef();
        sameCanonicalA.snapshotTs = 1000L;
        sameCanonicalA.canonicalOrchestratorAddress = "0xcanonical";
        WorkflowSessionStateMachine.applyCapabilityAttribution(state, sameCanonicalA, 1000L, 24L * 60L * 60L * 1000L);

        LifecycleSignal second = signal(LifecycleSignal.SignalType.STREAM_TRACE, 2000L);
        second.workflowSessionId = "s|r";
        second.traceType = "gateway_receive_first_processed_segment";
        second.orchestratorAddress = "0xhotB";
        second.sourceEventUid = "e2";
        WorkflowSessionStateMachine.applySignal(state, second);

        CapabilitySnapshotRef sameCanonicalB = new CapabilitySnapshotRef();
        sameCanonicalB.snapshotTs = 2000L;
        sameCanonicalB.canonicalOrchestratorAddress = "0xcanonical";
        WorkflowSessionStateMachine.applyCapabilityAttribution(state, sameCanonicalB, 2000L, 24L * 60L * 60L * 1000L);

        EventPayloads.FactWorkflowSession fact = WorkflowSessionStateMachine.toFact(state);
        assertEquals(0, fact.swapCount);
    }

    private static LifecycleSignal signal(LifecycleSignal.SignalType type, long ts) {
        LifecycleSignal signal = new LifecycleSignal();
        signal.signalType = type;
        signal.signalTimestamp = ts;
        signal.ingestTimestamp = ts;
        return signal;
    }
}
