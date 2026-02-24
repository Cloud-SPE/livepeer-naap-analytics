package com.livepeer.analytics.lifecycle;

import com.livepeer.analytics.model.EventPayloads;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkflowSessionStateMachineTest {

    @Test
    void classifiesKnownStreamSuccessWithoutCanonicalSwapFallback() {
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
        assertEquals(0, fact.swapCount); // fallback uses canonical identity only
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

    @Test
    void prodRegression_mixedSignalSequenceKeepsStreamdiffusionAttribution() {
        // Why this test exists:
        // The original defect appeared under a real signal sequence where stream-trace signals
        // often had empty pipeline while capability snapshots for the same hot wallet contained
        // both streamdiffusion and llama models. This test simulates that mixed sequence and
        // verifies attribution does not drift to llama.
        WorkflowSessionAccumulator state = new WorkflowSessionAccumulator();
        CapabilitySnapshotBucket bucket = new CapabilitySnapshotBucket();
        long ttlMs = 24L * 60L * 60L * 1000L;

        CapabilityAttributionSelector.upsertCandidate(
                bucket,
                ref(1_739_040_506_612L, "0xd00354656922168815fcd1e51cbddb9e359e3c7f",
                        "streamdiffusion-sdxl", "GPU-6f1bacf5-d5be-9cad-aa38-e35f905daebe"),
                1_739_040_506_612L,
                ttlMs,
                32);
        CapabilityAttributionSelector.upsertCandidate(
                bucket,
                ref(1_739_040_509_903L, "0xd00354656922168815fcd1e51cbddb9e359e3c7f",
                        "meta-llama/Meta-Llama-3.1-8B-Instruct", null),
                1_739_040_509_903L,
                ttlMs,
                32);
        CapabilityAttributionSelector.upsertCandidate(
                bucket,
                ref(1_739_040_519_000L, "0xd00354656922168815fcd1e51cbddb9e359e3c7f",
                        "streamdiffusion-sdxl", "GPU-6f1bacf5-d5be-9cad-aa38-e35f905daebe"),
                1_739_040_519_000L,
                ttlMs,
                32);

        LifecycleSignal status = signal(LifecycleSignal.SignalType.STREAM_STATUS, 1_739_041_800_304L);
        status.workflowSessionId = "aiJobTesterStream-1771579795824071406|f43724b2";
        status.streamId = "aiJobTesterStream-1771579795824071406";
        status.requestId = "f43724b2";
        status.pipeline = "streamdiffusion-sdxl";
        status.orchestratorAddress = "0x52cf2968b3dc6016778742d3539449a6597d5954";
        status.sourceEventUid = "evt-status";

        WorkflowSessionStateMachine.applySignal(state, status);
        CapabilitySnapshotRef firstSelection = CapabilityAttributionSelector.selectBestCandidate(
                bucket, status.pipeline, status.signalTimestamp, ttlMs);
        WorkflowSessionStateMachine.applyCapabilityAttribution(state, firstSelection, status.signalTimestamp, ttlMs);

        LifecycleSignal trace = signal(LifecycleSignal.SignalType.STREAM_TRACE, 1_739_041_803_000L);
        trace.workflowSessionId = status.workflowSessionId;
        trace.streamId = status.streamId;
        trace.requestId = status.requestId;
        trace.traceType = "gateway_receive_first_processed_segment";
        trace.pipeline = ""; // prod-like trace shape: empty pipeline
        trace.orchestratorAddress = status.orchestratorAddress;
        trace.sourceEventUid = "evt-trace";

        WorkflowSessionStateMachine.applySignal(state, trace);
        CapabilitySnapshotRef secondSelection = CapabilityAttributionSelector.selectBestCandidate(
                bucket, state.pipeline, trace.signalTimestamp, ttlMs);
        WorkflowSessionStateMachine.applyCapabilityAttribution(state, secondSelection, trace.signalTimestamp, ttlMs);

        assertEquals("streamdiffusion-sdxl", state.pipeline);
        assertEquals("streamdiffusion-sdxl", state.modelId);
        assertEquals("GPU-6f1bacf5-d5be-9cad-aa38-e35f905daebe", state.gpuId);
    }

    private static CapabilitySnapshotRef ref(
            long snapshotTs,
            String canonicalAddress,
            String modelId,
            String gpuId) {
        CapabilitySnapshotRef ref = new CapabilitySnapshotRef();
        ref.snapshotTs = snapshotTs;
        ref.canonicalOrchestratorAddress = canonicalAddress;
        ref.modelId = modelId;
        ref.gpuId = gpuId;
        return ref;
    }

    private static LifecycleSignal signal(LifecycleSignal.SignalType type, long ts) {
        LifecycleSignal signal = new LifecycleSignal();
        signal.signalType = type;
        signal.signalTimestamp = ts;
        signal.ingestTimestamp = ts;
        return signal;
    }
}
