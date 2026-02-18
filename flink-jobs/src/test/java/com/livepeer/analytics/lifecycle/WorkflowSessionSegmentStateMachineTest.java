package com.livepeer.analytics.lifecycle;

import com.livepeer.analytics.model.EventPayloads;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class WorkflowSessionSegmentStateMachineTest {

    @Test
    void emitsNewSegmentWhenOrchestratorChanges() {
        WorkflowSessionSegmentAccumulator state = new WorkflowSessionSegmentAccumulator();

        LifecycleSignal first = signal("s|r", 1000L, "0xhot1", "gateway_receive_stream_request");
        List<EventPayloads.FactWorkflowSessionSegment> out1 =
                WorkflowSessionSegmentStateMachine.applySignal(state, first, snapshot(1000L, "0xofficial1"), ttlMs());
        assertEquals(1, out1.size());
        assertEquals(0, out1.get(0).segmentIndex);
        assertEquals("0xofficial1", out1.get(0).orchestratorAddress);
        assertEquals(null, out1.get(0).segmentEndTs);

        LifecycleSignal second = signal("s|r", 2000L, "0xhot2", "orchestrator_swap");
        List<EventPayloads.FactWorkflowSessionSegment> out2 =
                WorkflowSessionSegmentStateMachine.applySignal(state, second, snapshot(2000L, "0xofficial2"), ttlMs());
        assertEquals(2, out2.size());
        assertEquals(0, out2.get(0).segmentIndex);
        assertEquals(2000L, out2.get(0).segmentEndTs);
        assertEquals(1, out2.get(1).segmentIndex);
        assertEquals("0xofficial2", out2.get(1).orchestratorAddress);
    }

    @Test
    void closesSegmentOnExplicitCloseEdge() {
        WorkflowSessionSegmentAccumulator state = new WorkflowSessionSegmentAccumulator();

        LifecycleSignal first = signal("s|r", 1000L, "0xhot1", "gateway_receive_stream_request");
        WorkflowSessionSegmentStateMachine.applySignal(state, first, snapshot(1000L, "0xofficial1"), ttlMs());

        LifecycleSignal close = signal("s|r", 2500L, "0xhot1", "gateway_ingest_stream_closed");
        List<EventPayloads.FactWorkflowSessionSegment> out =
                WorkflowSessionSegmentStateMachine.applySignal(state, close, snapshot(2500L, "0xofficial1"), ttlMs());

        assertEquals(1, out.size());
        assertEquals(2500L, out.get(0).segmentEndTs);
        assertEquals("stream_closed", out.get(0).reason);
    }

    @Test
    void doesNotCreateNewSegmentOnNonBoundarySignals() {
        WorkflowSessionSegmentAccumulator state = new WorkflowSessionSegmentAccumulator();

        LifecycleSignal first = signal("s|r", 1000L, "0xhot1", "gateway_receive_stream_request");
        List<EventPayloads.FactWorkflowSessionSegment> out1 =
                WorkflowSessionSegmentStateMachine.applySignal(state, first, snapshot(1000L, "0xofficial1"), ttlMs());
        assertEquals(1, out1.size());
        assertEquals(0, out1.get(0).segmentIndex);

        // Same orchestrator + non-boundary trace should keep single segment.
        LifecycleSignal second = signal("s|r", 1500L, "0xhot1", "gateway_receive_first_processed_segment");
        List<EventPayloads.FactWorkflowSessionSegment> out2 =
                WorkflowSessionSegmentStateMachine.applySignal(state, second, snapshot(1500L, "0xofficial1"), ttlMs());
        assertEquals(1, out2.size());
        assertEquals(0, out2.get(0).segmentIndex);
        assertEquals(null, out2.get(0).segmentEndTs);
    }

    private static LifecycleSignal signal(String sessionId, long ts, String orchestrator, String traceType) {
        LifecycleSignal signal = new LifecycleSignal();
        signal.signalType = LifecycleSignal.SignalType.STREAM_TRACE;
        signal.workflowSessionId = sessionId;
        signal.signalTimestamp = ts;
        signal.ingestTimestamp = ts;
        signal.orchestratorAddress = orchestrator;
        signal.traceType = traceType;
        signal.sourceEventUid = "e-" + ts;
        return signal;
    }

    private static long ttlMs() {
        return 24L * 60L * 60L * 1000L;
    }

    private static CapabilitySnapshotRef snapshot(long ts, String canonicalAddress) {
        CapabilitySnapshotRef ref = new CapabilitySnapshotRef();
        ref.snapshotTs = ts;
        ref.canonicalOrchestratorAddress = canonicalAddress;
        return ref;
    }
}
