package com.livepeer.analytics.lifecycle;

import com.livepeer.analytics.model.EventPayloads;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class WorkflowLifecycleCoverageAggregatorFunctionTest {

    @Test
    void emitsMatchedCoverageWhenStartupAndPlayableEdgesExist() {
        WorkflowSessionAccumulator state = new WorkflowSessionAccumulator();
        state.workflowSessionId = "s|r";
        state.streamId = "s";
        state.requestId = "r";
        state.knownStream = true;
        state.firstProcessedTs = 2000L;
        state.firstPlayableTs = 2500L;
        state.version = 4L;

        LifecycleSignal signal = signal("gateway_receive_few_processed_segments", 2500L);

        EventPayloads.FactLifecycleEdgeCoverage row =
                WorkflowLifecycleCoverageAggregatorFunction.toCoverageFact(state, signal);

        assertEquals(1, row.knownStream);
        assertEquals(1, row.startupEdgeMatched);
        assertEquals(1, row.playableEdgeMatched);
        assertEquals("", row.unmatchedReason);
    }

    @Test
    void emitsAwaitingProcessedReasonBeforeTerminalClose() {
        WorkflowSessionAccumulator state = new WorkflowSessionAccumulator();
        state.workflowSessionId = "s|r";
        state.knownStream = true;

        LifecycleSignal signal = signal("gateway_receive_stream_request", 1000L);

        EventPayloads.FactLifecycleEdgeCoverage row =
                WorkflowLifecycleCoverageAggregatorFunction.toCoverageFact(state, signal);

        assertEquals("awaiting_first_processed_segment_edge", row.unmatchedReason);
        assertEquals(0, row.isTerminalSignal);
    }

    @Test
    void emitsTerminalUnmatchedReasonWhenClosedWithoutProcessedEdge() {
        WorkflowSessionAccumulator state = new WorkflowSessionAccumulator();
        state.workflowSessionId = "s|r";
        state.knownStream = true;

        LifecycleSignal signal = signal("gateway_ingest_stream_closed", 3000L);

        EventPayloads.FactLifecycleEdgeCoverage row =
                WorkflowLifecycleCoverageAggregatorFunction.toCoverageFact(state, signal);

        assertEquals(1, row.isTerminalSignal);
        assertEquals("missing_first_processed_segment_edge", row.unmatchedReason);
    }

    private static LifecycleSignal signal(String traceType, long ts) {
        LifecycleSignal signal = new LifecycleSignal();
        signal.signalType = LifecycleSignal.SignalType.STREAM_TRACE;
        signal.traceType = traceType;
        signal.signalTimestamp = ts;
        signal.ingestTimestamp = ts;
        signal.sourceEventUid = "evt";
        return signal;
    }
}
