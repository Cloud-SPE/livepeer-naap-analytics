package com.livepeer.analytics.lifecycle;

import com.livepeer.analytics.model.EventPayloads;
import com.livepeer.analytics.model.ParsedEvent;
import com.livepeer.analytics.util.StringSemantics;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Emits lifecycle edge-coverage diagnostics per workflow session signal.
 *
 * <p>Design:
 * - Reuses {@link WorkflowSessionStateMachine} transitions so correlation semantics are identical
 *   to the main session fact stream.
 * - Produces a small observability fact row after every signal to track edge-pair completeness
 *   and unmatched reasons at runtime.
 * </p>
 */
public class WorkflowLifecycleCoverageAggregatorFunction extends KeyedProcessFunction<
        String, LifecycleSignal, ParsedEvent<EventPayloads.FactLifecycleEdgeCoverage>> {
    private static final long serialVersionUID = 1L;

    private transient ValueState<WorkflowSessionAccumulator> sessionState;

    @Override
    public void open(Configuration parameters) {
        sessionState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("workflow-lifecycle-coverage-state", WorkflowSessionAccumulator.class));
    }

    @Override
    public void processElement(
            LifecycleSignal signal,
            Context ctx,
            Collector<ParsedEvent<EventPayloads.FactLifecycleEdgeCoverage>> out) throws Exception {
        if (signal == null || StringSemantics.isBlank(signal.workflowSessionId)) {
            return;
        }

        WorkflowSessionAccumulator state = sessionState.value();
        if (state == null) {
            state = new WorkflowSessionAccumulator();
            state.workflowSessionId = signal.workflowSessionId;
        }

        WorkflowSessionStateMachine.applySignal(state, signal);
        sessionState.update(state);

        EventPayloads.FactLifecycleEdgeCoverage row = toCoverageFact(state, signal);
        out.collect(new ParsedEvent<>(signal.sourceEvent, row));
    }

    static EventPayloads.FactLifecycleEdgeCoverage toCoverageFact(
            WorkflowSessionAccumulator state,
            LifecycleSignal signal) {
        EventPayloads.FactLifecycleEdgeCoverage row = new EventPayloads.FactLifecycleEdgeCoverage();
        row.signalTs = signal.signalTimestamp;
        row.workflowSessionId = state.workflowSessionId;
        row.streamId = state.streamId;
        row.requestId = state.requestId;
        row.pipeline = state.pipeline;
        row.modelId = state.modelId;
        row.gateway = state.gateway;
        row.orchestratorAddress = state.orchestratorAddress;
        row.traceType = signal.traceType == null ? "" : signal.traceType;
        row.sourceEventUid = signal.sourceEventUid == null ? "" : signal.sourceEventUid;

        row.knownStream = state.knownStream ? 1 : 0;
        row.hasFirstProcessedEdge = state.firstProcessedTs != null ? 1 : 0;
        row.hasFirstPlayableEdge = state.firstPlayableTs != null ? 1 : 0;
        row.startupEdgeMatched = (state.knownStream && state.firstProcessedTs != null) ? 1 : 0;
        row.playableEdgeMatched = (state.knownStream && state.firstPlayableTs != null) ? 1 : 0;
        row.isTerminalSignal = isTerminalSignal(signal) ? 1 : 0;
        row.unmatchedReason = unmatchedReason(state, signal);
        row.version = state.version;
        return row;
    }

    private static String unmatchedReason(WorkflowSessionAccumulator state, LifecycleSignal signal) {
        if (!state.knownStream) {
            return "missing_stream_request_edge";
        }
        if (isTerminalSignal(signal) && state.firstProcessedTs == null) {
            return "missing_first_processed_segment_edge";
        }
        if (isTerminalSignal(signal) && state.firstPlayableTs == null) {
            return "missing_playable_edge";
        }
        if (state.firstProcessedTs == null) {
            return "awaiting_first_processed_segment_edge";
        }
        if (state.firstPlayableTs == null) {
            return "awaiting_playable_edge";
        }
        return "";
    }

    private static boolean isTerminalSignal(LifecycleSignal signal) {
        if (signal.signalType == LifecycleSignal.SignalType.STREAM_TRACE) {
            return "gateway_ingest_stream_closed".equals(signal.traceType);
        }
        return false;
    }
}
