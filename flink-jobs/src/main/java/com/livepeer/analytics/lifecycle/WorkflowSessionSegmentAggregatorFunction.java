package com.livepeer.analytics.lifecycle;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import com.livepeer.analytics.model.EventPayloads;
import com.livepeer.analytics.model.ParsedEvent;
import com.livepeer.analytics.util.BuildMetadata;

import java.util.List;
import java.util.Locale;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stateful segment builder keyed by workflow session id with broadcast capability enrichment.
 *
 * <p>Design notes:
 * - Uses keyed state to retain current open segment context per workflow session.
 * - Delegates transition logic to {@link WorkflowSessionSegmentStateMachine}.
 * - Emits versioned segment upserts; on boundary events the state machine can emit two rows:
 *   close previous segment + open next segment.
 * </p>
 */
public class WorkflowSessionSegmentAggregatorFunction extends KeyedBroadcastProcessFunction<
        String,
        LifecycleSignal,
        ParsedEvent<EventPayloads.NetworkCapability>,
        ParsedEvent<EventPayloads.FactWorkflowSessionSegment>> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(WorkflowSessionSegmentAggregatorFunction.class);
    private static final long DEFAULT_ATTRIBUTION_TTL_MS = 24L * 60L * 60L * 1000L;

    private transient ValueState<WorkflowSessionSegmentAccumulator> segmentState;

    @Override
    public void open(Configuration parameters) {
        segmentState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("workflow-session-segment-state", WorkflowSessionSegmentAccumulator.class));
        LOG.info(
                "Lifecycle attribution mode initialized (operator=workflow-session-segment, mode=multi-candidate, ttl_ms={}, max_candidates_per_wallet={}, broadcast_descriptor={}, build_version={})",
                DEFAULT_ATTRIBUTION_TTL_MS,
                CapabilityAttributionSelector.DEFAULT_MAX_CANDIDATES_PER_WALLET,
                CapabilityBroadcastState.CAPABILITY_STATE_DESCRIPTOR.getName(),
                BuildMetadata.current().identity());
    }

    @Override
    public void processElement(
            LifecycleSignal signal,
            ReadOnlyContext ctx,
            Collector<ParsedEvent<EventPayloads.FactWorkflowSessionSegment>> out) throws Exception {
        if (signal == null || isEmpty(signal.workflowSessionId)) {
            return;
        }

        WorkflowSessionSegmentAccumulator state = segmentState.value();
        if (state == null) {
            state = new WorkflowSessionSegmentAccumulator();
        }

        // Resolve nearest capability snapshot for canonical orchestrator + model/GPU attribution.
        CapabilitySnapshotRef snapshot = null;
        String hotKey = normalizeAddress(signal.orchestratorAddress);
        // Critical: for empty trace pipelines, fallback to existing segment model context so
        // selector keeps compatibility constraints and does not pick unrelated models.
        String attributionPipeline = firstNonEmpty(signal.pipeline, state.modelId);
        if (!isEmpty(hotKey)) {
            CapabilitySnapshotBucket bucket =
                    ctx.getBroadcastState(CapabilityBroadcastState.CAPABILITY_STATE_DESCRIPTOR).get(hotKey);
            snapshot = CapabilityAttributionSelector.selectBestCandidate(
                    bucket,
                    attributionPipeline,
                    signal.signalTimestamp,
                    DEFAULT_ATTRIBUTION_TTL_MS);
        }

        // State machine may emit one or more rows per signal depending on boundary transitions.
        List<EventPayloads.FactWorkflowSessionSegment> results =
                WorkflowSessionSegmentStateMachine.applySignal(state, signal, snapshot, DEFAULT_ATTRIBUTION_TTL_MS);
        segmentState.update(state);
        for (EventPayloads.FactWorkflowSessionSegment row : results) {
            out.collect(new ParsedEvent<>(signal.sourceEvent, row));
        }
    }

    @Override
    public void processBroadcastElement(
            ParsedEvent<EventPayloads.NetworkCapability> capabilityEvent,
            Context ctx,
            Collector<ParsedEvent<EventPayloads.FactWorkflowSessionSegment>> out) throws Exception {
        if (capabilityEvent == null || capabilityEvent.payload == null) {
            return;
        }

        EventPayloads.NetworkCapability cap = capabilityEvent.payload;
        String hotAddress = normalizeAddress(cap.localAddress);
        if (isEmpty(hotAddress)) {
            return;
        }

        CapabilitySnapshotRef ref = new CapabilitySnapshotRef();
        ref.snapshotTs = cap.eventTimestamp;
        ref.canonicalOrchestratorAddress = normalizeAddress(cap.orchestratorAddress);
        ref.orchestratorUrl = cap.orchUri;
        ref.modelId = cap.modelId;
        ref.gpuId = cap.gpuId;
        CapabilitySnapshotBucket bucket = ctx.getBroadcastState(CapabilityBroadcastState.CAPABILITY_STATE_DESCRIPTOR)
                .get(hotAddress);
        if (bucket == null) {
            bucket = new CapabilitySnapshotBucket();
        }
        CapabilityAttributionSelector.upsertCandidate(
                bucket,
                ref,
                cap.eventTimestamp,
                DEFAULT_ATTRIBUTION_TTL_MS,
                CapabilityAttributionSelector.DEFAULT_MAX_CANDIDATES_PER_WALLET);
        ctx.getBroadcastState(CapabilityBroadcastState.CAPABILITY_STATE_DESCRIPTOR).put(hotAddress, bucket);
    }

    private static String normalizeAddress(String address) {
        return isEmpty(address) ? "" : address.trim().toLowerCase(Locale.ROOT);
    }

    private static boolean isEmpty(String value) {
        return value == null || value.trim().isEmpty();
    }

    private static String firstNonEmpty(String... values) {
        for (String value : values) {
            if (!isEmpty(value)) {
                return value;
            }
        }
        return "";
    }
}
