package com.livepeer.analytics.lifecycle;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import com.livepeer.analytics.model.EventPayloads;
import com.livepeer.analytics.model.ParsedEvent;
import com.livepeer.analytics.util.AddressNormalizer;
import com.livepeer.analytics.util.BuildMetadata;
import java.util.List;
import com.livepeer.analytics.util.StringSemantics;

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
    private final PipelineModelResolver.Mode resolverMode;

    public WorkflowSessionSegmentAggregatorFunction() {
        this(PipelineModelResolver.Mode.LEGACY_MISNAMED);
    }

    public WorkflowSessionSegmentAggregatorFunction(PipelineModelResolver.Mode resolverMode) {
        this.resolverMode = resolverMode == null
                ? PipelineModelResolver.Mode.LEGACY_MISNAMED
                : resolverMode;
    }

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
        if (signal == null || StringSemantics.isBlank(signal.workflowSessionId)) {
            return;
        }

        WorkflowSessionSegmentAccumulator state = segmentState.value();
        if (state == null) {
            state = new WorkflowSessionSegmentAccumulator();
        }

        // Resolve model compatibility context first, then find nearest capability snapshot.
        CapabilitySnapshotRef snapshot = null;
        String hotKey = AddressNormalizer.normalizeOrEmpty(signal.orchestratorAddress);
        PipelineModelResolver.Resolution preSelection = PipelineModelResolver.resolve(
                resolverMode,
                signal,
                state.pipeline,
                state.modelId,
                null);
        String modelHint = preSelection.compatibilityModelHint;
        if (!StringSemantics.isBlank(hotKey)) {
            CapabilitySnapshotBucket bucket =
                    ctx.getBroadcastState(CapabilityBroadcastState.CAPABILITY_STATE_DESCRIPTOR).get(hotKey);
            snapshot = CapabilityAttributionSelector.selectBestCandidate(
                    bucket,
                    modelHint,
                    signal.signalTimestamp,
                    DEFAULT_ATTRIBUTION_TTL_MS);
        }

        PipelineModelResolver.Resolution resolved = PipelineModelResolver.resolve(
                resolverMode,
                signal,
                state.pipeline,
                state.modelId,
                snapshot);
        if (!StringSemantics.isBlank(resolved.pipeline)) {
            signal.pipeline = resolved.pipeline;
        }
        if (!StringSemantics.isBlank(resolved.modelId)) {
            state.modelId = resolved.modelId;
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
        String hotAddress = AddressNormalizer.normalizeOrEmpty(cap.localAddress);
        String canonicalAddress = AddressNormalizer.normalizeOrEmpty(cap.orchestratorAddress);
        if (StringSemantics.isBlank(hotAddress) && StringSemantics.isBlank(canonicalAddress)) {
            return;
        }

        CapabilitySnapshotRef ref = new CapabilitySnapshotRef();
        ref.snapshotTs = cap.eventTimestamp;
        ref.canonicalOrchestratorAddress = canonicalAddress;
        ref.orchestratorUrl = cap.orchUri;
        ref.pipeline = cap.pipeline;
        ref.modelId = cap.modelId;
        ref.gpuId = cap.gpuId;
        // Index by both hot wallet and canonical address so signal lookups work
        // regardless of which identity variant upstream trace/status emitted.
        if (!StringSemantics.isBlank(hotAddress)) {
            upsertCapabilityIndex(ctx, hotAddress, ref, cap.eventTimestamp);
        }
        if (!StringSemantics.isBlank(canonicalAddress) && !canonicalAddress.equals(hotAddress)) {
            upsertCapabilityIndex(ctx, canonicalAddress, ref, cap.eventTimestamp);
        }
    }

    private static void upsertCapabilityIndex(Context ctx, String key, CapabilitySnapshotRef ref, long eventTs)
            throws Exception {
        CapabilitySnapshotBucket bucket = ctx.getBroadcastState(CapabilityBroadcastState.CAPABILITY_STATE_DESCRIPTOR)
                .get(key);
        if (bucket == null) {
            bucket = new CapabilitySnapshotBucket();
        }
        CapabilityAttributionSelector.upsertCandidate(
                bucket,
                ref,
                eventTs,
                DEFAULT_ATTRIBUTION_TTL_MS,
                CapabilityAttributionSelector.DEFAULT_MAX_CANDIDATES_PER_WALLET);
        ctx.getBroadcastState(CapabilityBroadcastState.CAPABILITY_STATE_DESCRIPTOR).put(key, bucket);
    }

}
