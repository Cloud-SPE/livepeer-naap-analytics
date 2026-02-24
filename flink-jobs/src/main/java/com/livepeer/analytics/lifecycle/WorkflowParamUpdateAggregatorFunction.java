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
import com.livepeer.analytics.util.StringSemantics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Emits lifecycle parameter-update markers keyed by workflow session id.
 *
 * <p>Design notes:
 * - Maintains lightweight keyed context (session identity + latest attribution context).
 * - Emits rows only for `ai_stream_events.type = params_update`.
 * - Uses the same broadcast capability map as other lifecycle operators to keep
 *   orchestrator/model/GPU attribution consistent across facts.
 * </p>
 */
public class WorkflowParamUpdateAggregatorFunction extends KeyedBroadcastProcessFunction<
        String,
        LifecycleSignal,
        ParsedEvent<EventPayloads.NetworkCapability>,
        ParsedEvent<EventPayloads.FactWorkflowParamUpdate>> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(WorkflowParamUpdateAggregatorFunction.class);
    private static final long DEFAULT_ATTRIBUTION_TTL_MS = 24L * 60L * 60L * 1000L;

    private transient ValueState<WorkflowParamUpdateAccumulator> stateRef;

    @Override
    public void open(Configuration parameters) {
        stateRef = getRuntimeContext().getState(
                new ValueStateDescriptor<>("workflow-param-update-state", WorkflowParamUpdateAccumulator.class));
        LOG.info(
                "Lifecycle attribution mode initialized (operator=workflow-param-update, mode=multi-candidate, ttl_ms={}, max_candidates_per_wallet={}, broadcast_descriptor={}, build_version={})",
                DEFAULT_ATTRIBUTION_TTL_MS,
                CapabilityAttributionSelector.DEFAULT_MAX_CANDIDATES_PER_WALLET,
                CapabilityBroadcastState.CAPABILITY_STATE_DESCRIPTOR.getName(),
                BuildMetadata.current().identity());
    }

    @Override
    public void processElement(
            LifecycleSignal signal,
            ReadOnlyContext ctx,
            Collector<ParsedEvent<EventPayloads.FactWorkflowParamUpdate>> out) throws Exception {
        if (signal == null || StringSemantics.isBlank(signal.workflowSessionId)) {
            return;
        }

        WorkflowParamUpdateAccumulator state = stateRef.value();
        if (state == null) {
            state = new WorkflowParamUpdateAccumulator();
        }

        state.workflowSessionId = StringSemantics.firstNonBlank(state.workflowSessionId, signal.workflowSessionId);
        state.streamId = StringSemantics.firstNonBlank(state.streamId, signal.streamId);
        state.requestId = StringSemantics.firstNonBlank(state.requestId, signal.requestId);
        state.pipeline = StringSemantics.firstNonBlank(state.pipeline, signal.pipeline);
        state.gateway = StringSemantics.firstNonBlank(state.gateway, signal.gateway);
        state.orchestratorUrl = StringSemantics.firstNonBlank(state.orchestratorUrl, signal.orchestratorUrl);

        // Reuse broadcast enrichment for canonical orchestrator and model/GPU attribution.
        CapabilitySnapshotRef snapshot = null;
        String signalAddress = AddressNormalizer.normalizeOrEmpty(signal.orchestratorAddress);
        // Critical: preserve non-empty pipeline context across signals; empty pipeline must not
        // broaden compatibility and allow nearest-but-incompatible snapshot selection.
        String attributionPipeline = StringSemantics.firstNonBlank(signal.pipeline, state.pipeline);
        if (!StringSemantics.isBlank(signalAddress)) {
            CapabilitySnapshotBucket bucket = ctx.getBroadcastState(CapabilityBroadcastState.CAPABILITY_STATE_DESCRIPTOR)
                    .get(signalAddress);
            snapshot = CapabilityAttributionSelector.selectBestCandidate(
                    bucket,
                    attributionPipeline,
                    signal.signalTimestamp,
                    DEFAULT_ATTRIBUTION_TTL_MS);
        }
        applySnapshotAttribution(state, snapshot, signal.signalTimestamp);

        state.version++;
        stateRef.update(state);

        // Emit markers strictly for parameter update signals.
        if (signal.signalType == LifecycleSignal.SignalType.AI_STREAM_EVENT
                && "params_update".equalsIgnoreCase(StringSemantics.firstNonBlank(signal.aiEventType, ""))) {
            EventPayloads.FactWorkflowParamUpdate row = new EventPayloads.FactWorkflowParamUpdate();
            row.updateTs = signal.signalTimestamp;
            row.workflowSessionId = state.workflowSessionId;
            row.streamId = state.streamId;
            row.requestId = state.requestId;
            row.pipeline = state.pipeline;
            row.gateway = state.gateway;
            row.orchestratorAddress = state.orchestratorAddress;
            row.orchestratorUrl = state.orchestratorUrl;
            row.modelId = state.modelId;
            row.gpuId = state.gpuId;
            row.attributionMethod = state.attributionMethod;
            row.attributionConfidence = state.attributionConfidence;
            row.updateType = "params_update";
            row.message = StringSemantics.firstNonBlank(signal.message, "");
            row.sourceEventUid = signal.sourceEventUid;
            row.version = state.version;
            out.collect(new ParsedEvent<>(signal.sourceEvent, row));
        }
    }

    @Override
    public void processBroadcastElement(
            ParsedEvent<EventPayloads.NetworkCapability> capabilityEvent,
            Context ctx,
            Collector<ParsedEvent<EventPayloads.FactWorkflowParamUpdate>> out) throws Exception {
        if (capabilityEvent == null || capabilityEvent.payload == null) {
            return;
        }
        EventPayloads.NetworkCapability cap = capabilityEvent.payload;
        String hotAddress = AddressNormalizer.normalizeOrEmpty(cap.localAddress);
        if (StringSemantics.isBlank(hotAddress)) {
            return;
        }
        CapabilitySnapshotRef ref = new CapabilitySnapshotRef();
        ref.snapshotTs = cap.eventTimestamp;
        ref.canonicalOrchestratorAddress = AddressNormalizer.normalizeOrEmpty(cap.orchestratorAddress);
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

    private void applySnapshotAttribution(
            WorkflowParamUpdateAccumulator state,
            CapabilitySnapshotRef snapshot,
            long signalTs) {
        if (snapshot == null) {
            return;
        }
        if (!StringSemantics.isBlank(snapshot.canonicalOrchestratorAddress)) {
            state.orchestratorAddress = snapshot.canonicalOrchestratorAddress;
        }
        state.orchestratorUrl = StringSemantics.firstNonBlank(state.orchestratorUrl, snapshot.orchestratorUrl);
        AttributionSemantics.Decision decision =
                AttributionSemantics.fromSnapshot(signalTs, snapshot.snapshotTs, DEFAULT_ATTRIBUTION_TTL_MS);
        state.attributionMethod = decision.method;
        state.attributionConfidence = decision.confidence;

        state.modelId = StringSemantics.blankToNull(snapshot.modelId);
        state.gpuId = StringSemantics.blankToNull(snapshot.gpuId);
    }
}
