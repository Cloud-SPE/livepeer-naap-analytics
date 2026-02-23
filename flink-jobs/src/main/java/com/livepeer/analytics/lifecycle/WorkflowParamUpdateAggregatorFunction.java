package com.livepeer.analytics.lifecycle;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import com.livepeer.analytics.model.EventPayloads;
import com.livepeer.analytics.model.ParsedEvent;

import java.util.Locale;

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
    private static final long DEFAULT_ATTRIBUTION_TTL_MS = 24L * 60L * 60L * 1000L;

    private transient ValueState<WorkflowParamUpdateAccumulator> stateRef;

    @Override
    public void open(Configuration parameters) {
        stateRef = getRuntimeContext().getState(
                new ValueStateDescriptor<>("workflow-param-update-state", WorkflowParamUpdateAccumulator.class));
    }

    @Override
    public void processElement(
            LifecycleSignal signal,
            ReadOnlyContext ctx,
            Collector<ParsedEvent<EventPayloads.FactWorkflowParamUpdate>> out) throws Exception {
        if (signal == null || isEmpty(signal.workflowSessionId)) {
            return;
        }

        WorkflowParamUpdateAccumulator state = stateRef.value();
        if (state == null) {
            state = new WorkflowParamUpdateAccumulator();
        }

        state.workflowSessionId = firstNonEmpty(state.workflowSessionId, signal.workflowSessionId);
        state.streamId = firstNonEmpty(state.streamId, signal.streamId);
        state.requestId = firstNonEmpty(state.requestId, signal.requestId);
        state.pipeline = firstNonEmpty(state.pipeline, signal.pipeline);
        state.gateway = firstNonEmpty(state.gateway, signal.gateway);
        state.orchestratorUrl = firstNonEmpty(state.orchestratorUrl, signal.orchestratorUrl);

        // Reuse broadcast enrichment for canonical orchestrator and model/GPU attribution.
        CapabilitySnapshotRef snapshot = null;
        String signalAddress = normalizeAddress(signal.orchestratorAddress);
        if (!isEmpty(signalAddress)) {
            CapabilitySnapshotBucket bucket = ctx.getBroadcastState(CapabilityBroadcastState.CAPABILITY_STATE_DESCRIPTOR)
                    .get(signalAddress);
            snapshot = CapabilityAttributionSelector.selectBestCandidate(
                    bucket,
                    signal.pipeline,
                    signal.signalTimestamp,
                    DEFAULT_ATTRIBUTION_TTL_MS);
        }
        applySnapshotAttribution(state, snapshot, signal.signalTimestamp);

        state.version++;
        stateRef.update(state);

        // Emit markers strictly for parameter update signals.
        if (signal.signalType == LifecycleSignal.SignalType.AI_STREAM_EVENT
                && "params_update".equalsIgnoreCase(firstNonEmpty(signal.aiEventType, ""))) {
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
            row.message = firstNonEmpty(signal.message, "");
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

    private void applySnapshotAttribution(
            WorkflowParamUpdateAccumulator state,
            CapabilitySnapshotRef snapshot,
            long signalTs) {
        if (snapshot == null) {
            return;
        }
        if (!isEmpty(snapshot.canonicalOrchestratorAddress)) {
            state.orchestratorAddress = snapshot.canonicalOrchestratorAddress;
        }
        state.orchestratorUrl = firstNonEmpty(state.orchestratorUrl, snapshot.orchestratorUrl);

        long delta = Math.abs(signalTs - snapshot.snapshotTs);
        if (delta == 0) {
            state.attributionMethod = "exact_orchestrator_time_match";
            state.attributionConfidence = 1.0f;
        } else if (snapshot.snapshotTs <= signalTs && delta <= DEFAULT_ATTRIBUTION_TTL_MS) {
            state.attributionMethod = "nearest_prior_snapshot";
            state.attributionConfidence = 0.9f;
        } else if (delta <= DEFAULT_ATTRIBUTION_TTL_MS) {
            state.attributionMethod = "nearest_snapshot_within_ttl";
            state.attributionConfidence = 0.7f;
        } else {
            state.attributionMethod = "proxy_address_join";
            state.attributionConfidence = 0.4f;
        }

        state.modelId = emptyToNull(snapshot.modelId);
        state.gpuId = emptyToNull(snapshot.gpuId);
    }

    private static String normalizeAddress(String address) {
        return isEmpty(address) ? "" : address.trim().toLowerCase(Locale.ROOT);
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
}
