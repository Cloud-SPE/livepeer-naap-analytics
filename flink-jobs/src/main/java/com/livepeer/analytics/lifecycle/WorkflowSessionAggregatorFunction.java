package com.livepeer.analytics.lifecycle;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import com.livepeer.analytics.model.EventPayloads;
import com.livepeer.analytics.model.ParsedEvent;
import com.livepeer.analytics.util.BuildMetadata;

import java.util.Locale;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stateful workflow-session builder keyed by workflow session id with broadcast capability enrichment.
 *
 * <p>Design notes:
 * - Keyed state keeps one mutable accumulator per workflow session.
 * - Broadcast state carries bounded capability candidates by hot-wallet address.
 * - For each lifecycle signal we update session state, apply attribution, then emit the latest
 *   versioned session row (ReplacingMergeTree upsert model in ClickHouse).
 * - This operator intentionally emits incremental snapshots; consumers should query latest version
 *   semantics (FINAL or argMax by version) for point-in-time session state.
 * </p>
 */
public class WorkflowSessionAggregatorFunction extends KeyedBroadcastProcessFunction<
        String,
        LifecycleSignal,
        ParsedEvent<EventPayloads.NetworkCapability>,
        ParsedEvent<EventPayloads.FactWorkflowSession>> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(WorkflowSessionAggregatorFunction.class);

    private static final long DEFAULT_ATTRIBUTION_TTL_MS = 24L * 60L * 60L * 1000L;

    private transient ValueState<WorkflowSessionAccumulator> sessionState;

    @Override
    public void open(Configuration parameters) {
        sessionState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("workflow-session-state", WorkflowSessionAccumulator.class));
        LOG.info(
                "Lifecycle attribution mode initialized (operator=workflow-session, mode=multi-candidate, ttl_ms={}, max_candidates_per_wallet={}, broadcast_descriptor={}, build_version={})",
                DEFAULT_ATTRIBUTION_TTL_MS,
                CapabilityAttributionSelector.DEFAULT_MAX_CANDIDATES_PER_WALLET,
                CapabilityBroadcastState.CAPABILITY_STATE_DESCRIPTOR.getName(),
                BuildMetadata.current().identity());
    }

    @Override
    public void processElement(
            LifecycleSignal signal,
            ReadOnlyContext ctx,
            Collector<ParsedEvent<EventPayloads.FactWorkflowSession>> out) throws Exception {
        if (signal == null || isEmpty(signal.workflowSessionId)) {
            return;
        }

        WorkflowSessionAccumulator state = sessionState.value();
        if (state == null) {
            state = new WorkflowSessionAccumulator();
            state.workflowSessionId = signal.workflowSessionId;
        }

        // Lookup capability by the observed hot-wallet identity in the signal.
        CapabilitySnapshotRef snapshot = null;
        String hotKey = normalizeAddress(signal.orchestratorAddress);
        // Critical: use persisted session pipeline when signal pipeline is empty (common for
        // trace edges). Passing empty pipeline reopens mixed-model selection drift.
        String attributionPipeline = firstNonEmpty(signal.pipeline, state.pipeline);
        if (!isEmpty(hotKey)) {
            CapabilitySnapshotBucket bucket =
                    ctx.getBroadcastState(CapabilityBroadcastState.CAPABILITY_STATE_DESCRIPTOR).get(hotKey);
            snapshot = CapabilityAttributionSelector.selectBestCandidate(
                    bucket,
                    attributionPipeline,
                    signal.signalTimestamp,
                    DEFAULT_ATTRIBUTION_TTL_MS);
        }

        // Apply deterministic state transitions and attribution in a fixed order.
        WorkflowSessionStateMachine.applySignal(state, signal);
        WorkflowSessionStateMachine.applyCapabilityAttribution(
                state,
                snapshot,
                signal.signalTimestamp,
                DEFAULT_ATTRIBUTION_TTL_MS);
        sessionState.update(state);

        // Emit latest state snapshot for this session (versioned upsert row).
        EventPayloads.FactWorkflowSession fact = WorkflowSessionStateMachine.toFact(state);
        out.collect(new ParsedEvent<>(signal.sourceEvent, fact));
    }

    @Override
    public void processBroadcastElement(
            ParsedEvent<EventPayloads.NetworkCapability> capabilityEvent,
            Context ctx,
            Collector<ParsedEvent<EventPayloads.FactWorkflowSession>> out) throws Exception {
        if (capabilityEvent == null || capabilityEvent.payload == null) {
            return;
        }

        EventPayloads.NetworkCapability cap = capabilityEvent.payload;
        String hotAddress = normalizeAddress(cap.localAddress);
        if (isEmpty(hotAddress)) {
            return;
        }

        // Normalize and cache capability snapshots keyed by hot wallet so keyed session signals
        // can map to canonical orchestrator identity + model/GPU attribution.
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
