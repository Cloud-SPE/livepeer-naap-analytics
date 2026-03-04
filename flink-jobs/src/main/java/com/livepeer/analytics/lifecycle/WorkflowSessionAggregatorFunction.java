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
    private final PipelineModelResolver.Mode resolverMode;

    public WorkflowSessionAggregatorFunction() {
        this(PipelineModelResolver.Mode.LEGACY_MISNAMED);
    }

    public WorkflowSessionAggregatorFunction(PipelineModelResolver.Mode resolverMode) {
        this.resolverMode = resolverMode == null
                ? PipelineModelResolver.Mode.LEGACY_MISNAMED
                : resolverMode;
    }

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
        if (signal == null || StringSemantics.isBlank(signal.workflowSessionId)) {
            return;
        }

        WorkflowSessionAccumulator state = sessionState.value();
        if (state == null) {
            state = new WorkflowSessionAccumulator();
            state.workflowSessionId = signal.workflowSessionId;
        }

        // Phase 1: derive a compatibility model hint without capability context.
        // This preserves spec behavior for legacy/misnamed payloads where `pipeline` can carry
        // model-like tokens, and gives attribution selection a normalized hint to match against.
        CapabilitySnapshotRef snapshot = null;
        String hotKey = resolveCapabilityLookupKey(signal.orchestratorAddress, state.orchestratorAddress);
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
                    CapabilityAttributionSelector.SelectionContext.of(
                            modelHint,
                            signal.orchestratorUrl,
                            state.gpuId,
                            signal.signalTimestamp,
                            DEFAULT_ATTRIBUTION_TTL_MS));
        }

        // Phase 2: resolve canonical pipeline/model using selected capability context.
        PipelineModelResolver.Resolution resolved = PipelineModelResolver.resolve(
                resolverMode,
                signal,
                state.pipeline,
                state.modelId,
                snapshot);
        applyResolvedPipelineToSignal(signal, resolved);

        // Apply deterministic state transitions and attribution in a fixed order.
        WorkflowSessionStateMachine.applySignal(state, signal);
        if (!StringSemantics.isBlank(resolved.pipeline)) {
            state.pipeline = resolved.pipeline;
        }
        if (!StringSemantics.isBlank(resolved.modelId)) {
            state.modelId = resolved.modelId;
        }
        WorkflowSessionStateMachine.applyCapabilityAttribution(
                state,
                snapshot,
                signal.signalTimestamp,
                DEFAULT_ATTRIBUTION_TTL_MS);
        if (isTerminalSignal(signal)) {
            applyTerminalAttributionFinalization(state, signal, ctx);
        }
        sessionState.update(state);

        // Emit latest state snapshot for this session (versioned upsert row).
        EventPayloads.FactWorkflowSession fact = WorkflowSessionStateMachine.toFact(state);
        out.collect(new ParsedEvent<>(signal.sourceEvent, fact));
    }

    /**
     * Applies canonical pipeline semantics directly to the lifecycle signal before state-machine
     * transitions execute.
     *
     * <p>Why this matters:
     * - The state machine ingests `signal.pipeline` for downstream session/segment facts.
     * - In legacy streams, incoming `signal.pipeline` may be model-like (`legacy_misnamed`).
     * - Writing `resolved.pipeline` back to the signal prevents model-id leakage into pipeline
     *   dimensions and keeps emitted facts aligned with serving/view contracts.
     *
     * <p>Behavior:
     * - Non-blank resolved values are copied through.
     * - Blank unresolved values intentionally normalize to empty string, avoiding stale prior
     *   model-like labels from persisting in the signal path.
     */
    static void applyResolvedPipelineToSignal(
            LifecycleSignal signal,
            PipelineModelResolver.Resolution resolved) {
        if (signal == null || resolved == null) {
            return;
        }
        signal.pipeline = StringSemantics.firstNonBlank(resolved.pipeline);
    }

    static String resolveCapabilityLookupKey(String signalOrchestratorAddress, String stateOrchestratorAddress) {
        return AddressNormalizer.normalizeOrEmpty(
                StringSemantics.firstNonBlank(signalOrchestratorAddress, stateOrchestratorAddress));
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
        String hotAddress = AddressNormalizer.normalizeOrEmpty(cap.localAddress);
        String canonicalAddress = AddressNormalizer.normalizeOrEmpty(cap.orchestratorAddress);
        if (StringSemantics.isBlank(hotAddress) && StringSemantics.isBlank(canonicalAddress)) {
            return;
        }

        // Normalize and cache capability snapshots keyed by hot wallet so keyed session signals
        // can map to canonical orchestrator identity + model/GPU attribution.
        CapabilitySnapshotRef ref = new CapabilitySnapshotRef();
        ref.snapshotTs = cap.eventTimestamp;
        ref.sourceEventId = cap.sourceEventId;
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

    private static boolean isTerminalSignal(LifecycleSignal signal) {
        return signal.signalType == LifecycleSignal.SignalType.STREAM_TRACE
                && "gateway_ingest_stream_closed".equals(signal.traceType);
    }

    private void applyTerminalAttributionFinalization(
            WorkflowSessionAccumulator state,
            LifecycleSignal signal,
            ReadOnlyContext ctx) throws Exception {
        String lookupAddress = resolveCapabilityLookupKey(signal.orchestratorAddress, state.orchestratorAddress);
        if (StringSemantics.isBlank(lookupAddress)) {
            return;
        }

        CapabilitySnapshotBucket bucket =
                ctx.getBroadcastState(CapabilityBroadcastState.CAPABILITY_STATE_DESCRIPTOR).get(lookupAddress);
        if (bucket == null) {
            return;
        }

        CapabilitySnapshotRef selected = CapabilityAttributionSelector.selectBestCandidate(
                bucket,
                CapabilityAttributionSelector.SelectionContext.of(
                        StringSemantics.firstNonBlank(state.modelId, signal.modelHint, signal.pipelineHint, signal.pipeline),
                        StringSemantics.firstNonBlank(signal.orchestratorUrl, state.orchestratorUrl),
                        state.gpuId,
                        signal.signalTimestamp,
                        DEFAULT_ATTRIBUTION_TTL_MS));
        if (selected == null) {
            return;
        }

        WorkflowSessionStateMachine.applyCapabilityAttribution(
                state,
                selected,
                signal.signalTimestamp,
                DEFAULT_ATTRIBUTION_TTL_MS);
    }

}
