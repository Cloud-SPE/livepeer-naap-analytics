package com.livepeer.analytics.lifecycle;

import com.livepeer.analytics.model.EventPayloads;
import com.livepeer.analytics.model.ParsedEvent;
import com.livepeer.analytics.model.StreamingEvent;
import com.livepeer.analytics.util.Hashing;
import com.livepeer.analytics.util.StringSemantics;
import com.livepeer.analytics.util.WorkflowSessionId;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Attributes trace edges to the best matching workflow segment window.
 *
 * <p>Why pending-buffer logic exists:
 * trace events can arrive before the corresponding segment row reaches this keyed operator.
 * Emitting immediately as unattributed would permanently lose canonical pipeline/model metadata
 * for otherwise attributable rows. To avoid that race, unmatched rows are buffered briefly and
 * retried when segment updates arrive (or on timer expiry as a safe fallback).</p>
 */
public class WorkflowTraceAttributionProcessFunction extends KeyedCoProcessFunction<
        String,
    ParsedEvent<EventPayloads.StreamTraceEvent>,
        ParsedEvent<EventPayloads.FactWorkflowSessionSegment>,
        ParsedEvent<EventPayloads.FactStreamTraceEdge>> {
    private static final long serialVersionUID = 1L;
    // Small processing-time delay to absorb normal segment-vs-trace reordering without
    // holding events indefinitely when segment evidence never arrives.
    private static final long ATTRIBUTION_WAIT_MS = 15_000L;

    private transient MapState<Integer, EventPayloads.FactWorkflowSessionSegment> segmentByIndex;
    private transient MapState<String, PendingTraceEvent> pendingByEventKey;

    @Override
    public void open(Configuration parameters) {
        segmentByIndex = getRuntimeContext().getMapState(
                new MapStateDescriptor<>(
                        "segment-by-index-for-trace",
                        Integer.class,
                        EventPayloads.FactWorkflowSessionSegment.class));
        pendingByEventKey = getRuntimeContext().getMapState(
            new MapStateDescriptor<>(
                "pending-trace-events-for-attribution",
                String.class,
                PendingTraceEvent.class));
    }

    @Override
    public void processElement1(
            ParsedEvent<EventPayloads.StreamTraceEvent> traceEvent,
            Context ctx,
            Collector<ParsedEvent<EventPayloads.FactStreamTraceEdge>> out) throws Exception {
        if (traceEvent == null || traceEvent.payload == null) {
            return;
        }

        EventPayloads.StreamTraceEvent trace = traceEvent.payload;
        long edgeTs = trace.dataTimestamp > 0 ? trace.dataTimestamp : trace.eventTimestamp;
        EventPayloads.FactWorkflowSessionSegment segment = findMatchingSegment(edgeTs);
        String sourceEventUid = hashRawJsonOrFallback(trace.rawJson, traceEvent.event);

        if (segment != null) {
            out.collect(toTraceFact(traceEvent, ctx.getCurrentKey(), edgeTs, sourceEventUid, segment, 1));
            return;
        }

        // No segment match yet: buffer and retry after a short delay.
        PendingTraceEvent pending = new PendingTraceEvent();
        pending.traceEvent = traceEvent;
        pending.edgeTs = edgeTs;
        pending.sourceEventUid = sourceEventUid;
        pending.workflowSessionKey = ctx.getCurrentKey();
        pending.emitAfterProcessingTs = ctx.timerService().currentProcessingTime() + ATTRIBUTION_WAIT_MS;
        pendingByEventKey.put(pendingKey(traceEvent, edgeTs, sourceEventUid), pending);
        ctx.timerService().registerProcessingTimeTimer(pending.emitAfterProcessingTs);
    }

    @Override
    public void processElement2(
            ParsedEvent<EventPayloads.FactWorkflowSessionSegment> segmentEvent,
            Context ctx,
            Collector<ParsedEvent<EventPayloads.FactStreamTraceEdge>> out) throws Exception {
        if (segmentEvent == null || segmentEvent.payload == null) {
            return;
        }

        EventPayloads.FactWorkflowSessionSegment incoming = segmentEvent.payload;
        EventPayloads.FactWorkflowSessionSegment existing = segmentByIndex.get(incoming.segmentIndex);
        if (existing == null || incoming.version >= existing.version) {
            segmentByIndex.put(incoming.segmentIndex, incoming);
        }

        // Segment updates may unlock attribution for previously buffered trace rows.
        List<String> resolvedKeys = new ArrayList<>();
        for (Map.Entry<String, PendingTraceEvent> entry : pendingByEventKey.entries()) {
            PendingTraceEvent pending = entry.getValue();
            if (pending == null || pending.traceEvent == null || pending.traceEvent.payload == null) {
                resolvedKeys.add(entry.getKey());
                continue;
            }
            EventPayloads.FactWorkflowSessionSegment segment = findMatchingSegment(pending.edgeTs);
            if (segment == null) {
                continue;
            }
            out.collect(toTraceFact(
                    pending.traceEvent,
                    StringSemantics.firstNonBlank(pending.workflowSessionKey, ctx.getCurrentKey()),
                    pending.edgeTs,
                    pending.sourceEventUid,
                    segment,
                    1));
            resolvedKeys.add(entry.getKey());
        }
        for (String key : resolvedKeys) {
            pendingByEventKey.remove(key);
        }
    }

    @Override
    public void onTimer(
            long timestamp,
            OnTimerContext ctx,
            Collector<ParsedEvent<EventPayloads.FactStreamTraceEdge>> out) throws Exception {
        List<String> expiredKeys = new ArrayList<>();
        for (Map.Entry<String, PendingTraceEvent> entry : pendingByEventKey.entries()) {
            PendingTraceEvent pending = entry.getValue();
            if (pending == null || pending.traceEvent == null || pending.traceEvent.payload == null) {
                expiredKeys.add(entry.getKey());
                continue;
            }
            if (pending.emitAfterProcessingTs > timestamp) {
                continue;
            }

            EventPayloads.FactWorkflowSessionSegment segment = findMatchingSegment(pending.edgeTs);
            int attributed = segment == null ? 0 : 1;
            out.collect(toTraceFact(
                    pending.traceEvent,
                    StringSemantics.firstNonBlank(pending.workflowSessionKey, ctx.getCurrentKey()),
                    pending.edgeTs,
                    pending.sourceEventUid,
                    segment,
                    attributed));
            expiredKeys.add(entry.getKey());
        }
        for (String key : expiredKeys) {
            pendingByEventKey.remove(key);
        }
    }

    /**
     * Finds the best segment that covers the trace-edge timestamp.
     *
     * <p>Tie-break order matches status attribution: latest segment start, then latest version,
     * then highest segment index.</p>
     */
    private EventPayloads.FactWorkflowSessionSegment findMatchingSegment(long ts) throws Exception {
        EventPayloads.FactWorkflowSessionSegment best = null;
        for (EventPayloads.FactWorkflowSessionSegment candidate : segmentByIndex.values()) {
            if (candidate == null) {
                continue;
            }
            boolean startsBefore = ts >= candidate.segmentStartTs;
            boolean endsAfter = candidate.segmentEndTs == null || ts < candidate.segmentEndTs;
            if (!startsBefore || !endsAfter) {
                continue;
            }

            if (best == null
                    || candidate.segmentStartTs > best.segmentStartTs
                    || (candidate.segmentStartTs == best.segmentStartTs && candidate.version > best.version)
                    || (candidate.segmentStartTs == best.segmentStartTs && candidate.version == best.version
                    && candidate.segmentIndex > best.segmentIndex)) {
                best = candidate;
            }
        }
        return best;
    }

    private static String sourceEventUid(StreamingEvent event) {
        if (event == null) {
            return "";
        }
        if (!StringSemantics.isBlank(event.eventId)) {
            return event.eventId;
        }
        return Hashing.sha256Hex(event.rawJson == null ? "" : event.rawJson);
    }

    private static String hashRawJsonOrFallback(String rawJson, StreamingEvent event) {
        if (!StringSemantics.isBlank(rawJson)) {
            return Hashing.sha256Hex(rawJson);
        }
        return sourceEventUid(event);
    }

    private static String deriveTraceCategory(String traceType) {
        if (traceType == null) {
            return "other";
        }
        if (traceType.startsWith("gateway_")) {
            return "gateway";
        }
        if (traceType.startsWith("orchestrator_")) {
            return "orchestrator";
        }
        if (traceType.startsWith("runner_")) {
            return "runner";
        }
        if (traceType.startsWith("app_")) {
            return "app";
        }
        return "other";
    }

    private static String pendingKey(
            ParsedEvent<EventPayloads.StreamTraceEvent> traceEvent,
            long edgeTs,
            String sourceEventUid) {
        return sourceEventUid
                + "|"
                + edgeTs
                + "|"
            + StringSemantics.firstNonBlank(
                traceEvent == null || traceEvent.payload == null ? null : traceEvent.payload.traceType);
    }

    private ParsedEvent<EventPayloads.FactStreamTraceEdge> toTraceFact(
            ParsedEvent<EventPayloads.StreamTraceEvent> traceEvent,
            String workflowSessionKey,
            long edgeTs,
            String sourceEventUid,
            EventPayloads.FactWorkflowSessionSegment segment,
            int isAttributed) {
        EventPayloads.StreamTraceEvent trace = traceEvent.payload;
        EventPayloads.FactStreamTraceEdge fact = new EventPayloads.FactStreamTraceEdge();
        fact.edgeTs = edgeTs;
        fact.workflowSessionId = StringSemantics.firstNonBlank(
                workflowSessionKey,
                WorkflowSessionId.from(trace.streamId, trace.requestId, sourceEventUid(traceEvent.event)));
        fact.streamId = trace.streamId;
        fact.requestId = trace.requestId;
        fact.gateway = StringSemantics.firstNonBlank(traceEvent.event == null ? null : traceEvent.event.gateway);
        fact.traceType = trace.traceType;
        fact.traceCategory = deriveTraceCategory(trace.traceType);
        fact.isSwapEvent = "orchestrator_swap".equals(trace.traceType) ? 1 : 0;

        if (isCanonicalAttribution(segment, isAttributed)) {
            // Canonical attribution path: identity dimensions come only from matched segment.
            fact.orchestratorAddress = StringSemantics.firstNonBlank(segment.orchestratorAddress);
            fact.orchestratorUrl = StringSemantics.firstNonBlank(segment.orchestratorUrl, trace.orchestratorUrl);
            fact.pipeline = PipelineModelResolver.canonicalPipeline(segment.pipeline, segment.modelId);
            fact.modelId = StringSemantics.blankToNull(segment.modelId);
            fact.isAttributed = 1;
        } else {
            // Unattributed fallback preserves raw trace context only.
            fact.orchestratorAddress = "";
            fact.orchestratorUrl = StringSemantics.firstNonBlank(trace.orchestratorUrl);
            fact.pipeline = "";
            fact.modelId = null;
            fact.isAttributed = 0;
        }

        fact.sourceEventUid = sourceEventUid;
        return new ParsedEvent<>(traceEvent.event, fact);
    }

    private static boolean isCanonicalAttribution(EventPayloads.FactWorkflowSessionSegment segment, int isAttributed) {
        return segment != null
                && isAttributed == 1
                && !StringSemantics.isBlank(segment.orchestratorAddress);
    }

    public static final class PendingTraceEvent implements Serializable {
        private static final long serialVersionUID = 1L;

        /** Original trace row waiting for segment alignment. */
        public ParsedEvent<EventPayloads.StreamTraceEvent> traceEvent;
        /** Event-time timestamp used for segment-window match. */
        public long edgeTs;
        /** Deterministic source identifier propagated into emitted fact rows. */
        public String sourceEventUid;
        /** Keyed workflow-session context captured at enqueue time. */
        public String workflowSessionKey;
        /** Processing-time deadline for emitting unattributed fallback if unresolved. */
        public long emitAfterProcessingTs;
    }
}
