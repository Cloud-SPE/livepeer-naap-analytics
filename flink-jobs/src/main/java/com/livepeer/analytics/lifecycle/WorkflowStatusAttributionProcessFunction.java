package com.livepeer.analytics.lifecycle;

import com.livepeer.analytics.model.EventPayloads;
import com.livepeer.analytics.model.ParsedEvent;
import com.livepeer.analytics.model.StreamingEvent;
import com.livepeer.analytics.util.EventUids;
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
 * Attributes status samples to workflow segment identity windows.
 *
 * <p>Why pending-buffer logic exists:
 * status samples and segment facts are emitted on separate keyed streams and may arrive out of
 * order. Without a short retry window, valid samples can be emitted as permanently unattributed
 * simply because segment state was delayed. This operator buffers unmatched samples briefly,
 * retries on segment updates, and emits a final fallback on timer expiry.</p>
 */
public class WorkflowStatusAttributionProcessFunction extends KeyedCoProcessFunction<
        String,
        ParsedEvent<EventPayloads.AiStreamStatus>,
        ParsedEvent<EventPayloads.FactWorkflowSessionSegment>,
        ParsedEvent<EventPayloads.FactStreamStatusSample>> {
    private static final long serialVersionUID = 1L;
    // Processing-time grace period used to absorb expected event reordering.
    private static final long ATTRIBUTION_WAIT_MS = 15_000L;

    private transient MapState<Integer, EventPayloads.FactWorkflowSessionSegment> segmentByIndex;
    private transient MapState<String, PendingStatusEvent> pendingByEventKey;

    @Override
    public void open(Configuration parameters) {
        segmentByIndex = getRuntimeContext().getMapState(
                new MapStateDescriptor<>(
                        "segment-by-index-for-status",
                        Integer.class,
                        EventPayloads.FactWorkflowSessionSegment.class));
        pendingByEventKey = getRuntimeContext().getMapState(
            new MapStateDescriptor<>(
                "pending-status-events-for-attribution",
                String.class,
                PendingStatusEvent.class));
    }

    @Override
    public void processElement1(
            ParsedEvent<EventPayloads.AiStreamStatus> statusEvent,
            Context ctx,
            Collector<ParsedEvent<EventPayloads.FactStreamStatusSample>> out) throws Exception {
        if (statusEvent == null || statusEvent.payload == null) {
            return;
        }

        EventPayloads.AiStreamStatus status = statusEvent.payload;
        long sampleTs = status.eventTimestamp;
        EventPayloads.FactWorkflowSessionSegment segment = findMatchingSegment(sampleTs);
        String sourceEventUid = StringSemantics.firstNonBlank(status.rawEventUid, sourceEventUid(statusEvent.event));

        if (segment != null) {
            out.collect(toStatusFact(statusEvent, ctx.getCurrentKey(), sampleTs, sourceEventUid, segment, 1));
            return;
        }

        // No matching segment yet: stage and retry attribution.
        PendingStatusEvent pending = new PendingStatusEvent();
        pending.statusEvent = statusEvent;
        pending.sampleTs = sampleTs;
        pending.sourceEventUid = sourceEventUid;
        pending.workflowSessionKey = ctx.getCurrentKey();
        pending.emitAfterProcessingTs = ctx.timerService().currentProcessingTime() + ATTRIBUTION_WAIT_MS;
        pendingByEventKey.put(pendingKey(statusEvent, sampleTs, sourceEventUid), pending);
        ctx.timerService().registerProcessingTimeTimer(pending.emitAfterProcessingTs);
    }

    @Override
    public void processElement2(
            ParsedEvent<EventPayloads.FactWorkflowSessionSegment> segmentEvent,
            Context ctx,
            Collector<ParsedEvent<EventPayloads.FactStreamStatusSample>> out) throws Exception {
        if (segmentEvent == null || segmentEvent.payload == null) {
            return;
        }

        EventPayloads.FactWorkflowSessionSegment incoming = segmentEvent.payload;
        EventPayloads.FactWorkflowSessionSegment existing = segmentByIndex.get(incoming.segmentIndex);
        if (existing == null || incoming.version >= existing.version) {
            segmentByIndex.put(incoming.segmentIndex, incoming);
        }

        // Segment updates may satisfy buffered status samples.
        List<String> resolvedKeys = new ArrayList<>();
        for (Map.Entry<String, PendingStatusEvent> entry : pendingByEventKey.entries()) {
            PendingStatusEvent pending = entry.getValue();
            if (pending == null || pending.statusEvent == null || pending.statusEvent.payload == null) {
                resolvedKeys.add(entry.getKey());
                continue;
            }
            EventPayloads.FactWorkflowSessionSegment segment = findMatchingSegment(pending.sampleTs);
            if (segment == null) {
                continue;
            }
            out.collect(toStatusFact(
                    pending.statusEvent,
                    StringSemantics.firstNonBlank(pending.workflowSessionKey, ctx.getCurrentKey()),
                    pending.sampleTs,
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
            Collector<ParsedEvent<EventPayloads.FactStreamStatusSample>> out) throws Exception {
        List<String> expiredKeys = new ArrayList<>();
        for (Map.Entry<String, PendingStatusEvent> entry : pendingByEventKey.entries()) {
            PendingStatusEvent pending = entry.getValue();
            if (pending == null || pending.statusEvent == null || pending.statusEvent.payload == null) {
                expiredKeys.add(entry.getKey());
                continue;
            }
            if (pending.emitAfterProcessingTs > timestamp) {
                continue;
            }

            EventPayloads.FactWorkflowSessionSegment segment = findMatchingSegment(pending.sampleTs);
            int attributed = segment == null ? 0 : 1;
            out.collect(toStatusFact(
                    pending.statusEvent,
                    StringSemantics.firstNonBlank(pending.workflowSessionKey, ctx.getCurrentKey()),
                    pending.sampleTs,
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
     * Finds the best segment that covers the sample timestamp.
     *
     * <p>Tie-break order is deterministic: newest segment window start first, then highest
     * segment version, then highest segment index. This preserves monotonic preference for
     * the latest authoritative segment state when overlaps occur.</p>
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
        return EventUids.rawEventUid(event);
    }

    private static String pendingKey(
            ParsedEvent<EventPayloads.AiStreamStatus> statusEvent,
            long sampleTs,
            String sourceEventUid) {
        return sourceEventUid
                + "|"
                + sampleTs
                + "|"
                + StringSemantics.firstNonBlank(
                        statusEvent == null || statusEvent.payload == null ? null : statusEvent.payload.streamId)
                + "|"
                + StringSemantics.firstNonBlank(
                        statusEvent == null || statusEvent.payload == null ? null : statusEvent.payload.requestId);
    }

    private ParsedEvent<EventPayloads.FactStreamStatusSample> toStatusFact(
            ParsedEvent<EventPayloads.AiStreamStatus> statusEvent,
            String workflowSessionKey,
            long sampleTs,
            String sourceEventUid,
            EventPayloads.FactWorkflowSessionSegment segment,
            int isAttributed) {
        EventPayloads.AiStreamStatus status = statusEvent.payload;
        EventPayloads.FactStreamStatusSample fact = new EventPayloads.FactStreamStatusSample();
        fact.sampleTs = sampleTs;
        fact.workflowSessionId = StringSemantics.firstNonBlank(
                workflowSessionKey,
                WorkflowSessionId.from(status.streamId, status.requestId, sourceEventUid(statusEvent.event)));
        fact.streamId = status.streamId;
        fact.requestId = status.requestId;
        fact.gateway = status.gateway;
        fact.state = status.state;
        fact.outputFps = status.outputFps;
        fact.inputFps = status.inputFps;

        if (isCanonicalAttribution(segment, isAttributed)) {
            // Canonical attribution path: segment identity is authoritative.
            fact.orchestratorAddress = StringSemantics.firstNonBlank(segment.orchestratorAddress);
            fact.orchestratorUrl = StringSemantics.firstNonBlank(segment.orchestratorUrl, status.orchestratorUrl);
            fact.pipeline = PipelineModelResolver.canonicalPipeline(segment.pipeline, segment.modelId);
            fact.modelId = StringSemantics.blankToNull(segment.modelId);
            fact.gpuId = StringSemantics.blankToNull(segment.gpuId);
            fact.region = StringSemantics.blankToNull(segment.region);
            fact.isAttributed = 1;
            fact.attributionMethod = "segment_window_join";
            fact.attributionConfidence = 1.0f;
        } else {
            // Unattributed fallback must never project canonical identity dimensions.
            fact.orchestratorAddress = "";
            fact.orchestratorUrl = StringSemantics.firstNonBlank(status.orchestratorUrl);
            fact.pipeline = "";
            fact.modelId = null;
            fact.gpuId = null;
            fact.region = null;
            fact.isAttributed = 0;
            fact.attributionMethod = "none";
            fact.attributionConfidence = 0.0f;
        }

        fact.sourceEventUid = sourceEventUid;
        return new ParsedEvent<>(statusEvent.event, fact);
    }

    private static boolean isCanonicalAttribution(EventPayloads.FactWorkflowSessionSegment segment, int isAttributed) {
        return segment != null
                && isAttributed == 1
                && !StringSemantics.isBlank(segment.orchestratorAddress);
    }

    public static final class PendingStatusEvent implements Serializable {
        private static final long serialVersionUID = 1L;

        /** Original status row waiting for segment alignment. */
        public ParsedEvent<EventPayloads.AiStreamStatus> statusEvent;
        /** Event-time timestamp used for segment-window match. */
        public long sampleTs;
        /** Deterministic source identifier propagated into emitted fact rows. */
        public String sourceEventUid;
        /** Keyed workflow-session context captured at enqueue time. */
        public String workflowSessionKey;
        /** Processing-time deadline for emitting unattributed fallback if unresolved. */
        public long emitAfterProcessingTs;
    }
}
