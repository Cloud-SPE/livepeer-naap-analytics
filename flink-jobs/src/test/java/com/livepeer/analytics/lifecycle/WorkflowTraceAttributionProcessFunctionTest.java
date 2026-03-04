package com.livepeer.analytics.lifecycle;

import com.livepeer.analytics.model.EventPayloads;
import com.livepeer.analytics.model.ParsedEvent;
import com.livepeer.analytics.model.StreamingEvent;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class WorkflowTraceAttributionProcessFunctionTest {

    @Test
    void buffersUnmatchedTraceAndEmitsAttributedAfterSegmentArrives() throws Exception {
        WorkflowTraceAttributionProcessFunction function = new WorkflowTraceAttributionProcessFunction();

        try (KeyedTwoInputStreamOperatorTestHarness<
                String,
                ParsedEvent<EventPayloads.StreamTraceEvent>,
                ParsedEvent<EventPayloads.FactWorkflowSessionSegment>,
                ParsedEvent<EventPayloads.FactStreamTraceEdge>> harness =
                 new KeyedTwoInputStreamOperatorTestHarness<String,
                     ParsedEvent<EventPayloads.StreamTraceEvent>,
                     ParsedEvent<EventPayloads.FactWorkflowSessionSegment>,
                     ParsedEvent<EventPayloads.FactStreamTraceEdge>>(
                     new KeyedCoProcessOperator<>(function),
                             event -> "wf-1",
                             event -> event.payload.workflowSessionId,
                             Types.STRING)) {

            harness.open();

            ParsedEvent<EventPayloads.StreamTraceEvent> traceEvent = traceEvent(1_000L);
            harness.processElement1(new StreamRecord<>(traceEvent, 1_000L));
            assertEquals(0, harness.extractOutputValues().size());

            ParsedEvent<EventPayloads.FactWorkflowSessionSegment> segmentEvent = segmentEvent("wf-1", 900L, null);
            harness.processElement2(new StreamRecord<>(segmentEvent, 1_001L));

            List<ParsedEvent<EventPayloads.FactStreamTraceEdge>> out = harness.extractOutputValues();
            assertEquals(1, out.size());
            EventPayloads.FactStreamTraceEdge fact = out.get(0).payload;
            assertEquals(1, fact.isAttributed);
            assertEquals("live-video-to-video", fact.pipeline);
            assertEquals("streamdiffusion-sdxl-v2v", fact.modelId);
            assertEquals("0xcanonical", fact.orchestratorAddress);
            assertEquals("wf-1", fact.workflowSessionId);
        }
    }

    private static ParsedEvent<EventPayloads.StreamTraceEvent> traceEvent(long ts) {
        EventPayloads.StreamTraceEvent payload = new EventPayloads.StreamTraceEvent();
        payload.eventTimestamp = ts;
        payload.dataTimestamp = ts;
        payload.streamId = "stream-1";
        payload.requestId = "req-1";
        payload.traceType = "gateway_receive_first_processed_segment";
        payload.orchestratorUrl = "https://orch.example";
        payload.rawJson = "{\"id\":\"trace-1\"}";

        StreamingEvent event = new StreamingEvent();
        event.gateway = "naap-gateway";
        event.eventId = "trace-evt-1";
        event.rawJson = payload.rawJson;

        return new ParsedEvent<>(event, payload);
    }

    private static ParsedEvent<EventPayloads.FactWorkflowSessionSegment> segmentEvent(
            String workflowSessionId,
            long startTs,
            Long endTs) {
        EventPayloads.FactWorkflowSessionSegment segment = new EventPayloads.FactWorkflowSessionSegment();
        segment.workflowSessionId = workflowSessionId;
        segment.segmentIndex = 0;
        segment.segmentStartTs = startTs;
        segment.segmentEndTs = endTs;
        segment.orchestratorAddress = "0xcanonical";
        segment.orchestratorUrl = "https://orch.example";
        segment.pipeline = "live-video-to-video";
        segment.modelId = "streamdiffusion-sdxl-v2v";
        segment.gpuId = "gpu-1";
        segment.version = 1L;

        return new ParsedEvent<>(null, segment);
    }
}
