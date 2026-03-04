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

class WorkflowStatusAttributionProcessFunctionTest {

    @Test
    void buffersUnmatchedStatusAndEmitsAttributedAfterSegmentArrives() throws Exception {
        WorkflowStatusAttributionProcessFunction function = new WorkflowStatusAttributionProcessFunction();

        try (KeyedTwoInputStreamOperatorTestHarness<
                String,
                ParsedEvent<EventPayloads.AiStreamStatus>,
                ParsedEvent<EventPayloads.FactWorkflowSessionSegment>,
                ParsedEvent<EventPayloads.FactStreamStatusSample>> harness =
                 new KeyedTwoInputStreamOperatorTestHarness<String,
                     ParsedEvent<EventPayloads.AiStreamStatus>,
                     ParsedEvent<EventPayloads.FactWorkflowSessionSegment>,
                     ParsedEvent<EventPayloads.FactStreamStatusSample>>(
                     new KeyedCoProcessOperator<>(function),
                             event -> "wf-1",
                             event -> event.payload.workflowSessionId,
                             Types.STRING)) {

            harness.open();

            ParsedEvent<EventPayloads.AiStreamStatus> statusEvent = statusEvent(2_000L);
            harness.processElement1(new StreamRecord<>(statusEvent, 2_000L));
            assertEquals(0, harness.extractOutputValues().size());

            ParsedEvent<EventPayloads.FactWorkflowSessionSegment> segmentEvent = segmentEvent("wf-1", 1_900L, null);
            harness.processElement2(new StreamRecord<>(segmentEvent, 2_001L));

            List<ParsedEvent<EventPayloads.FactStreamStatusSample>> out = harness.extractOutputValues();
            assertEquals(1, out.size());
            EventPayloads.FactStreamStatusSample fact = out.get(0).payload;
            assertEquals(1, fact.isAttributed);
            assertEquals("live-video-to-video", fact.pipeline);
            assertEquals("streamdiffusion-sdxl-v2v", fact.modelId);
            assertEquals("gpu-1", fact.gpuId);
            assertEquals("0xcanonical", fact.orchestratorAddress);
            assertEquals("segment_window_join", fact.attributionMethod);
            assertEquals("wf-1", fact.workflowSessionId);
        }
    }

    private static ParsedEvent<EventPayloads.AiStreamStatus> statusEvent(long ts) {
        EventPayloads.AiStreamStatus payload = new EventPayloads.AiStreamStatus();
        payload.eventTimestamp = ts;
        payload.streamId = "stream-1";
        payload.requestId = "req-1";
        payload.gateway = "naap-gateway";
        payload.orchestratorUrl = "https://orch.example";
        payload.state = "running";
        payload.outputFps = 24.0f;
        payload.inputFps = 30.0f;
        payload.rawJson = "{\"id\":\"status-1\"}";

        StreamingEvent event = new StreamingEvent();
        event.gateway = "naap-gateway";
        event.eventId = "status-evt-1";
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
        segment.region = "us-west";
        segment.version = 1L;

        return new ParsedEvent<>(null, segment);
    }
}
