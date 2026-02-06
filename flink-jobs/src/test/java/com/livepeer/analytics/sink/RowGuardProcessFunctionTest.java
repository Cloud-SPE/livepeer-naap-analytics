package com.livepeer.analytics.sink;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.Test;

import com.livepeer.analytics.model.ParsedEvent;
import com.livepeer.analytics.model.RejectedEventEnvelope;
import com.livepeer.analytics.model.StreamingEvent;

import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.*;

class RowGuardProcessFunctionTest {

    @Test
    void parsedEventOversizeEmitsDlq() throws Exception {
        OutputTag<RejectedEventEnvelope> dlqTag = new OutputTag<RejectedEventEnvelope>("dlq"){};
        ParsedEventRowGuardProcessFunction<String> fn = new ParsedEventRowGuardProcessFunction<>(
                payload -> "12345678901",
                dlqTag,
                10,
                true);

        var harness = ProcessFunctionTestHarnesses.forProcessFunction(fn);
        harness.open();

        StreamingEvent event = new StreamingEvent();
        event.eventId = "evt-1";
        event.eventType = "ai_stream_status";
        event.eventVersion = "1";
        event.timestamp = 1710000000000L;
        event.rawJson = "{}";

        ParsedEvent<String> parsed = new ParsedEvent<>(event, "payload");
        harness.processElement(parsed, 0L);

        ConcurrentLinkedQueue<StreamRecord<RejectedEventEnvelope>> rawDlqRecords = harness.getSideOutput(dlqTag);
        assertNotNull(rawDlqRecords);
        assertEquals(1, rawDlqRecords.size());
        
        RejectedEventEnvelope envelope = rawDlqRecords.iterator().next().getValue();
        assertEquals("SINK_GUARD", envelope.failure.stage);
        assertEquals("RECORD_TOO_LARGE", envelope.failure.failureClass);
        assertEquals(0, harness.getOutput().size());
    }

    @Test
    void dlqOversizeIsDropped() throws Exception {
        EnvelopeRowGuardProcessFunction fn = new EnvelopeRowGuardProcessFunction(
                envelope -> "12345678901",
                10);

        var harness = ProcessFunctionTestHarnesses.forProcessFunction(fn);
        harness.open();

        RejectedEventEnvelope envelope = new RejectedEventEnvelope();
        envelope.schemaVersion = "v1";
        envelope.ingestionTimestamp = System.currentTimeMillis();

        harness.processElement(envelope, 0L);

        assertEquals(0, harness.getOutput().size());
    }

    @Test
    void parsedEventWithinLimitEmitsRow() throws Exception {
        OutputTag<RejectedEventEnvelope> dlqTag = new OutputTag<RejectedEventEnvelope>("dlq"){};
        ParsedEventRowGuardProcessFunction<String> fn = new ParsedEventRowGuardProcessFunction<>(
                payload -> "ok",
                dlqTag,
                10,
                true);

        var harness = ProcessFunctionTestHarnesses.forProcessFunction(fn);
        harness.open();

        StreamingEvent event = new StreamingEvent();
        event.eventId = "evt-2";
        event.eventType = "ai_stream_status";
        event.eventVersion = "1";
        event.timestamp = 1710000000000L;
        event.rawJson = "{}";

        ParsedEvent<String> parsed = new ParsedEvent<>(event, "payload");
        harness.processElement(parsed, 0L);

        assertNull(harness.getSideOutput(dlqTag), "Expected no DLQ side output for valid events");
        assertEquals(1, harness.extractOutputValues().size());
        assertEquals("ok", harness.extractOutputValues().get(0));
    }
}
