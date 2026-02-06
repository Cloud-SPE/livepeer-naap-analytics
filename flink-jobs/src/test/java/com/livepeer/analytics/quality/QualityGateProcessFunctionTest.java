package com.livepeer.analytics.quality;

import org.apache.flink.util.OutputTag;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.Test;

import com.livepeer.analytics.model.KafkaInboundRecord;
import com.livepeer.analytics.model.RejectedEventEnvelope;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.*;

class QualityGateProcessFunctionTest {

    @Test
    void replayFlagPropagatesToDlqOnSchemaFailure() throws Exception {
        OutputTag<RejectedEventEnvelope> dlqTag = new OutputTag<RejectedEventEnvelope>("dlq"){};
        QualityGateConfig config = QualityGateConfig.fromEnv();

        OneInputStreamOperatorTestHarness<KafkaInboundRecord, ValidatedEvent> harness =
                ProcessFunctionTestHarnesses.forProcessFunction(new QualityGateProcessFunction(config, dlqTag));
        harness.open();

        String json = "{\"id\":\"evt-1\",\"type\":\"ai_stream_status\",\"timestamp\":\"1710000000000\",\"__replay\":true,\"data\":{\"request_id\":\"r1\",\"pipeline\":\"p1\"}}";
        KafkaInboundRecord record = new KafkaInboundRecord();
        record.topic = "streaming_events";
        record.partition = 0;
        record.offset = 1L;
        record.recordTimestamp = 1710000000000L;
        record.value = json.getBytes(StandardCharsets.UTF_8);

        harness.processElement(record, 0L);

        ConcurrentLinkedQueue<StreamRecord<RejectedEventEnvelope>> rawDlqRecords = harness.getSideOutput(dlqTag);
        assertEquals(1, rawDlqRecords.size());
        RejectedEventEnvelope envelope = rawDlqRecords.iterator().next().getValue();
        assertEquals("SCHEMA_INVALID", envelope.failure.failureClass);


        assertNotNull(rawDlqRecords);
        assertEquals(1, rawDlqRecords.size());
        assertTrue(rawDlqRecords.iterator().next().getValue().replay, "Expected replay flag to be preserved in DLQ envelope");
        assertEquals(0, harness.extractOutputValues().size());
    }

    @Test
    void validEventEmitsValidatedEvent() throws Exception {
        OutputTag<RejectedEventEnvelope> dlqTag = new OutputTag<RejectedEventEnvelope>("dlq"){};
        QualityGateConfig config = QualityGateConfig.fromEnv();

        OneInputStreamOperatorTestHarness<KafkaInboundRecord, ValidatedEvent> harness =
                ProcessFunctionTestHarnesses.forProcessFunction(new QualityGateProcessFunction(config, dlqTag));
        harness.open();

        String json = "{\"id\":\"evt-2\",\"type\":\"ai_stream_status\",\"timestamp\":1710000000000," +
                "\"data\":{\"stream_id\":\"s1\",\"request_id\":\"r1\",\"pipeline\":\"p1\"}}";
        KafkaInboundRecord record = new KafkaInboundRecord();
        record.topic = "streaming_events";
        record.partition = 0;
        record.offset = 2L;
        record.recordTimestamp = 1710000000000L;
        record.value = json.getBytes(StandardCharsets.UTF_8);

        harness.processElement(record, 0L);

        assertNull(harness.getSideOutput(dlqTag), "Expected no DLQ side output for valid events");
        assertEquals(1, harness.extractOutputValues().size());
        ValidatedEvent validated = harness.extractOutputValues().get(0);
        assertEquals("evt-2", validated.event.eventId);
        assertEquals("ai_stream_status", validated.event.eventType);
    }
}
