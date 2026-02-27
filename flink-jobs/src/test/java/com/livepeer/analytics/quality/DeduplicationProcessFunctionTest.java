package com.livepeer.analytics.quality;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.Test;

import com.livepeer.analytics.model.RejectedEventEnvelope;
import com.livepeer.analytics.model.StreamingEvent;

import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.*;

class DeduplicationProcessFunctionTest {

    @Test
    void duplicateEmitsQuarantineEnvelope() throws Exception {
        OutputTag<RejectedEventEnvelope> quarantineTag = new OutputTag<RejectedEventEnvelope>("quarantine"){};
        QualityGateConfig config = QualityGateConfig.fromEnv();

        DeduplicationProcessFunction function = new DeduplicationProcessFunction(config, quarantineTag);
        try (KeyedOneInputStreamOperatorTestHarness<String, ValidatedEvent, ValidatedEvent> harness =
            ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                function,
                (ValidatedEvent e) -> e.event.dedupKey,
                Types.STRING)) {

            harness.open();

            ValidatedEvent event = buildValidatedEvent("evt-1", "ai_stream_status", "dedup-1");

            harness.processElement(event, 0L);
            harness.processElement(event, 1L);

            ConcurrentLinkedQueue<StreamRecord<RejectedEventEnvelope>> rawQuarantineRecords = harness.getSideOutput(quarantineTag);
            assertEquals(1, rawQuarantineRecords.size());
            RejectedEventEnvelope envelope = rawQuarantineRecords.iterator().next().getValue();
            assertEquals("DUPLICATE", envelope.failure.failureClass);

            assertEquals(1, harness.extractOutputValues().size());
        }
    }

    private static ValidatedEvent buildValidatedEvent(String eventId, String eventType, String dedupKey) {
        StreamingEvent event = new StreamingEvent();
        event.eventId = eventId;
        event.eventType = eventType;
        event.eventVersion = "1";
        event.dedupKey = dedupKey;
        event.dedupStrategy = "event_id";
        event.timestamp = 1710000000000L;
        event.rawJson = "{}";
        event.replay = false;
        return new ValidatedEvent(event, null);
    }
}
