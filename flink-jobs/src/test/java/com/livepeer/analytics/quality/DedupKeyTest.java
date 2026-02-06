package com.livepeer.analytics.quality;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;

import com.livepeer.analytics.model.StreamingEvent;
import com.livepeer.analytics.util.JsonSupport;

import static org.junit.jupiter.api.Assertions.*;

class DedupKeyTest {

    @Test
    void usesEventIdWhenPresent() throws Exception {
        StreamingEvent event = new StreamingEvent();
        event.eventId = "evt-123";
        event.eventType = "ai_stream_status";
        event.rawJson = "{\"id\":\"evt-123\",\"type\":\"ai_stream_status\",\"timestamp\":1710000000000,\"data\":{\"stream_id\":\"s1\",\"request_id\":\"r1\",\"pipeline\":\"p1\",\"pipeline_id\":\"pid\"}}";

        JsonNode node = JsonSupport.MAPPER.readTree(event.rawJson);
        QualityGateProcessFunction.DedupKey dedupKey = QualityGateProcessFunction.DedupKey.build(event, node);

        assertEquals("evt-123", dedupKey.key);
        assertEquals("event_id", dedupKey.strategy);
    }

    @Test
    void hashesPayloadWhenEventIdMissing() throws Exception {
        StreamingEvent event = new StreamingEvent();
        event.eventId = "";
        event.eventType = "ai_stream_status";
        event.rawJson = "{\"type\":\"ai_stream_status\",\"timestamp\":1710000000000,\"data\":{\"stream_id\":\"s1\",\"request_id\":\"r1\",\"pipeline\":\"p1\",\"pipeline_id\":\"pid\"}}";

        JsonNode node = JsonSupport.MAPPER.readTree(event.rawJson);
        QualityGateProcessFunction.DedupKey dedupKey = QualityGateProcessFunction.DedupKey.build(event, node);

        assertNotNull(dedupKey.key);
        assertFalse(dedupKey.key.isEmpty());
        assertEquals("payload_hash", dedupKey.strategy);
    }
}
