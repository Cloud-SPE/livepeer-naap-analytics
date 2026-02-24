package com.livepeer.analytics.quality;

import org.junit.jupiter.api.Test;

import com.livepeer.analytics.model.RejectedEventEnvelope;
import com.livepeer.analytics.model.StreamingEvent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RejectedEventEnvelopeFactoryTest {

    @Test
    void parseFailureIncludesExceptionClassWhenMessageMissing() {
        StreamingEvent event = new StreamingEvent();
        event.eventId = "evt-1";
        event.eventType = "network_capabilities";
        event.rawJson = "{\"id\":\"evt-1\",\"type\":\"network_capabilities\",\"data\":[]}";
        event.timestamp = 1771936670203L;

        RejectedEventEnvelope envelope = RejectedEventEnvelopeFactory.forParseFailure(event, new NullPointerException());

        assertNotNull(envelope.failure);
        assertEquals("PARSE", envelope.failure.stage);
        assertEquals("PARSING_FAILED", envelope.failure.failureClass);
        assertEquals("parse_exception", envelope.failure.reason);
        assertTrue(envelope.failure.details.contains("NullPointerException"));
    }
}
