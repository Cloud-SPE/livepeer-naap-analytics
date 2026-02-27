package com.livepeer.analytics.parse;

import com.fasterxml.jackson.databind.JsonNode;

import com.livepeer.analytics.model.EventPayloads;
import com.livepeer.analytics.quality.ValidatedEvent;

import java.util.ArrayList;
import java.util.List;

/**
 * Parser for `create_new_payment` events.
 */
final class PaymentEventParser {
    private PaymentEventParser() {}

    static List<EventPayloads.PaymentEvent> parse(ValidatedEvent event) throws Exception {
        JsonNode data = ParseSupport.requireData(event, "create_new_payment");
        EventPayloads.PaymentEvent payment = new EventPayloads.PaymentEvent();

        payment.eventTimestamp = event.event.timestamp;
        payment.requestId = data.path("requestID").asText("");
        payment.sessionId = data.path("sessionID").asText("");
        payment.manifestId = data.path("manifestID").asText("");
        payment.sender = data.path("sender").asText("");
        payment.recipient = data.path("recipient").asText("");
        payment.orchestrator = data.path("orchestrator").asText("");
        payment.faceValue = data.path("faceValue").asText("");
        payment.price = data.path("price").asText("");
        payment.numTickets = data.path("numTickets").asText("");
        payment.winProb = data.path("winProb").asText("");
        payment.clientIp = data.path("clientIP").asText("");
        payment.capability = data.path("capability").asText("");
        payment.rawJson = event.event.rawJson;

        List<EventPayloads.PaymentEvent> results = new ArrayList<>(1);
        results.add(payment);
        return results;
    }
}
