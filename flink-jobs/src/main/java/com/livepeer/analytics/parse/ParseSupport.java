package com.livepeer.analytics.parse;

import com.fasterxml.jackson.databind.JsonNode;

import com.livepeer.analytics.quality.ValidatedEvent;

/**
 * Shared parser support helpers.
 */
final class ParseSupport {
    private ParseSupport() {}

    static JsonNode requireData(ValidatedEvent event, String eventType) throws Exception {
        JsonNode data = event.data();
        if (data == null || data.isMissingNode() || data.isNull()) {
            throw new Exception("Missing data payload for " + eventType);
        }
        return data;
    }
}
