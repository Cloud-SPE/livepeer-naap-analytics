package com.livepeer.analytics.parse;

import com.fasterxml.jackson.databind.JsonNode;

import com.livepeer.analytics.model.EventPayloads;
import com.livepeer.analytics.quality.ValidatedEvent;

import java.util.ArrayList;
import java.util.List;

/**
 * Parser for `discovery_results` events.
 */
final class DiscoveryResultsParser {
    private DiscoveryResultsParser() {}

    static List<EventPayloads.DiscoveryResult> parse(ValidatedEvent event) throws Exception {
        JsonNode dataArray = event.data();
        if (dataArray == null || !dataArray.isArray() || dataArray.size() == 0) {
            throw new Exception("No valid discovery data found");
        }

        List<EventPayloads.DiscoveryResult> results = new ArrayList<>();
        for (JsonNode disc : dataArray) {
            EventPayloads.DiscoveryResult row = new EventPayloads.DiscoveryResult();
            row.eventTimestamp = event.event.timestamp;
            row.orchestratorAddress = disc.path("address").asText("");
            row.orchestratorUrl = disc.path("url").asText("");
            row.latencyMs = JsonNodeUtils.asIntOrDefault(disc.path("latency_ms"), 0);
            row.rawJson = disc.toString();
            results.add(row);
        }

        if (results.isEmpty()) {
            throw new Exception("No valid discovery data found");
        }
        return results;
    }
}
