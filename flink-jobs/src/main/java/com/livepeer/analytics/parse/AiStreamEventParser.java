package com.livepeer.analytics.parse;

import com.fasterxml.jackson.databind.JsonNode;

import com.livepeer.analytics.model.EventPayloads;
import com.livepeer.analytics.quality.ValidatedEvent;

import java.util.ArrayList;
import java.util.List;

/**
 * Parser for `ai_stream_events` events.
 */
final class AiStreamEventParser {
    private AiStreamEventParser() {}

    static List<EventPayloads.AiStreamEvent> parse(ValidatedEvent event) throws Exception {
        JsonNode data = ParseSupport.requireData(event, "ai_stream_events");
        EventPayloads.AiStreamEvent parsed = new EventPayloads.AiStreamEvent();

        parsed.eventTimestamp = event.event.timestamp;
        parsed.streamId = data.path("stream_id").asText("");
        parsed.requestId = data.path("request_id").asText("");
        parsed.pipeline = data.path("pipeline").asText("");
        parsed.pipelineId = data.path("pipeline_id").asText("");
        parsed.eventType = data.path("type").asText("");
        parsed.message = data.path("message").asText("");
        parsed.capability = data.path("capability").asText("");
        parsed.rawJson = event.event.rawJson;

        List<EventPayloads.AiStreamEvent> results = new ArrayList<>(1);
        results.add(parsed);
        return results;
    }
}
