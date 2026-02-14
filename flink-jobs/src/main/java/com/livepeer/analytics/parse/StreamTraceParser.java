package com.livepeer.analytics.parse;

import com.fasterxml.jackson.databind.JsonNode;

import com.livepeer.analytics.model.EventPayloads;
import com.livepeer.analytics.quality.ValidatedEvent;

import java.util.ArrayList;
import java.util.List;

/**
 * Parser for `stream_trace` events.
 */
final class StreamTraceParser {
    private StreamTraceParser() {}

    static List<EventPayloads.StreamTraceEvent> parse(ValidatedEvent event) throws Exception {
        JsonNode data = ParseSupport.requireData(event, "stream_trace");
        EventPayloads.StreamTraceEvent trace = new EventPayloads.StreamTraceEvent();

        trace.eventTimestamp = event.event.timestamp;
        trace.streamId = data.path("stream_id").asText("");
        trace.requestId = data.path("request_id").asText("");
        trace.pipelineId = data.path("pipeline_id").asText("");
        trace.traceType = data.path("type").asText("");

        JsonNode orchInfo = data.path("orchestrator_info");
        trace.orchestratorAddress = orchInfo.path("address").asText("");
        trace.orchestratorUrl = orchInfo.path("url").asText("");

        trace.dataTimestamp = JsonNodeUtils.parseTimestampMillisOrDefault(data.path("timestamp"), event.event.timestamp);
        trace.rawJson = event.event.rawJson;

        List<EventPayloads.StreamTraceEvent> results = new ArrayList<>(1);
        results.add(trace);
        return results;
    }
}
