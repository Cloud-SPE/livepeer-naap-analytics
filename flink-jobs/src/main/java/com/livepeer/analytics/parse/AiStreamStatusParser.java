package com.livepeer.analytics.parse;

import com.fasterxml.jackson.databind.JsonNode;

import com.livepeer.analytics.model.EventPayloads;
import com.livepeer.analytics.quality.ValidatedEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Parser for `ai_stream_status` events.
 */
final class AiStreamStatusParser {
    private AiStreamStatusParser() {}

    static List<EventPayloads.AiStreamStatus> parse(ValidatedEvent event) throws Exception {
        JsonNode data = ParseSupport.requireData(event, "ai_stream_status");
        EventPayloads.AiStreamStatus status = new EventPayloads.AiStreamStatus();

        status.eventTimestamp = event.event.timestamp;
        status.streamId = data.path("stream_id").asText("");
        status.requestId = data.path("request_id").asText("");
        status.gateway = event.event.gateway;

        JsonNode orchInfo = data.path("orchestrator_info");
        status.orchestratorAddress = normalizeAddress(orchInfo.path("address").asText(""));
        status.orchestratorUrl = orchInfo.path("url").asText("");

        // Upstream `pipeline` is the model label (e.g. streamdiffusion-sdxl).
        status.pipeline = data.path("pipeline").asText("");
        status.state = data.path("state").asText("");

        JsonNode inferStatus = data.path("inference_status");
        status.outputFps = (float) inferStatus.path("fps").asDouble(0.0);
        status.restartCount = inferStatus.path("restart_count").asInt(0);
        status.lastError = JsonNodeUtils.asNullableText(inferStatus.path("last_error"));
        status.lastErrorTime = JsonNodeUtils.parseTimestampMillis(inferStatus.path("last_error_time"));
        status.paramsHash = inferStatus.path("last_params_hash").asText("");

        JsonNode lastParams = inferStatus.path("last_params");
        status.promptText = JsonNodeUtils.asNullableText(lastParams.path("prompt"));
        status.promptWidth = lastParams.path("width").asInt(0);
        status.promptHeight = lastParams.path("height").asInt(0);

        JsonNode inputStatus = data.path("input_status");
        status.inputFps = (float) inputStatus.path("fps").asDouble(0.0);

        status.startTime = JsonNodeUtils.parseTimestampMillis(data.path("start_time"));
        status.rawJson = event.event.rawJson;

        List<EventPayloads.AiStreamStatus> results = new ArrayList<>(1);
        results.add(status);
        return results;
    }

    private static String normalizeAddress(String address) {
        if (address == null || address.trim().isEmpty()) {
            return "";
        }
        return address.trim().toLowerCase(Locale.ROOT);
    }
}
