package com.livepeer.analytics.parse;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;

import com.livepeer.analytics.model.EventPayloads;
import com.livepeer.analytics.model.StreamingEvent;
import com.livepeer.analytics.quality.ValidatedEvent;
import com.livepeer.analytics.sink.ClickHouseRowMappers;
import com.livepeer.analytics.parse.JsonNodeUtils;
import com.livepeer.analytics.util.JsonSupport;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class EventParsersTest {

    @Test
    void networkCapabilitiesEmitsAllHardwareEntries() throws Exception {
        String rawJson = new String(
            EventParsersTest.class.getClassLoader().getResourceAsStream("net_caps.json").readAllBytes(),
            java.nio.charset.StandardCharsets.UTF_8
        );
        JsonNode root = JsonSupport.MAPPER.readTree(rawJson);

        ValidatedEvent event = buildValidatedEvent(root, rawJson);
        List<EventPayloads.NetworkCapability> results = EventParsers.parseNetworkCapabilities(event);

        assertEquals(5, results.size());
        assertTrue(results.stream().anyMatch(r -> "0x9d61ae5875e89036fbf6059f3116d01a22ace3c8".equals(r.orchestratorAddress)));
        assertTrue(results.stream().anyMatch(r -> "NVIDIA GeForce RTX 4090".equals(r.gpuName)));
        assertTrue(results.stream().anyMatch(r -> Integer.valueOf(35).equals(r.capabilityId)));
        assertTrue(results.stream().anyMatch(r -> "Live video to video".equals(r.capabilityName)));
    }

    @Test
    void networkCapabilitiesEmitsAdvertisedCapabilities() throws Exception {
        String rawJson = new String(
            EventParsersTest.class.getClassLoader().getResourceAsStream("net_caps.json").readAllBytes(),
            java.nio.charset.StandardCharsets.UTF_8
        );
        JsonNode root = JsonSupport.MAPPER.readTree(rawJson);

        ValidatedEvent event = buildValidatedEvent(root, rawJson);
        List<EventPayloads.NetworkCapabilityAdvertised> results = EventParsers.parseNetworkCapabilitiesAdvertised(event);

        assertFalse(results.isEmpty());
        assertTrue(results.stream().anyMatch(r -> Integer.valueOf(35).equals(r.capabilityId)));
        assertTrue(results.stream().anyMatch(r -> "Live video to video".equals(r.capabilityName)));
        assertTrue(results.stream().anyMatch(r -> "transcoding".equals(r.capabilityGroup)));
    }

    @Test
    void networkCapabilitiesEmitsModelConstraintsAndPrices() throws Exception {
        String rawJson = new String(
            EventParsersTest.class.getClassLoader().getResourceAsStream("net_caps.json").readAllBytes(),
            java.nio.charset.StandardCharsets.UTF_8
        );
        JsonNode root = JsonSupport.MAPPER.readTree(rawJson);

        ValidatedEvent event = buildValidatedEvent(root, rawJson);
        List<EventPayloads.NetworkCapabilityModelConstraint> constraints = EventParsers.parseNetworkCapabilitiesModelConstraints(event);
        List<EventPayloads.NetworkCapabilityPrice> prices = EventParsers.parseNetworkCapabilitiesPrices(event);

        assertFalse(constraints.isEmpty());
        assertTrue(constraints.stream().anyMatch(r -> "streamdiffusion-sdxl".equals(r.modelId)));
        assertFalse(prices.isEmpty());
        assertTrue(prices.stream().anyMatch(r -> Integer.valueOf(2149).equals(r.pricePerUnit)));
    }

    @Test
    void discoveryResultsEmitsMultipleEntries() throws Exception {
        String rawJson = new String(
            EventParsersTest.class.getClassLoader().getResourceAsStream("discovery_results.json").readAllBytes(),
            java.nio.charset.StandardCharsets.UTF_8
        );
        JsonNode root = JsonSupport.MAPPER.readTree(rawJson);

        ValidatedEvent event = buildValidatedEvent(root, rawJson);
        List<EventPayloads.DiscoveryResult> results = EventParsers.parseDiscoveryResults(event);

        assertEquals(2, results.size());
        assertEquals(84, results.get(0).latencyMs);
        assertEquals(119, results.get(1).latencyMs);

        String row = ClickHouseRowMappers.discoveryResultsRow(results.get(0));
        assertTrue(row.contains("\"orchestrator_address\""));
        assertTrue(row.contains("\"latency_ms\""));
    }

    @Test
    void aiStreamStatusRowMatchesExpectedColumns() throws Exception {
        String rawJson = new String(
            EventParsersTest.class.getClassLoader().getResourceAsStream("ai_stream_status.json").readAllBytes(),
            java.nio.charset.StandardCharsets.UTF_8
        );
        JsonNode root = JsonSupport.MAPPER.readTree(rawJson);

        ValidatedEvent event = buildValidatedEvent(root, rawJson);
        List<EventPayloads.AiStreamStatus> results = EventParsers.parseAiStreamStatus(event);
        assertEquals(1, results.size());

        String row = ClickHouseRowMappers.aiStreamStatusRow(results.get(0));
        JsonNode node = JsonSupport.MAPPER.readTree(row);
        assertTrue(node.isObject());
        assertEquals(results.get(0).streamId, node.path("stream_id").asText());
        assertTrue(node.has("event_timestamp"));
    }

    private static ValidatedEvent buildValidatedEvent(JsonNode root, String rawJson) {
        StreamingEvent event = new StreamingEvent();
        event.eventId = root.path("id").asText("");
        event.eventType = root.path("type").asText("");
        event.eventVersion = "1";
        event.timestamp = JsonNodeUtils.parseTimestampMillisOrDefault(root.path("timestamp"), System.currentTimeMillis());
        event.rawJson = rawJson;
        event.replay = root.path("__replay").asBoolean(false);
        return new ValidatedEvent(event, root);
    }

}
