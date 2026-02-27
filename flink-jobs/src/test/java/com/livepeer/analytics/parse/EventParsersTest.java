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
        String rawJson = readResource("net_caps.json");
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
        String rawJson = readResource("net_caps.json");
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
        String rawJson = readResource("net_caps.json");
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
        String rawJson = readResource("discovery_results.json");
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
        String rawJson = readResource("ai_stream_status.json");
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

    @Test
    void networkCapabilitiesParsersHandleProdParseFailureFixture() throws Exception {
        String rawJson = readResource("net_caps_prod_parse_failed_20260224.json");
        JsonNode root = JsonSupport.MAPPER.readTree(rawJson);
        ValidatedEvent event = buildValidatedEvent(root, rawJson);

        List<EventPayloads.NetworkCapability> snapshots = EventParsers.parseNetworkCapabilities(event);
        List<EventPayloads.NetworkCapabilityAdvertised> advertised = EventParsers.parseNetworkCapabilitiesAdvertised(event);
        List<EventPayloads.NetworkCapabilityModelConstraint> constraints = EventParsers.parseNetworkCapabilitiesModelConstraints(event);
        List<EventPayloads.NetworkCapabilityPrice> prices = EventParsers.parseNetworkCapabilitiesPrices(event);

        assertFalse(snapshots.isEmpty());
        assertFalse(advertised.isEmpty());
        assertFalse(constraints.isEmpty());
        assertFalse(prices.isEmpty());
        assertEquals("a0b048eb-cb94-4639-8f9f-f283a22a154d", event.event.eventId);
    }

    @Test
    void networkCapabilitiesParsersHandleSingleObjectData() throws Exception {
        String rawJson = "{\"id\":\"evt-obj-1\",\"type\":\"network_capabilities\",\"timestamp\":1710000000000,\"data\":{\"address\":\"0xabc\",\"local_address\":\"0xabc\",\"orch_uri\":\"https://orch.example\",\"capabilities\":{\"version\":\"0.8.9\",\"capacities\":{\"35\":1},\"constraints\":{\"PerCapability\":{\"35\":{\"models\":{\"streamdiffusion-sdxl\":{\"warm\":true,\"capacity\":1,\"runnerVersion\":\"0.14.1\"}}}}}},\"capabilities_prices\":[{\"pricePerUnit\":2750,\"pixelsPerUnit\":1,\"capability\":35,\"constraint\":\"streamdiffusion-sdxl\"}],\"hardware\":[{\"pipeline\":\"live-video-to-video\",\"model_id\":\"streamdiffusion-sdxl\",\"gpu_info\":{\"0\":{\"id\":\"GPU-1\",\"name\":\"NVIDIA GeForce RTX 4090\",\"major\":8,\"minor\":9,\"memory_free\":1,\"memory_total\":2}}}]}}";
        JsonNode root = JsonSupport.MAPPER.readTree(rawJson);
        ValidatedEvent event = buildValidatedEvent(root, rawJson);

        List<EventPayloads.NetworkCapability> snapshots = EventParsers.parseNetworkCapabilities(event);
        List<EventPayloads.NetworkCapabilityAdvertised> advertised = EventParsers.parseNetworkCapabilitiesAdvertised(event);
        List<EventPayloads.NetworkCapabilityModelConstraint> constraints = EventParsers.parseNetworkCapabilitiesModelConstraints(event);
        List<EventPayloads.NetworkCapabilityPrice> prices = EventParsers.parseNetworkCapabilitiesPrices(event);

        assertFalse(snapshots.isEmpty());
        assertFalse(advertised.isEmpty());
        assertFalse(constraints.isEmpty());
        assertFalse(prices.isEmpty());
    }

    @Test
    void networkCapabilitiesParsersHandleEmptyArrayData() throws Exception {
        String rawJson = "{\"id\":\"evt-empty-1\",\"type\":\"network_capabilities\",\"timestamp\":1710000000000,\"data\":[]}";
        JsonNode root = JsonSupport.MAPPER.readTree(rawJson);
        ValidatedEvent event = buildValidatedEvent(root, rawJson);

        assertTrue(EventParsers.parseNetworkCapabilities(event).isEmpty());
        assertTrue(EventParsers.parseNetworkCapabilitiesAdvertised(event).isEmpty());
        assertTrue(EventParsers.parseNetworkCapabilitiesModelConstraints(event).isEmpty());
        assertTrue(EventParsers.parseNetworkCapabilitiesPrices(event).isEmpty());
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

    private static String readResource(String resourceName) throws Exception {
        return new String(
                EventParsersTest.class.getClassLoader().getResourceAsStream(resourceName).readAllBytes(),
                java.nio.charset.StandardCharsets.UTF_8
        );
    }

}
