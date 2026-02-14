package com.livepeer.analytics.sink;

import com.fasterxml.jackson.core.type.TypeReference;
import org.junit.jupiter.api.Test;

import com.livepeer.analytics.model.EventPayloads;
import com.livepeer.analytics.model.RejectedEventEnvelope;
import com.livepeer.analytics.util.JsonSupport;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;

class ClickHouseSchemaSyncTest {
    private static final String SCHEMA_PATH_PRIMARY = "../configs/clickhouse-init/01-schema.sql";
    private static final String SCHEMA_PATH_FALLBACK = "configs/clickhouse-init/01-schema.sql";

    @Test
    void aiStreamStatusRowMatchesSchema() throws Exception {
        EventPayloads.AiStreamStatus payload = new EventPayloads.AiStreamStatus();
        payload.eventTimestamp = 1710000000000L;
        payload.streamId = "s1";
        payload.requestId = "r1";
        payload.gateway = "gw";
        payload.orchestratorAddress = "0xabc";
        payload.orchestratorUrl = "https://orch";
        payload.pipeline = "pipe";
        payload.pipelineId = "p1";
        payload.outputFps = 30.0f;
        payload.inputFps = 30.0f;
        payload.state = "running";
        payload.restartCount = 0;
        payload.paramsHash = "h";
        payload.startTime = 1710000000000L;
        payload.rawJson = "{}";

        assertRowMatchesTable("ai_stream_status", ClickHouseRowMappers.aiStreamStatusRow(payload));
    }

    @Test
    void streamIngestMetricsRowMatchesSchema() throws Exception {
        EventPayloads.StreamIngestMetrics payload = new EventPayloads.StreamIngestMetrics();
        payload.eventTimestamp = 1710000000000L;
        payload.streamId = "s1";
        payload.requestId = "r1";
        payload.pipelineId = "p1";
        payload.connectionQuality = "good";
        payload.videoJitter = 0.1f;
        payload.videoPacketsReceived = 1;
        payload.videoPacketsLost = 0;
        payload.videoPacketLossPct = 0.0f;
        payload.videoRtt = 1.0f;
        payload.videoLastInputTs = 0.0f;
        payload.videoLatency = 0.0f;
        payload.audioJitter = 0.1f;
        payload.audioPacketsReceived = 1;
        payload.audioPacketsLost = 0;
        payload.audioPacketLossPct = 0.0f;
        payload.audioRtt = 1.0f;
        payload.audioLastInputTs = 0.0f;
        payload.audioLatency = 0.0f;
        payload.bytesReceived = 1L;
        payload.bytesSent = 1L;
        payload.rawJson = "{}";

        assertRowMatchesTable("stream_ingest_metrics", ClickHouseRowMappers.streamIngestMetricsRow(payload));
    }

    @Test
    void streamTraceRowMatchesSchema() throws Exception {
        EventPayloads.StreamTraceEvent payload = new EventPayloads.StreamTraceEvent();
        payload.eventTimestamp = 1710000000000L;
        payload.streamId = "s1";
        payload.requestId = "r1";
        payload.pipelineId = "p1";
        payload.orchestratorAddress = "0xabc";
        payload.orchestratorUrl = "https://orch";
        payload.traceType = "gateway_receive_stream_request";
        payload.dataTimestamp = 1710000000000L;
        payload.rawJson = "{}";

        assertRowMatchesTable("stream_trace_events", ClickHouseRowMappers.streamTraceRow(payload));
    }

    @Test
    void networkCapabilitiesRowMatchesSchema() throws Exception {
        EventPayloads.NetworkCapability payload = new EventPayloads.NetworkCapability();
        payload.eventTimestamp = 1710000000000L;
        payload.sourceEventId = "e1";
        payload.orchestratorAddress = "0xabc";
        payload.localAddress = "0xdef";
        payload.orchUri = "https://orch";
        payload.gpuId = "GPU-1";
        payload.gpuName = "GPU";
        payload.gpuMemoryTotal = 1L;
        payload.gpuMemoryFree = 1L;
        payload.gpuMajor = 8;
        payload.gpuMinor = 9;
        payload.pipeline = "pipe";
        payload.modelId = "model";
        payload.capabilityId = 35;
        payload.capabilityName = "Live video to video";
        payload.capabilityGroup = "live-video";
        payload.capabilityCatalogVersion = "v1";
        payload.runnerVersion = "0.1";
        payload.capacity = 1;
        payload.capacityInUse = 1;
        payload.warm = 1;
        payload.pricePerUnit = 1;
        payload.pixelsPerUnit = 1;
        payload.orchestratorVersion = "0.1";
        payload.rawJson = "{}";

        assertRowMatchesTable("network_capabilities", ClickHouseRowMappers.networkCapabilitiesRow(payload));
    }

    @Test
    void networkCapabilitiesAdvertisedRowMatchesSchema() throws Exception {
        EventPayloads.NetworkCapabilityAdvertised payload = new EventPayloads.NetworkCapabilityAdvertised();
        payload.eventTimestamp = 1710000000000L;
        payload.sourceEventId = "e1";
        payload.orchestratorAddress = "0xabc";
        payload.localAddress = "0xdef";
        payload.orchUri = "https://orch";
        payload.capabilityId = 35;
        payload.capabilityName = "Live video to video";
        payload.capabilityGroup = "live-video";
        payload.capabilityCatalogVersion = "v1";
        payload.capacity = 1;
        payload.rawJson = "{}";

        assertRowMatchesTable("network_capabilities_advertised", ClickHouseRowMappers.networkCapabilitiesAdvertisedRow(payload));
    }

    @Test
    void networkCapabilitiesModelConstraintsRowMatchesSchema() throws Exception {
        EventPayloads.NetworkCapabilityModelConstraint payload = new EventPayloads.NetworkCapabilityModelConstraint();
        payload.eventTimestamp = 1710000000000L;
        payload.sourceEventId = "e1";
        payload.orchestratorAddress = "0xabc";
        payload.localAddress = "0xdef";
        payload.orchUri = "https://orch";
        payload.capabilityId = 35;
        payload.capabilityName = "Live video to video";
        payload.capabilityGroup = "live-video";
        payload.capabilityCatalogVersion = "v1";
        payload.modelId = "streamdiffusion-sdxl";
        payload.runnerVersion = "0.14.1";
        payload.capacity = 1;
        payload.capacityInUse = 0;
        payload.warm = 1;
        payload.rawJson = "{}";

        assertRowMatchesTable("network_capabilities_model_constraints", ClickHouseRowMappers.networkCapabilitiesModelConstraintsRow(payload));
    }

    @Test
    void networkCapabilitiesPricesRowMatchesSchema() throws Exception {
        EventPayloads.NetworkCapabilityPrice payload = new EventPayloads.NetworkCapabilityPrice();
        payload.eventTimestamp = 1710000000000L;
        payload.sourceEventId = "e1";
        payload.orchestratorAddress = "0xabc";
        payload.localAddress = "0xdef";
        payload.orchUri = "https://orch";
        payload.capabilityId = 35;
        payload.capabilityName = "Live video to video";
        payload.capabilityGroup = "live-video";
        payload.capabilityCatalogVersion = "v1";
        payload.constraint = "streamdiffusion-sdxl";
        payload.pricePerUnit = 2149;
        payload.pixelsPerUnit = 1;
        payload.rawJson = "{}";

        assertRowMatchesTable("network_capabilities_prices", ClickHouseRowMappers.networkCapabilitiesPricesRow(payload));
    }

    @Test
    void aiStreamEventsRowMatchesSchema() throws Exception {
        EventPayloads.AiStreamEvent payload = new EventPayloads.AiStreamEvent();
        payload.eventTimestamp = 1710000000000L;
        payload.streamId = "s1";
        payload.requestId = "r1";
        payload.pipeline = "pipe";
        payload.pipelineId = "p1";
        payload.eventType = "error";
        payload.message = "msg";
        payload.capability = "cap";
        payload.rawJson = "{}";

        assertRowMatchesTable("ai_stream_events", ClickHouseRowMappers.aiStreamEventsRow(payload));
    }

    @Test
    void discoveryResultsRowMatchesSchema() throws Exception {
        EventPayloads.DiscoveryResult payload = new EventPayloads.DiscoveryResult();
        payload.eventTimestamp = 1710000000000L;
        payload.orchestratorAddress = "0xabc";
        payload.orchestratorUrl = "https://orch";
        payload.latencyMs = 1;
        payload.rawJson = "{}";

        assertRowMatchesTable("discovery_results", ClickHouseRowMappers.discoveryResultsRow(payload));
    }

    @Test
    void paymentEventsRowMatchesSchema() throws Exception {
        EventPayloads.PaymentEvent payload = new EventPayloads.PaymentEvent();
        payload.eventTimestamp = 1710000000000L;
        payload.requestId = "r1";
        payload.sessionId = "s1";
        payload.manifestId = "m1";
        payload.sender = "sender";
        payload.recipient = "recipient";
        payload.orchestrator = "orch";
        payload.faceValue = "1";
        payload.price = "1";
        payload.numTickets = "1";
        payload.winProb = "1";
        payload.clientIp = "127.0.0.1";
        payload.capability = "cap";
        payload.rawJson = "{}";

        assertRowMatchesTable("payment_events", ClickHouseRowMappers.paymentEventsRow(payload));
    }

    @Test
    void dlqRowMatchesSchema() throws Exception {
        RejectedEventEnvelope envelope = new RejectedEventEnvelope();
        envelope.schemaVersion = "v1";
        envelope.replay = false;
        envelope.ingestionTimestamp = 1710000000000L;
        envelope.eventTimestamp = 1710000000000L;
        envelope.source = new RejectedEventEnvelope.SourcePointer();
        envelope.source.topic = "t";
        envelope.source.partition = 0;
        envelope.source.offset = 1L;
        envelope.source.recordTimestamp = 1710000000000L;
        envelope.identity = new RejectedEventEnvelope.Identity();
        envelope.identity.eventId = "e";
        envelope.identity.eventType = "t";
        envelope.identity.eventVersion = "1";
        envelope.failure = new RejectedEventEnvelope.FailureDetails();
        envelope.failure.stage = "PARSE";
        envelope.failure.failureClass = "PARSING_FAILED";
        envelope.failure.reason = "reason";
        envelope.failure.details = "details";
        envelope.dimensions = new RejectedEventEnvelope.EventDimensions();
        envelope.dimensions.orchestrator = "orch";
        envelope.dimensions.broadcaster = "broad";
        envelope.dimensions.region = "region";
        envelope.payload = new RejectedEventEnvelope.Payload();
        envelope.payload.encoding = "json";
        envelope.payload.body = "{}";
        envelope.payload.canonicalJson = "{}";

        String row = ClickHouseRowMappers.dlqRow(envelope);
        assertRowMatchesTable("streaming_events_dlq", row);
        assertRowMatchesTable("streaming_events_quarantine", row);
    }

    private static void assertRowMatchesTable(String table, String jsonRow) throws Exception {
        Map<String, Object> row = JsonSupport.MAPPER.readValue(jsonRow, new TypeReference<Map<String, Object>>() {});
        Set<String> rowKeys = row.keySet();
        SchemaSpec schema = loadSchemaColumns().get(table);
        assertNotNull(schema, "Missing schema definition for table " + table);
        Set<String> allowed = new HashSet<>(schema.required);
        allowed.addAll(schema.optional);
        assertTrue(rowKeys.containsAll(schema.required), "Row is missing required columns for table " + table);
        assertTrue(allowed.containsAll(rowKeys), "Row contains unknown columns for table " + table);
    }

    private static Map<String, SchemaSpec> loadSchemaColumns() throws IOException {
        String sql = readSchemaSql();
        Map<String, SchemaSpec> tableColumns = new HashMap<>();
        Pattern pattern = Pattern.compile(
                "CREATE TABLE IF NOT EXISTS\\s+(\\w+)\\s*\\((.*?)\\)\\s*ENGINE",
                Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
        Matcher matcher = pattern.matcher(sql);
        while (matcher.find()) {
            String table = matcher.group(1);
            String body = matcher.group(2);
            Set<String> required = new LinkedHashSet<>();
            Set<String> optional = new LinkedHashSet<>();
            for (String line : body.split("\\r?\\n")) {
                String trimmed = stripInlineComment(line).trim();
                if (trimmed.isEmpty() || trimmed.startsWith("--")) {
                    continue;
                }
                if (trimmed.endsWith(",")) {
                    trimmed = trimmed.substring(0, trimmed.length() - 1).trim();
                }
                String[] parts = trimmed.split("\\s+");
                if (parts.length > 1) {
                    boolean isOptional = trimmed.contains("MATERIALIZED") || trimmed.contains("DEFAULT");
                    if (isOptional) {
                        optional.add(parts[0]);
                    } else {
                        required.add(parts[0]);
                    }
                }
            }
            tableColumns.put(table, new SchemaSpec(required, optional));
        }
        return tableColumns;
    }

    private static String readSchemaSql() throws IOException {
        Path primary = Path.of(SCHEMA_PATH_PRIMARY);
        if (Files.exists(primary)) {
            return Files.readString(primary);
        }
        Path fallback = Path.of(SCHEMA_PATH_FALLBACK);
        if (Files.exists(fallback)) {
            return Files.readString(fallback);
        }
        throw new IOException("ClickHouse schema SQL not found in expected paths");
    }

    private static String stripInlineComment(String line) {
        int idx = line.indexOf("--");
        return idx >= 0 ? line.substring(0, idx) : line;
    }

    private static final class SchemaSpec {
        private final Set<String> required;
        private final Set<String> optional;

        private SchemaSpec(Set<String> required, Set<String> optional) {
            this.required = required;
            this.optional = optional;
        }
    }
}
