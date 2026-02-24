package com.livepeer.analytics.quality;

import com.fasterxml.jackson.databind.JsonNode;

import com.livepeer.analytics.model.KafkaInboundRecord;
import com.livepeer.analytics.model.RejectedEventEnvelope;
import com.livepeer.analytics.model.StreamingEvent;
import com.livepeer.analytics.parse.JsonNodeUtils;
import com.livepeer.analytics.util.JsonSupport;

import java.util.Base64;

/**
 * Builds DLQ/quarantine envelopes for different failure modes in the pipeline.
 */
public final class RejectedEventEnvelopeFactory {
    private RejectedEventEnvelopeFactory() {}

    public static RejectedEventEnvelope forRawRecord(
            KafkaInboundRecord record,
            String failureClass,
            String stage,
            String reason,
            String details,
            String rawJson,
            String canonicalJson,
            byte[] rawBytes) {
        RejectedEventEnvelope envelope = baseEnvelope(record, null, null, null, rawJson, canonicalJson, rawBytes, false);
        envelope.failure = buildFailure(stage, failureClass, reason, details);
        return envelope;
    }

    public static RejectedEventEnvelope forSchemaValidationFailure(
            KafkaInboundRecord record,
            JsonNode root,
            String failureClass,
            String stage,
            String reason,
            String details,
            String rawJson) {
        String canonical = null;
        try {
            canonical = JsonSupport.CANONICAL_MAPPER.writeValueAsString(root);
        } catch (Exception ignored) {
            // best-effort canonical JSON
        }
        String eventId = root.path("id").asText("");
        String eventType = root.path("type").asText("");
        String eventVersion = extractEventVersion(root);
        Long eventTimestamp = JsonNodeUtils.parseTimestampMillis(root.path("timestamp"));
        boolean replay = root.path("__replay").asBoolean(false);

        RejectedEventEnvelope envelope = baseEnvelope(record, eventId, eventType, eventVersion, rawJson, canonical, null, replay);
        envelope.dimensions = RejectedEventEnvelopeSupport.extractDimensions(root);
        envelope.eventTimestamp = eventTimestamp;
        envelope.failure = buildFailure(stage, failureClass, reason, details);
        return envelope;
    }

    public static RejectedEventEnvelope forParseFailure(StreamingEvent event, Exception ex) {
        RejectedEventEnvelope envelope = baseEnvelopeFromEvent(event);
        String details = ex == null ? "Unknown parse error"
                : (ex.getMessage() == null || ex.getMessage().isBlank()
                ? ex.getClass().getName()
                : ex.getClass().getName() + ": " + ex.getMessage());
        envelope.failure = buildFailure("PARSE", "PARSING_FAILED", "parse_exception", details);
        return envelope;
    }

    public static RejectedEventEnvelope forSinkFailure(StreamingEvent event, String table, Exception ex) {
        RejectedEventEnvelope envelope = baseEnvelopeFromEvent(event);
        envelope.failure = buildFailure("CLICKHOUSE_SINK", "DLQ_SINK_WRITE", "sink_write_failed", "Table " + table + ": " + ex.getMessage());
        return envelope;
    }

    public static RejectedEventEnvelope forSinkGuardFailure(StreamingEvent event, String reason, String details) {
        RejectedEventEnvelope envelope = baseEnvelopeFromEvent(event);
        envelope.failure = buildFailure("SINK_GUARD", "RECORD_TOO_LARGE", reason, details);
        return envelope;
    }

    public static RejectedEventEnvelope forDedupFailure(StreamingEvent event, String reason) {
        RejectedEventEnvelope envelope = baseEnvelopeFromEvent(event);
        envelope.failure = buildFailure("DEDUP", "DUPLICATE", reason, "Duplicate detected for key: " + event.dedupKey);
        return envelope;
    }

    private static RejectedEventEnvelope baseEnvelopeFromEvent(StreamingEvent event) {
        RejectedEventEnvelope envelope = new RejectedEventEnvelope();
        envelope.schemaVersion = "v1";
        envelope.source = event.source;
        envelope.identity = new RejectedEventEnvelope.Identity();
        envelope.identity.eventId = event.eventId;
        envelope.identity.eventType = event.eventType;
        envelope.identity.eventVersion = event.eventVersion;
        envelope.dimensions = event.dimensions;
        envelope.payload = new RejectedEventEnvelope.Payload();
        envelope.payload.encoding = "json";
        envelope.payload.body = event.rawJson;
        envelope.payload.canonicalJson = null;
        envelope.dedupKey = event.dedupKey;
        envelope.dedupStrategy = event.dedupStrategy;
        envelope.replay = event.replay;
        envelope.eventTimestamp = event.timestamp;
        envelope.ingestionTimestamp = System.currentTimeMillis();
        return envelope;
    }

    private static RejectedEventEnvelope baseEnvelope(
            KafkaInboundRecord record,
            String eventId,
            String eventType,
            String eventVersion,
            String rawJson,
            String canonicalJson,
            byte[] rawBytes,
            boolean replay) {
        RejectedEventEnvelope envelope = new RejectedEventEnvelope();
        envelope.schemaVersion = "v1";
        envelope.source = RejectedEventEnvelopeSupport.buildSourcePointer(record);
        envelope.identity = new RejectedEventEnvelope.Identity();
        envelope.identity.eventId = eventId;
        envelope.identity.eventType = eventType;
        envelope.identity.eventVersion = eventVersion;
        envelope.payload = new RejectedEventEnvelope.Payload();
        if (rawBytes != null) {
            envelope.payload.encoding = "base64";
            envelope.payload.body = Base64.getEncoder().encodeToString(rawBytes);
        } else {
            envelope.payload.encoding = "json";
            envelope.payload.body = rawJson;
        }
        envelope.payload.canonicalJson = canonicalJson;
        envelope.ingestionTimestamp = System.currentTimeMillis();
        envelope.replay = replay;
        return envelope;
    }

    private static RejectedEventEnvelope.FailureDetails buildFailure(String stage, String failureClass, String reason, String details) {
        RejectedEventEnvelope.FailureDetails failure = new RejectedEventEnvelope.FailureDetails();
        failure.stage = stage;
        failure.failureClass = failureClass;
        failure.reason = reason;
        failure.details = details;
        return failure;
    }

    private static String extractEventVersion(JsonNode root) {
        if (root == null) {
            return null;
        }
        JsonNode versionNode = root.get("version");
        if (versionNode == null || versionNode.isMissingNode() || versionNode.isNull()) {
            versionNode = root.path("data").get("version");
        }
        if (versionNode == null || versionNode.isMissingNode() || versionNode.isNull()) {
            return null;
        }
        if (versionNode.isTextual()) {
            return versionNode.asText();
        }
        if (versionNode.isNumber()) {
            return String.valueOf(versionNode.asInt());
        }
        return versionNode.asText(null);
    }

}
