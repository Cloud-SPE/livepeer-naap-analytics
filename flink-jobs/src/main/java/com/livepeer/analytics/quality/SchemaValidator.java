package com.livepeer.analytics.quality;

import com.fasterxml.jackson.databind.JsonNode;

import com.livepeer.analytics.parse.JsonNodeUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Lightweight schema validator that enforces required fields per event type and supported versions.
 */
public class SchemaValidator {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(SchemaValidator.class);
    private final Set<String> supportedVersions;
    private final Map<String, List<String>> requiredFieldsByType;

    public SchemaValidator(Set<String> supportedVersions) {
        this.supportedVersions = supportedVersions;
        this.requiredFieldsByType = buildRequiredFields();
    }

    public ValidationResult validate(JsonNode root) {
        if (root == null || !root.isObject()) {
            LOG.debug("Schema validation failed: root is null or not an object");
            return ValidationResult.invalid("SCHEMA_INVALID", "root_not_object", "Root node is missing or not an object");
        }

        JsonNode typeNode = root.get("type");
        if (typeNode == null || !typeNode.isTextual()) {
            LOG.debug("Schema validation failed: missing or invalid type");
            return ValidationResult.invalid("SCHEMA_INVALID", "missing_type", "Event type is missing or not a string");
        }

        JsonNode timestampNode = root.get("timestamp");
        if (timestampNode == null || timestampNode.isMissingNode()) {
            LOG.debug("Schema validation failed: missing timestamp");
            return ValidationResult.invalid("SCHEMA_INVALID", "missing_timestamp", "Event timestamp is missing or not numeric");
        }
        // Accept numeric or numeric string timestamps to avoid rejecting producers that serialize numbers as strings.
        Long tsValue = JsonNodeUtils.parseTimestampMillis(timestampNode);
        if (tsValue == null) {
            LOG.debug("Schema validation failed: timestamp is not numeric");
            return ValidationResult.invalid("SCHEMA_INVALID", "missing_timestamp", "Event timestamp is missing or not numeric");
        }

        JsonNode dataNode = root.get("data");
        if (dataNode == null || dataNode.isMissingNode()) {
            LOG.debug("Schema validation failed: missing data payload for type {}", typeNode.asText());
            return ValidationResult.invalid("SCHEMA_INVALID", "missing_data", "Event data payload is missing");
        }

        String eventType = typeNode.asText();
        if (!requiredFieldsByType.containsKey(eventType)) {
            LOG.debug("Schema validation failed: unsupported event type {}", eventType);
            return ValidationResult.invalid("SCHEMA_INVALID", "unsupported_event_type", "Unsupported event type: " + eventType);
        }

        String eventVersion = extractVersion(root);
        if (eventVersion != null && !supportedVersions.isEmpty() && !supportedVersions.contains(eventVersion)) {
            LOG.debug("Schema validation failed: unsupported version {} for type {}", eventVersion, eventType);
            return ValidationResult.invalid("UNSUPPORTED_VERSION", "unsupported_version", "Unsupported event version: " + eventVersion);
        }

        for (String requiredPath : requiredFieldsByType.getOrDefault(eventType, Collections.emptyList())) {
            JsonNode node = resolvePath(root, requiredPath);
            if (node == null || node.isMissingNode() || node.isNull() || (node.isTextual() && node.asText().isEmpty())) {
                LOG.debug("Schema validation failed: missing required field {}", requiredPath);
                return ValidationResult.invalid("SCHEMA_INVALID", "missing_field", "Missing required field: " + requiredPath);
            }
        }

        if ("discovery_results".equals(eventType)) {
            JsonNode dataArray = root.get("data");
            if (!dataArray.isArray() || dataArray.size() == 0) {
                LOG.debug("Schema validation failed: empty data array for type {}", eventType);
                return ValidationResult.invalid("SCHEMA_INVALID", "empty_array", "Expected non-empty data array");
            }
        }

        if ("network_capabilities".equals(eventType)) {
            JsonNode data = root.get("data");
            // Producers emit either a single orchestrator object or an array of objects.
            if (!(data.isArray() || data.isObject())) {
                LOG.debug("Schema validation failed: invalid data shape for type {}", eventType);
                return ValidationResult.invalid("SCHEMA_INVALID", "invalid_data_shape", "Expected data object or array");
            }
        }

        return ValidationResult.valid(eventVersion);
    }

    private static Map<String, List<String>> buildRequiredFields() {
        Map<String, List<String>> map = new HashMap<>();
        map.put("ai_stream_status", Arrays.asList(
                "data.stream_id",
                "data.request_id",
                "data.pipeline"
        ));
        map.put("stream_ingest_metrics", Arrays.asList(
                "data.stream_id",
                "data.request_id"
        ));
        map.put("stream_trace", Arrays.asList(
                "id",  //guid at root level
                "data.stream_id",
                "data.request_id",
                "data.type"
        ));
        map.put("network_capabilities", Collections.singletonList("data"));
        map.put("ai_stream_events", Arrays.asList(
                "data.stream_id",
                "data.request_id",
                "data.pipeline",
                "data.type"
        ));
        map.put("discovery_results", Collections.singletonList("data"));
        map.put("create_new_payment", Arrays.asList(
                "data.sessionID",
                "data.manifestID",
                "data.sender",
                "data.recipient",
                "data.orchestrator",
                "data.faceValue",
                "data.numTickets",
                "data.price",
                "data.winProb"
        ));
        return map;
    }

    private static JsonNode resolvePath(JsonNode root, String dotPath) {
        JsonNode current = root;
        String[] parts = dotPath.split("\\.");
        for (String part : parts) {
            if (current == null) {
                return null;
            }
            current = current.get(part);
        }
        return current;
    }

    private static String extractVersion(JsonNode root) {
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

    public static class ValidationResult {
        public final boolean valid;
        public final String failureClass;
        public final String reason;
        public final String details;
        public final String eventVersion;

        private ValidationResult(boolean valid, String failureClass, String reason, String details, String eventVersion) {
            this.valid = valid;
            this.failureClass = failureClass;
            this.reason = reason;
            this.details = details;
            this.eventVersion = eventVersion;
        }

        public static ValidationResult valid(String eventVersion) {
            return new ValidationResult(true, null, null, null, eventVersion == null ? "1" : eventVersion);
        }

        public static ValidationResult invalid(String failureClass, String reason, String details) {
            return new ValidationResult(false, failureClass, reason, details, null);
        }
    }
}
