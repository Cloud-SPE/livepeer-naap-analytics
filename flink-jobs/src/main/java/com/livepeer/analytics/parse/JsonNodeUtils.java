package com.livepeer.analytics.parse;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Shared JSON helper methods for parsing optional fields with safe defaults.
 */
public final class JsonNodeUtils {
    private JsonNodeUtils() {}

    public static Long parseTimestampMillis(JsonNode node) {
        if (node == null || node.isMissingNode() || node.isNull()) {
            return null;
        }
        if (node.isNumber()) {
            return node.asLong();
        }
        if (node.isTextual()) {
            String raw = node.asText().trim();
            if (raw.isEmpty() || !raw.matches("\\d+")) {
                return null;
            }
            try {
                return Long.parseLong(raw);
            } catch (NumberFormatException ex) {
                return null;
            }
        }
        return null;
    }

    public static long parseTimestampMillisOrDefault(JsonNode node, long defaultValue) {
        Long parsed = parseTimestampMillis(node);
        return parsed == null ? defaultValue : parsed;
    }

    public static String asNullableText(JsonNode node) {
        if (node == null || node.isMissingNode() || node.isNull()) {
            return null;
        }
        String value = node.asText();
        return value == null || value.isEmpty() ? null : value;
    }

    public static Integer asNullableInt(JsonNode node) {
        if (node == null || node.isMissingNode() || node.isNull()) {
            return null;
        }
        if (node.isInt() || node.isLong() || node.isNumber()) {
            return node.asInt();
        }
        if (node.isTextual()) {
            String raw = node.asText().trim();
            if (raw.isEmpty()) {
                return null;
            }
            try {
                return Integer.parseInt(raw);
            } catch (NumberFormatException ex) {
                return null;
            }
        }
        return null;
    }

    public static Long asNullableLong(JsonNode node) {
        if (node == null || node.isMissingNode() || node.isNull()) {
            return null;
        }
        if (node.isLong() || node.isInt() || node.isNumber()) {
            return node.asLong();
        }
        if (node.isTextual()) {
            String raw = node.asText().trim();
            if (raw.isEmpty()) {
                return null;
            }
            try {
                return Long.parseLong(raw);
            } catch (NumberFormatException ex) {
                return null;
            }
        }
        return null;
    }

    public static int asIntOrDefault(JsonNode node, int defaultValue) {
        Integer value = asNullableInt(node);
        return value == null ? defaultValue : value;
    }

    public static long asLongOrDefault(JsonNode node, long defaultValue) {
        Long value = asNullableLong(node);
        return value == null ? defaultValue : value;
    }
}
