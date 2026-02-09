package com.livepeer.analytics.sink;

import com.livepeer.analytics.util.JsonSupport;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * JSONEachRow builder for ClickHouse ingestion.
 */
final class ClickHouseJsonRow {
    private static final DateTimeFormatter TS_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneOffset.UTC);

    private final Map<String, Object> fields = new LinkedHashMap<>();

    static ClickHouseJsonRow create() {
        return new ClickHouseJsonRow();
    }

    ClickHouseJsonRow addString(String name, String value) {
        fields.put(name, value == null ? "" : value);
        return this;
    }

    ClickHouseJsonRow addNullableString(String name, String value) {
        fields.put(name, value);
        return this;
    }

    ClickHouseJsonRow addInt(String name, int value) {
        fields.put(name, value);
        return this;
    }

    ClickHouseJsonRow addNullableInt(String name, Integer value) {
        fields.put(name, value);
        return this;
    }

    ClickHouseJsonRow addLong(String name, long value) {
        fields.put(name, value);
        return this;
    }

    ClickHouseJsonRow addNullableLong(String name, Long value) {
        fields.put(name, value);
        return this;
    }

    ClickHouseJsonRow addFloat(String name, float value) {
        fields.put(name, value);
        return this;
    }

    ClickHouseJsonRow addBoolean(String name, boolean value) {
        fields.put(name, value ? 1 : 0);
        return this;
    }

    ClickHouseJsonRow addTimestampMillis(String name, long value) {
        fields.put(name, formatTimestampMillis(value));
        return this;
    }

    ClickHouseJsonRow addNullableTimestampMillis(String name, Long value) {
        fields.put(name, value == null ? null : formatTimestampMillis(value));
        return this;
    }

    String build() {
        try {
            return JsonSupport.MAPPER.writeValueAsString(fields);
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to serialize ClickHouse JSON row", ex);
        }
    }

    private static String formatTimestampMillis(long value) {
        return TS_FORMATTER.format(Instant.ofEpochMilli(value));
    }
}
