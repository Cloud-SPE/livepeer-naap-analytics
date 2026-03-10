package com.livepeer.analytics.sink;

/**
 * Converts a payload into a single ClickHouse JSON row.
 * {@code org} is the logical organization label derived from the source Kafka topic.
 */
@FunctionalInterface
public interface RowMapper<T> extends java.io.Serializable {
    String map(T payload, String org);
}
