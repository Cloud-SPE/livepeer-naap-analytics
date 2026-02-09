package com.livepeer.analytics.sink;

/**
 * Converts a payload into a single ClickHouse CSV row.
 */
@FunctionalInterface
public interface RowMapper<T> extends java.io.Serializable {
    String map(T payload);
}
