package com.livepeer.analytics.sink;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.clickhouse.convertor.ClickHouseConvertor;
import org.apache.flink.connector.clickhouse.data.ClickHousePayload;
import org.apache.flink.connector.clickhouse.sink.ClickHouseAsyncSink;
import org.apache.flink.connector.clickhouse.sink.ClickHouseClientConfig;

import com.clickhouse.data.ClickHouseFormat;
import com.livepeer.analytics.quality.QualityGateConfig;

/**
 * Builds ClickHouse AsyncSink instances using the Flink ClickHouse connector.
 */
public final class ClickHouseSinkFactory {
    private ClickHouseSinkFactory() {}

    public static Sink<String> build(QualityGateConfig config, String table) {
        // Client config
        ClickHouseClientConfig clientConfig = new ClickHouseClientConfig(
                config.clickhouseUrl,
                config.clickhouseUser,
                config.clickhousePassword,
                config.clickhouseDatabase,
                table
        );

        // Converter: raw CSV String -> ClickHousePayload
        ElementConverter<String, ClickHousePayload> converter = new ClickHouseConvertor<>(String.class);

        // Async sink
        ClickHouseAsyncSink<String> sink = new ClickHouseAsyncSink<>(
                converter,
                config.clickhouseSinkMaxBatchSize,
                config.clickhouseSinkMaxInFlightRequests,
                config.clickhouseSinkMaxBufferedRequests,
                config.clickhouseSinkMaxBatchSizeBytes,
                config.clickhouseSinkMaxTimeInBufferMs,
                config.clickhouseSinkMaxRecordSizeBytes,
                clientConfig
        );

        // Format
        sink.setClickHouseFormat(ClickHouseFormat.JSONEachRow);

        return sink;
    }
}
