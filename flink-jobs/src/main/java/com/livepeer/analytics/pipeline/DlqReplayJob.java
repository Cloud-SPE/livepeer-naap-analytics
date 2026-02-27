package com.livepeer.analytics.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.livepeer.analytics.model.RejectedEventEnvelope;
import com.livepeer.analytics.util.JsonSupport;

import java.time.Instant;

public class DlqReplayJob {
    private static final Logger LOG = LoggerFactory.getLogger(DlqReplayJob.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String bootstrap = envOrDefault("REPLAY_KAFKA_BOOTSTRAP", envOrDefault("QUALITY_KAFKA_BOOTSTRAP", "kafka:9092"));
        String dlqTopic = envOrDefault("REPLAY_DLQ_TOPIC", envOrDefault("QUALITY_DLQ_TOPIC", "events.dlq.streaming_events.v1"));
        String outputTopic = envOrDefault("REPLAY_OUTPUT_TOPIC", envOrDefault("QUALITY_INPUT_TOPIC", "streaming_events"));
        Long startMs = envLong("REPLAY_START_EPOCH_MS");
        Long endMs = envLong("REPLAY_END_EPOCH_MS");

        LOG.info("Starting DLQ replay: dlqTopic={}, outputTopic={}, window=[{}, {}]",
                dlqTopic, outputTopic, startMs, endMs);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(dlqTopic)
                .setGroupId(envOrDefault("REPLAY_GROUP_ID", "flink-dlq-replay-v1"))
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<RejectedEventEnvelope> envelopes = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "DLQ Source")
                .flatMap((String value, org.apache.flink.util.Collector<RejectedEventEnvelope> out) -> {
                    try {
                        RejectedEventEnvelope envelope = JsonSupport.MAPPER.readValue(value, RejectedEventEnvelope.class);
                        out.collect(envelope);
                    } catch (Exception ex) {
                        LOG.warn("Failed to parse DLQ envelope: {}", ex.getMessage());
                    }
                })
                .returns(RejectedEventEnvelope.class);

        DataStream<String> replayEvents = envelopes
                .filter(envelope -> withinWindow(envelope, startMs, endMs))
                .flatMap((RejectedEventEnvelope envelope, org.apache.flink.util.Collector<String> out) -> {
                    if (envelope.payload == null || envelope.payload.body == null) {
                        LOG.debug("Skipping envelope without payload body");
                        return;
                    }
                    if (!"json".equalsIgnoreCase(envelope.payload.encoding)) {
                        LOG.debug("Skipping non-json payload encoding: {}", envelope.payload.encoding);
                        return;
                    }
                    try {
                        JsonNode node = JsonSupport.MAPPER.readTree(envelope.payload.body);
                        if (node.isObject()) {
                            ObjectNode obj = (ObjectNode) node;
                            obj.put("__replay", true);
                            obj.put("__replay_ts", Instant.now().toEpochMilli());
                            obj.put("__replay_source", "dlq");
                            if (envelope.failure != null) {
                                obj.put("__replay_failure_class", envelope.failure.failureClass);
                            }
                            out.collect(JsonSupport.MAPPER.writeValueAsString(obj));
                        }
                    } catch (Exception ex) {
                        LOG.warn("Failed to build replay payload: {}", ex.getMessage());
                    }
                })
                .returns(String.class);

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrap)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(outputTopic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();

        replayEvents.sinkTo(sink).name("Replay to Kafka");

        env.execute("Livepeer DLQ Replay");
    }

    private static boolean withinWindow(RejectedEventEnvelope envelope, Long startMs, Long endMs) {
        if (startMs == null && endMs == null) {
            return true;
        }
        long ts = envelope.eventTimestamp != null ? envelope.eventTimestamp : envelope.source.recordTimestamp;
        if (startMs != null && ts < startMs) {
            return false;
        }
        if (endMs != null && ts > endMs) {
            return false;
        }
        return true;
    }

    private static String envOrDefault(String key, String defaultValue) {
        String value = System.getenv(key);
        return value == null || value.isEmpty() ? defaultValue : value;
    }

    private static Long envLong(String key) {
        String value = System.getenv(key);
        if (value == null || value.isEmpty()) {
            return null;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException ex) {
            return null;
        }
    }
}
