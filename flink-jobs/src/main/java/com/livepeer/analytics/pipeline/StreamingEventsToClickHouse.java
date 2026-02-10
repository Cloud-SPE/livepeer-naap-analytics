package com.livepeer.analytics.pipeline;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.OutputTag;

import com.livepeer.analytics.model.EventPayloads;
import com.livepeer.analytics.model.KafkaInboundRecord;
import com.livepeer.analytics.model.ParsedEvent;
import com.livepeer.analytics.model.RejectedEventEnvelope;
import com.livepeer.analytics.parse.EventParsers;
import com.livepeer.analytics.quality.DeduplicationProcessFunction;
import com.livepeer.analytics.quality.QualityGateConfig;
import com.livepeer.analytics.quality.QualityGateProcessFunction;
import com.livepeer.analytics.quality.RejectedEventEnvelopeFactory;
import com.livepeer.analytics.quality.ValidatedEvent;
import com.livepeer.analytics.sink.ClickHouseRowMappers;
import com.livepeer.analytics.sink.ClickHouseSinkFactory;
import com.livepeer.analytics.sink.EnvelopeRowGuardProcessFunction;
import com.livepeer.analytics.sink.ParsedEventRowGuardProcessFunction;
import com.livepeer.analytics.sink.RowMapper;
import com.livepeer.analytics.util.JsonSupport;

import java.util.List;

/**
 * Main Flink pipeline:
 * - Ingest Kafka records
 * - Validate schema + build dedup key
 * - Deduplicate with state TTL
 * - Parse typed events (fan-out for array payloads)
 * - Guard against oversized sink rows
 * - Sink to ClickHouse and DLQ/quarantine topics
 */
public class StreamingEventsToClickHouse {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(StreamingEventsToClickHouse.class);

    private static final OutputTag<RejectedEventEnvelope> DLQ_TAG = new OutputTag<RejectedEventEnvelope>("dlq"){};
    private static final OutputTag<RejectedEventEnvelope> QUARANTINE_TAG = new OutputTag<RejectedEventEnvelope>("quarantine"){};

    // ============================================
    // MAIN JOB
    // ============================================

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60000);

        QualityGateConfig config = QualityGateConfig.fromEnv();
        LOG.info("Starting quality gate with inputTopic={}, dlqTopic={}, quarantineTopic={}",
                config.inputTopic, config.dlqTopic, config.quarantineTopic);

        KafkaSource<KafkaInboundRecord> kafkaSource = KafkaSource.<KafkaInboundRecord>builder()
                .setBootstrapServers(config.kafkaBootstrap)
                .setTopics(config.inputTopic)
                .setGroupId(config.kafkaGroupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new KafkaInboundRecordDeserializationSchema())
                .build();

        // Quality gate: deserialize → validate → build dedup key, then forward or emit DLQ.
        // JSON is parsed once here and reused downstream via ValidatedEvent.
        SingleOutputStreamOperator<ValidatedEvent> validatedStream = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .process(new QualityGateProcessFunction(config, DLQ_TAG))
                .returns(ValidatedEvent.class)
                .name("Quality Gate");

        // Deduplicate with TTL-backed state; duplicates go to quarantine.
        SingleOutputStreamOperator<ValidatedEvent> dedupedStream = validatedStream
                .keyBy(event -> event.event.dedupKey)
                .process(new DeduplicationProcessFunction(config, QUARANTINE_TAG))
                .returns(ValidatedEvent.class)
                .name("Dedup");

        DataStream<RejectedEventEnvelope> qualityDlqStream = validatedStream.getSideOutput(DLQ_TAG);
        DataStream<RejectedEventEnvelope> quarantineStream = dedupedStream.getSideOutput(QUARANTINE_TAG);

        SingleOutputStreamOperator<ParsedEvent<EventPayloads.AiStreamStatus>> aiStatusStream = parseEvents(
                dedupedStream,
                "ai_stream_status",
                EventParsers::parseAiStreamStatus,
                DLQ_TAG,
                new TypeHint<ParsedEvent<EventPayloads.AiStreamStatus>>() {},
                "Parse: AI Status");

        SingleOutputStreamOperator<ParsedEvent<EventPayloads.StreamIngestMetrics>> ingestMetricsStream = parseEvents(
                dedupedStream,
                "stream_ingest_metrics",
                EventParsers::parseStreamIngestMetrics,
                DLQ_TAG,
                new TypeHint<ParsedEvent<EventPayloads.StreamIngestMetrics>>() {},
                "Parse: Ingest Metrics");

        SingleOutputStreamOperator<ParsedEvent<EventPayloads.StreamTraceEvent>> traceStream = parseEvents(
                dedupedStream,
                "stream_trace",
                EventParsers::parseStreamTrace,
                DLQ_TAG,
                new TypeHint<ParsedEvent<EventPayloads.StreamTraceEvent>>() {},
                "Parse: Trace Events");

        SingleOutputStreamOperator<ParsedEvent<EventPayloads.NetworkCapability>> networkCapStream = parseEvents(
                dedupedStream,
                "network_capabilities",
                EventParsers::parseNetworkCapabilities,
                DLQ_TAG,
                new TypeHint<ParsedEvent<EventPayloads.NetworkCapability>>() {},
                "Parse: Network Caps");

        SingleOutputStreamOperator<ParsedEvent<EventPayloads.AiStreamEvent>> aiEventsStream = parseEvents(
                dedupedStream,
                "ai_stream_events",
                EventParsers::parseAiStreamEvent,
                DLQ_TAG,
                new TypeHint<ParsedEvent<EventPayloads.AiStreamEvent>>() {},
                "Parse: AI Events");

        SingleOutputStreamOperator<ParsedEvent<EventPayloads.DiscoveryResult>> discoveryStream = parseEvents(
                dedupedStream,
                "discovery_results",
                EventParsers::parseDiscoveryResults,
                DLQ_TAG,
                new TypeHint<ParsedEvent<EventPayloads.DiscoveryResult>>() {},
                "Parse: Discovery");

        SingleOutputStreamOperator<ParsedEvent<EventPayloads.PaymentEvent>> paymentStream = parseEvents(
                dedupedStream,
                "create_new_payment",
                EventParsers::parsePaymentEvent,
                DLQ_TAG,
                new TypeHint<ParsedEvent<EventPayloads.PaymentEvent>>() {},
                "Parse: Payments");

        DataStream<RejectedEventEnvelope> parseDlqStream = aiStatusStream.getSideOutput(DLQ_TAG)
                .union(ingestMetricsStream.getSideOutput(DLQ_TAG))
                .union(traceStream.getSideOutput(DLQ_TAG))
                .union(networkCapStream.getSideOutput(DLQ_TAG))
                .union(aiEventsStream.getSideOutput(DLQ_TAG))
                .union(discoveryStream.getSideOutput(DLQ_TAG))
                .union(paymentStream.getSideOutput(DLQ_TAG));

        SingleOutputStreamOperator<String> aiStatusRows = mapRowsWithGuard(
                aiStatusStream,
                ClickHouseRowMappers::aiStreamStatusRow,
                DLQ_TAG,
                config,
                "Rows: AI Status",
                true);

        SingleOutputStreamOperator<String> ingestMetricsRows = mapRowsWithGuard(
                ingestMetricsStream,
                ClickHouseRowMappers::streamIngestMetricsRow,
                DLQ_TAG,
                config,
                "Rows: Ingest Metrics",
                true);

        SingleOutputStreamOperator<String> traceRows = mapRowsWithGuard(
                traceStream,
                ClickHouseRowMappers::streamTraceRow,
                DLQ_TAG,
                config,
                "Rows: Trace Events",
                true);

        SingleOutputStreamOperator<String> networkCapsRows = mapRowsWithGuard(
                networkCapStream,
                ClickHouseRowMappers::networkCapabilitiesRow,
                DLQ_TAG,
                config,
                "Rows: Network Caps",
                true);

        SingleOutputStreamOperator<String> aiEventsRows = mapRowsWithGuard(
                aiEventsStream,
                ClickHouseRowMappers::aiStreamEventsRow,
                DLQ_TAG,
                config,
                "Rows: AI Events",
                true);

        SingleOutputStreamOperator<String> discoveryRows = mapRowsWithGuard(
                discoveryStream,
                ClickHouseRowMappers::discoveryResultsRow,
                DLQ_TAG,
                config,
                "Rows: Discovery",
                true);

        SingleOutputStreamOperator<String> paymentRows = mapRowsWithGuard(
                paymentStream,
                ClickHouseRowMappers::paymentEventsRow,
                DLQ_TAG,
                config,
                "Rows: Payments",
                true);

        DataStream<RejectedEventEnvelope> guardDlqStream = aiStatusRows.getSideOutput(DLQ_TAG)
                .union(ingestMetricsRows.getSideOutput(DLQ_TAG))
                .union(traceRows.getSideOutput(DLQ_TAG))
                .union(networkCapsRows.getSideOutput(DLQ_TAG))
                .union(aiEventsRows.getSideOutput(DLQ_TAG))
                .union(discoveryRows.getSideOutput(DLQ_TAG))
                .union(paymentRows.getSideOutput(DLQ_TAG));

        // ClickHouse connector handles retry/backoff; sink failures surface as job failures, not per-record DLQ.
        sinkToClickHouse(aiStatusRows, config, "ai_stream_status", "CH: AI Status");
        sinkToClickHouse(ingestMetricsRows, config, "stream_ingest_metrics", "CH: Ingest Metrics");
        sinkToClickHouse(traceRows, config, "stream_trace_events", "CH: Trace Events");
        sinkToClickHouse(networkCapsRows, config, "network_capabilities", "CH: Network Caps");
        sinkToClickHouse(aiEventsRows, config, "ai_stream_events", "CH: AI Events");
        sinkToClickHouse(discoveryRows, config, "discovery_results", "CH: Discovery");
        sinkToClickHouse(paymentRows, config, "payment_events", "CH: Payments");

        // Centralized DLQ stream to feed Kafka and ClickHouse audit sinks.
        DataStream<RejectedEventEnvelope> dlqStream = qualityDlqStream.union(parseDlqStream).union(guardDlqStream);

        KafkaSink<String> dlqKafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(config.kafkaBootstrap)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(config.dlqTopic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();

        KafkaSink<String> quarantineKafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(config.kafkaBootstrap)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(config.quarantineTopic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();

        dlqStream.map(JsonSupport::toJson).sinkTo(dlqKafkaSink).name("Kafka: DLQ");
        quarantineStream.map(JsonSupport::toJson).sinkTo(quarantineKafkaSink).name("Kafka: Quarantine");

        SingleOutputStreamOperator<String> dlqRows = mapEnvelopeRowsWithGuard(
                dlqStream,
                ClickHouseRowMappers::dlqRow,
                config,
                "Rows: DLQ");

        SingleOutputStreamOperator<String> quarantineRows = mapEnvelopeRowsWithGuard(
                quarantineStream,
                ClickHouseRowMappers::dlqRow,
                config,
                "Rows: Quarantine");

        sinkToClickHouse(dlqRows, config, "streaming_events_dlq", "CH: DLQ");
        sinkToClickHouse(quarantineRows, config, "streaming_events_quarantine", "CH: Quarantine");

        env.execute("Livepeer Analytics Quality Gate");
    }

    private static void sinkToClickHouse(DataStream<String> rows, QualityGateConfig config, String table, String name) {
        Sink<String> sink = ClickHouseSinkFactory.build(config, table);
        LOG.info("Configuring ClickHouse sink (table={}, url={}, database={})",
                table, config.clickhouseUrl, config.clickhouseDatabase);
        rows.sinkTo(sink).name(name);
    }

    private static <T> SingleOutputStreamOperator<ParsedEvent<T>> parseEvents(
            SingleOutputStreamOperator<ValidatedEvent> input,
            String eventType,
            EventParser<T> parser,
            OutputTag<RejectedEventEnvelope> dlqTag,
            TypeHint<ParsedEvent<T>> typeHint,
            String name) {
        return input
                .filter(event -> event != null && event.event != null && eventType.equals(event.event.eventType))
                .process(new SafeParser<>(parser, dlqTag))
                .returns(typeHint)
                .name(name);
    }

    @FunctionalInterface
    interface EventParser<T> extends java.io.Serializable {
        List<T> parse(ValidatedEvent event) throws Exception;
    }

    private static <T> SingleOutputStreamOperator<String> mapRowsWithGuard(
            SingleOutputStreamOperator<ParsedEvent<T>> input,
            RowMapper<T> mapper,
            OutputTag<RejectedEventEnvelope> dlqTag,
            QualityGateConfig config,
            String name,
            boolean emitDlqOnGuardFailure) {
        return input.process(new ParsedEventRowGuardProcessFunction<>(
                mapper,
                dlqTag,
                config.clickhouseSinkMaxRecordSizeBytes,
                emitDlqOnGuardFailure)).returns(String.class).name(name);
    }

    private static SingleOutputStreamOperator<String> mapEnvelopeRowsWithGuard(
            DataStream<RejectedEventEnvelope> input,
            RowMapper<RejectedEventEnvelope> mapper,
            QualityGateConfig config,
            String name) {
        return input.process(new EnvelopeRowGuardProcessFunction(
                mapper,
                config.clickhouseSinkMaxRecordSizeBytes)).returns(String.class).name(name);
    }

    static class SafeParser<T> extends ProcessFunction<ValidatedEvent, ParsedEvent<T>> {
        private static final long serialVersionUID = 1L;
        private final EventParser<T> parser;
        private final OutputTag<RejectedEventEnvelope> dlqTag;

        SafeParser(EventParser<T> parser, OutputTag<RejectedEventEnvelope> dlqTag) {
            this.parser = parser;
            this.dlqTag = dlqTag;
        }

        @Override
        public void processElement(ValidatedEvent event, Context ctx, org.apache.flink.util.Collector<ParsedEvent<T>> out) {
            if (event == null || event.event == null) {
                return;
            }
            try {
                List<T> results = parser.parse(event);
                if (results != null) {
                    for (T result : results) {
                        out.collect(new ParsedEvent<>(event.event, result));
                    }
                }
            } catch (Exception ex) {
                LOG.debug("Parse failure (type={}, id={}): {}", event.event.eventType, event.event.eventId, ex.getMessage());
                ctx.output(dlqTag, RejectedEventEnvelopeFactory.forParseFailure(event.event, ex));
            }
        }
    }
}
