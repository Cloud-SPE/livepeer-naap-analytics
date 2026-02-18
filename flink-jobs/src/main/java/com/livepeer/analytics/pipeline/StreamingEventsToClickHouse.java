package com.livepeer.analytics.pipeline;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.OutputTag;

import com.livepeer.analytics.lifecycle.LifecycleSignal;
import com.livepeer.analytics.lifecycle.CapabilityBroadcastState;
import com.livepeer.analytics.lifecycle.WorkflowLifecycleCoverageAggregatorFunction;
import com.livepeer.analytics.lifecycle.WorkflowParamUpdateAggregatorFunction;
import com.livepeer.analytics.lifecycle.WorkflowSessionAggregatorFunction;
import com.livepeer.analytics.lifecycle.WorkflowSessionSegmentAggregatorFunction;
import com.livepeer.analytics.lifecycle.WorkflowLatencyDerivation;
import com.livepeer.analytics.model.EventPayloads;
import com.livepeer.analytics.model.KafkaInboundRecord;
import com.livepeer.analytics.model.ParsedEvent;
import com.livepeer.analytics.model.RejectedEventEnvelope;
import com.livepeer.analytics.model.StreamingEvent;
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
import com.livepeer.analytics.util.Hashing;
import com.livepeer.analytics.util.JsonSupport;
import com.livepeer.analytics.util.WorkflowSessionId;

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

        SingleOutputStreamOperator<ParsedEvent<EventPayloads.NetworkCapabilityAdvertised>> networkCapAdvertisedStream = parseEvents(
                dedupedStream,
                "network_capabilities",
                EventParsers::parseNetworkCapabilitiesAdvertised,
                DLQ_TAG,
                new TypeHint<ParsedEvent<EventPayloads.NetworkCapabilityAdvertised>>() {},
                "Parse: Network Caps Advertised");

        SingleOutputStreamOperator<ParsedEvent<EventPayloads.NetworkCapabilityModelConstraint>> networkCapModelConstraintStream = parseEvents(
                dedupedStream,
                "network_capabilities",
                EventParsers::parseNetworkCapabilitiesModelConstraints,
                DLQ_TAG,
                new TypeHint<ParsedEvent<EventPayloads.NetworkCapabilityModelConstraint>>() {},
                "Parse: Network Caps Model Constraints");

        SingleOutputStreamOperator<ParsedEvent<EventPayloads.NetworkCapabilityPrice>> networkCapPriceStream = parseEvents(
                dedupedStream,
                "network_capabilities",
                EventParsers::parseNetworkCapabilitiesPrices,
                DLQ_TAG,
                new TypeHint<ParsedEvent<EventPayloads.NetworkCapabilityPrice>>() {},
                "Parse: Network Caps Prices");

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

        SingleOutputStreamOperator<LifecycleSignal> statusLifecycleSignals = aiStatusStream
                .map(StreamingEventsToClickHouse::toLifecycleSignalFromStatus)
                .returns(LifecycleSignal.class)
                .name("Lifecycle Signal: AI Status");

        SingleOutputStreamOperator<LifecycleSignal> traceLifecycleSignals = traceStream
                .map(StreamingEventsToClickHouse::toLifecycleSignalFromTrace)
                .returns(LifecycleSignal.class)
                .name("Lifecycle Signal: Trace");

        SingleOutputStreamOperator<LifecycleSignal> aiEventLifecycleSignals = aiEventsStream
                .map(StreamingEventsToClickHouse::toLifecycleSignalFromAiEvent)
                .returns(LifecycleSignal.class)
                .name("Lifecycle Signal: AI Event");

        DataStream<LifecycleSignal> lifecycleSignals = statusLifecycleSignals
                .union(traceLifecycleSignals)
                .union(aiEventLifecycleSignals);

        BroadcastStream<ParsedEvent<EventPayloads.NetworkCapability>> capabilityBroadcast =
                networkCapStream.broadcast(CapabilityBroadcastState.CAPABILITY_STATE_DESCRIPTOR);

        SingleOutputStreamOperator<ParsedEvent<EventPayloads.FactWorkflowSession>> workflowSessionsStream = lifecycleSignals
                .keyBy(signal -> signal.workflowSessionId)
                .connect(capabilityBroadcast)
                .process(new WorkflowSessionAggregatorFunction())
                .returns(new TypeHint<ParsedEvent<EventPayloads.FactWorkflowSession>>() {})
                .name("Sessionize: Workflow Sessions");

        // Derive latency KPIs from deterministic session-edge timestamps emitted by Flink state.
        // This keeps edge semantics versioned in code and minimizes complex ClickHouse SQL pairing logic.
        SingleOutputStreamOperator<ParsedEvent<EventPayloads.FactWorkflowLatencySample>> workflowLatencySamplesStream = workflowSessionsStream
                .map(StreamingEventsToClickHouse::toWorkflowLatencySample)
                .returns(new TypeHint<ParsedEvent<EventPayloads.FactWorkflowLatencySample>>() {})
                .name("Derive: Workflow Latency Samples");

        SingleOutputStreamOperator<ParsedEvent<EventPayloads.FactWorkflowSessionSegment>> workflowSessionSegmentsStream = lifecycleSignals
                .keyBy(signal -> signal.workflowSessionId)
                .connect(capabilityBroadcast)
                .process(new WorkflowSessionSegmentAggregatorFunction())
                .returns(new TypeHint<ParsedEvent<EventPayloads.FactWorkflowSessionSegment>>() {})
                .name("Sessionize: Workflow Session Segments");

        SingleOutputStreamOperator<ParsedEvent<EventPayloads.FactWorkflowParamUpdate>> workflowParamUpdatesStream = lifecycleSignals
                .keyBy(signal -> signal.workflowSessionId)
                .connect(capabilityBroadcast)
                .process(new WorkflowParamUpdateAggregatorFunction())
                .returns(new TypeHint<ParsedEvent<EventPayloads.FactWorkflowParamUpdate>>() {})
                .name("Sessionize: Workflow Param Updates");

        SingleOutputStreamOperator<ParsedEvent<EventPayloads.FactLifecycleEdgeCoverage>> lifecycleCoverageStream = lifecycleSignals
                .keyBy(signal -> signal.workflowSessionId)
                .process(new WorkflowLifecycleCoverageAggregatorFunction())
                .returns(new TypeHint<ParsedEvent<EventPayloads.FactLifecycleEdgeCoverage>>() {})
                .name("Sessionize: Lifecycle Edge Coverage");

        DataStream<RejectedEventEnvelope> parseDlqStream = aiStatusStream.getSideOutput(DLQ_TAG)
                .union(ingestMetricsStream.getSideOutput(DLQ_TAG))
                .union(traceStream.getSideOutput(DLQ_TAG))
                .union(networkCapStream.getSideOutput(DLQ_TAG))
                .union(networkCapAdvertisedStream.getSideOutput(DLQ_TAG))
                .union(networkCapModelConstraintStream.getSideOutput(DLQ_TAG))
                .union(networkCapPriceStream.getSideOutput(DLQ_TAG))
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

        SingleOutputStreamOperator<String> networkCapsAdvertisedRows = mapRowsWithGuard(
                networkCapAdvertisedStream,
                ClickHouseRowMappers::networkCapabilitiesAdvertisedRow,
                DLQ_TAG,
                config,
                "Rows: Network Caps Advertised",
                true);

        SingleOutputStreamOperator<String> networkCapsModelConstraintRows = mapRowsWithGuard(
                networkCapModelConstraintStream,
                ClickHouseRowMappers::networkCapabilitiesModelConstraintsRow,
                DLQ_TAG,
                config,
                "Rows: Network Caps Model Constraints",
                true);

        SingleOutputStreamOperator<String> networkCapsPriceRows = mapRowsWithGuard(
                networkCapPriceStream,
                ClickHouseRowMappers::networkCapabilitiesPricesRow,
                DLQ_TAG,
                config,
                "Rows: Network Caps Prices",
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

        SingleOutputStreamOperator<String> workflowSessionRows = mapRowsWithGuard(
                workflowSessionsStream,
                ClickHouseRowMappers::factWorkflowSessionsRow,
                DLQ_TAG,
                config,
                "Rows: Workflow Sessions",
                true);

        SingleOutputStreamOperator<String> workflowLatencySampleRows = mapRowsWithGuard(
                workflowLatencySamplesStream,
                ClickHouseRowMappers::factWorkflowLatencySamplesRow,
                DLQ_TAG,
                config,
                "Rows: Workflow Latency Samples",
                true);

        SingleOutputStreamOperator<String> workflowSessionSegmentRows = mapRowsWithGuard(
                workflowSessionSegmentsStream,
                ClickHouseRowMappers::factWorkflowSessionSegmentsRow,
                DLQ_TAG,
                config,
                "Rows: Workflow Session Segments",
                true);

        SingleOutputStreamOperator<String> workflowParamUpdateRows = mapRowsWithGuard(
                workflowParamUpdatesStream,
                ClickHouseRowMappers::factWorkflowParamUpdatesRow,
                DLQ_TAG,
                config,
                "Rows: Workflow Param Updates",
                true);

        SingleOutputStreamOperator<String> lifecycleCoverageRows = mapRowsWithGuard(
                lifecycleCoverageStream,
                ClickHouseRowMappers::factLifecycleEdgeCoverageRow,
                DLQ_TAG,
                config,
                "Rows: Lifecycle Edge Coverage",
                true);

        DataStream<RejectedEventEnvelope> guardDlqStream = aiStatusRows.getSideOutput(DLQ_TAG)
                .union(ingestMetricsRows.getSideOutput(DLQ_TAG))
                .union(traceRows.getSideOutput(DLQ_TAG))
                .union(networkCapsRows.getSideOutput(DLQ_TAG))
                .union(networkCapsAdvertisedRows.getSideOutput(DLQ_TAG))
                .union(networkCapsModelConstraintRows.getSideOutput(DLQ_TAG))
                .union(networkCapsPriceRows.getSideOutput(DLQ_TAG))
                .union(aiEventsRows.getSideOutput(DLQ_TAG))
                .union(discoveryRows.getSideOutput(DLQ_TAG))
                .union(paymentRows.getSideOutput(DLQ_TAG))
                .union(workflowSessionRows.getSideOutput(DLQ_TAG))
                .union(workflowLatencySampleRows.getSideOutput(DLQ_TAG))
                .union(workflowSessionSegmentRows.getSideOutput(DLQ_TAG))
                .union(workflowParamUpdateRows.getSideOutput(DLQ_TAG))
                .union(lifecycleCoverageRows.getSideOutput(DLQ_TAG));

        // ClickHouse connector handles retry/backoff; sink failures surface as job failures, not per-record DLQ.
        sinkToClickHouse(aiStatusRows, config, "ai_stream_status", "CH: AI Status");
        sinkToClickHouse(ingestMetricsRows, config, "stream_ingest_metrics", "CH: Ingest Metrics");
        sinkToClickHouse(traceRows, config, "stream_trace_events", "CH: Trace Events");
        sinkToClickHouse(networkCapsRows, config, "network_capabilities", "CH: Network Caps");
        sinkToClickHouse(networkCapsAdvertisedRows, config, "network_capabilities_advertised", "CH: Network Caps Advertised");
        sinkToClickHouse(networkCapsModelConstraintRows, config, "network_capabilities_model_constraints", "CH: Network Caps Model Constraints");
        sinkToClickHouse(networkCapsPriceRows, config, "network_capabilities_prices", "CH: Network Caps Prices");
        sinkToClickHouse(aiEventsRows, config, "ai_stream_events", "CH: AI Events");
        sinkToClickHouse(discoveryRows, config, "discovery_results", "CH: Discovery");
        sinkToClickHouse(paymentRows, config, "payment_events", "CH: Payments");
        sinkToClickHouse(workflowSessionRows, config, "fact_workflow_sessions", "CH: Workflow Sessions");
        sinkToClickHouse(workflowLatencySampleRows, config, "fact_workflow_latency_samples", "CH: Workflow Latency Samples");
        sinkToClickHouse(workflowSessionSegmentRows, config, "fact_workflow_session_segments", "CH: Workflow Session Segments");
        sinkToClickHouse(workflowParamUpdateRows, config, "fact_workflow_param_updates", "CH: Workflow Param Updates");
        sinkToClickHouse(lifecycleCoverageRows, config, "fact_lifecycle_edge_coverage", "CH: Lifecycle Edge Coverage");

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

    private static LifecycleSignal toLifecycleSignalFromStatus(ParsedEvent<EventPayloads.AiStreamStatus> parsed) {
        LifecycleSignal signal = new LifecycleSignal();
        signal.signalType = LifecycleSignal.SignalType.STREAM_STATUS;
        signal.workflowSessionId = WorkflowSessionId.from(parsed.payload.streamId, parsed.payload.requestId, parsed.event.eventId);
        signal.signalTimestamp = parsed.payload.eventTimestamp;
        signal.ingestTimestamp = parsed.event.timestamp;
        signal.streamId = parsed.payload.streamId;
        signal.requestId = parsed.payload.requestId;
        signal.pipeline = parsed.payload.pipeline;
        signal.pipelineId = parsed.payload.pipelineId;
        signal.gateway = parsed.payload.gateway;
        signal.orchestratorAddress = parsed.payload.orchestratorAddress;
        signal.orchestratorUrl = parsed.payload.orchestratorUrl;
        signal.startTimeMs = parsed.payload.startTime;
        signal.sourceEventUid = sourceEventUid(parsed.event);
        signal.sourceEvent = parsed.event;
        return signal;
    }

    private static LifecycleSignal toLifecycleSignalFromTrace(ParsedEvent<EventPayloads.StreamTraceEvent> parsed) {
        LifecycleSignal signal = new LifecycleSignal();
        signal.signalType = LifecycleSignal.SignalType.STREAM_TRACE;
        signal.workflowSessionId = WorkflowSessionId.from(parsed.payload.streamId, parsed.payload.requestId, parsed.event.eventId);
        signal.signalTimestamp = parsed.payload.dataTimestamp > 0 ? parsed.payload.dataTimestamp : parsed.payload.eventTimestamp;
        signal.ingestTimestamp = parsed.event.timestamp;
        signal.streamId = parsed.payload.streamId;
        signal.requestId = parsed.payload.requestId;
        signal.pipelineId = parsed.payload.pipelineId;
        signal.orchestratorAddress = parsed.payload.orchestratorAddress;
        signal.orchestratorUrl = parsed.payload.orchestratorUrl;
        signal.traceType = parsed.payload.traceType;
        signal.sourceEventUid = sourceEventUid(parsed.event);
        signal.sourceEvent = parsed.event;
        return signal;
    }

    private static LifecycleSignal toLifecycleSignalFromAiEvent(ParsedEvent<EventPayloads.AiStreamEvent> parsed) {
        LifecycleSignal signal = new LifecycleSignal();
        signal.signalType = LifecycleSignal.SignalType.AI_STREAM_EVENT;
        signal.workflowSessionId = WorkflowSessionId.from(parsed.payload.streamId, parsed.payload.requestId, parsed.event.eventId);
        signal.signalTimestamp = parsed.payload.eventTimestamp;
        signal.ingestTimestamp = parsed.event.timestamp;
        signal.streamId = parsed.payload.streamId;
        signal.requestId = parsed.payload.requestId;
        signal.pipeline = parsed.payload.pipeline;
        signal.pipelineId = parsed.payload.pipelineId;
        signal.aiEventType = parsed.payload.eventType;
        signal.message = parsed.payload.message;
        signal.sourceEventUid = sourceEventUid(parsed.event);
        signal.sourceEvent = parsed.event;
        return signal;
    }

    private static ParsedEvent<EventPayloads.FactWorkflowLatencySample> toWorkflowLatencySample(
            ParsedEvent<EventPayloads.FactWorkflowSession> parsed) {
        EventPayloads.FactWorkflowLatencySample sample = WorkflowLatencyDerivation.fromSession(parsed.payload);
        return new ParsedEvent<>(parsed.event, sample);
    }

    private static String sourceEventUid(StreamingEvent event) {
        if (event == null) {
            return "";
        }
        if (event.eventId != null && !event.eventId.trim().isEmpty()) {
            return event.eventId;
        }
        return Hashing.sha256Hex(event.rawJson == null ? "" : event.rawJson);
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
