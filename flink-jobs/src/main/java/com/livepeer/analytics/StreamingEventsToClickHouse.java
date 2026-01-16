package com.livepeer.analytics;

import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.core.datastream.sink.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.*;

/**
 * Flink 2.1.1 Java job for streaming Livepeer events to ClickHouse
 */
public class StreamingEventsToClickHouse {

    // ============================================
    // DATA CLASSES (replacing Scala case classes)
    // ============================================

    public static class StreamingEvent {
        public String eventId;
        public String eventType;
        public long timestamp;
        public String gateway;
        public String rawJson;

        public StreamingEvent() {}

        public StreamingEvent(String eventId, String eventType, long timestamp, String gateway, String rawJson) {
            this.eventId = eventId;
            this.eventType = eventType;
            this.timestamp = timestamp;
            this.gateway = gateway;
            this.rawJson = rawJson;
        }
    }

    public static class OrchestratorMetadata {
        public String orchestratorAddress;
        public String localAddress;
        public String orchUri;
        public String gpuId;
        public String gpuName;
        public String modelId;
        public long lastUpdated;

        public OrchestratorMetadata() {}
    }

    public static class AiStreamStatus {
        public Timestamp eventTimestamp;
        public String streamId;
        public String requestId;
        public String gateway;
        public String orchestratorAddress;
        public String orchestratorUrl;
        public String gpuId;
        public String region;
        public String pipeline;
        public String pipelineId;
        public float outputFps;
        public float inputFps;
        public String state;
        public int restartCount;
        public String lastError;
        public String promptText;
        public int promptWidth;
        public int promptHeight;
        public String paramsHash;

        public AiStreamStatus() {}
    }

    public static class StreamIngestMetrics {
        public Timestamp eventTimestamp;
        public String streamId;
        public String requestId;
        public String gateway;
        public String orchestratorAddress;
        public String pipelineId;
        public String connectionQuality;
        public float videoJitter;
        public long videoPacketsReceived;
        public long videoPacketsLost;
        public float videoPacketLossPct;
        public float audioJitter;
        public long audioPacketsReceived;
        public long audioPacketsLost;
        public float audioPacketLossPct;
        public long bytesReceived;
        public long bytesSent;

        public StreamIngestMetrics() {}
    }

    public static class StreamTraceEvent {
        public Timestamp eventTimestamp;
        public String streamId;
        public String requestId;
        public String gateway;
        public String orchestratorAddress;
        public String orchestratorUrl;
        public String pipelineId;
        public String traceType;
        public Timestamp dataTimestamp;

        public StreamTraceEvent() {}
    }

    public static class NetworkCapability {
        public Timestamp eventTimestamp;
        public String orchestratorAddress;
        public String localAddress;
        public String orchUri;
        public String gpuId;
        public String gpuName;
        public Long gpuMemoryTotal;
        public Long gpuMemoryFree;
        public Integer gpuMajor;
        public String pipeline;
        public String modelId;
        public String runnerVersion;
        public Integer capacity;
        public Integer capacityInUse;
        public Integer pricePerUnit;
        public String orchestratorVersion;
        public String rawJson;

        public NetworkCapability() {}
    }

    public static class AiStreamEvent {
        public Timestamp eventTimestamp;
        public String streamId;
        public String requestId;
        public String gateway;
        public String pipeline;
        public String pipelineId;
        public String eventType;
        public String message;
        public String capability;

        public AiStreamEvent() {}
    }

    public static class DiscoveryResult {
        public Timestamp eventTimestamp;
        public String orchestratorAddress;
        public String orchestratorUrl;
        public int latencyMs;
        public String gateway;

        public DiscoveryResult() {}
    }

    public static class PaymentEvent {
        public Timestamp eventTimestamp;
        public String requestId;
        public String sessionId;
        public String manifestId;
        public String sender;
        public String recipient;
        public String orchestrator;
        public String faceValue;
        public String price;
        public String numTickets;
        public String winProb;
        public String gateway;
        public String clientIp;
        public String capability;

        public PaymentEvent() {}
    }

    // ============================================
    // MAIN
    // ============================================

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60000);

        // Kafka Source
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("kafka:9092")
                .setTopics("streaming-events")
                .setGroupId("flink-streaming-events-consumer")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> rawStream = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // Parse JSON events
        DataStream<StreamingEvent> parsedStream = rawStream
                .map(StreamingEventsToClickHouse::parseEvent)
                .filter(Objects::nonNull);

        // ============================================
        // STEP 1: Extract & Store Network Capabilities
        // ============================================

        DataStream<NetworkCapability> networkCapabilities = parsedStream
                .filter(e -> "network_capabilities".equals(e.eventType))
                .flatMap(new org.apache.flink.api.common.functions.FlatMapFunction<StreamingEvent, NetworkCapability>() {
                    @Override
                    public void flatMap(StreamingEvent event, Collector<NetworkCapability> out) {
                        toNetworkCapabilities(event).forEach(out::collect);
                    }
                });

        networkCapabilities
                .addSink(createNetworkCapabilitiesSink())
                .name("ClickHouse: network_capabilities");

        // ============================================
        // STEP 2: Build Broadcast State
        // ============================================

        MapStateDescriptor<String, OrchestratorMetadata> metadataStateDescriptor =
                new MapStateDescriptor<>(
                        "orchestrator-metadata",
                        TypeInformation.of(String.class),
                        TypeInformation.of(OrchestratorMetadata.class)
                );

        DataStream<OrchestratorMetadata> orchestratorMetadataStream = networkCapabilities
                .map(StreamingEventsToClickHouse::toOrchestratorMetadata);

        BroadcastStream<OrchestratorMetadata> broadcastStream =
                orchestratorMetadataStream.broadcast(metadataStateDescriptor);

        // ============================================
        // STEP 3: Enrich AI Stream Status
        // ============================================

        DataStream<AiStreamStatus> aiStreamStatusRaw = parsedStream
                .filter(e -> "ai_stream_status".equals(e.eventType))
                .map(StreamingEventsToClickHouse::toAiStreamStatusRaw)
                .filter(Objects::nonNull);

        BroadcastConnectedStream<AiStreamStatus, OrchestratorMetadata> connectedStream =
                aiStreamStatusRaw.connect(broadcastStream);

        DataStream<AiStreamStatus> enrichedAiStreamStatus = connectedStream
                .process(new EnrichAiStreamStatusFunction(metadataStateDescriptor));

        enrichedAiStreamStatus
                .addSink(createAiStreamStatusSink())
                .name("ClickHouse: ai_stream_status");

        // ============================================
        // STEP 4: Other Event Types
        // ============================================

        parsedStream
                .filter(e -> "stream_ingest_metrics".equals(e.eventType))
                .map(StreamingEventsToClickHouse::toStreamIngestMetrics)
                .filter(Objects::nonNull)
                .addSink(createStreamIngestMetricsSink())
                .name("ClickHouse: stream_ingest_metrics");

        parsedStream
                .filter(e -> "stream_trace".equals(e.eventType))
                .map(StreamingEventsToClickHouse::toStreamTraceEvent)
                .filter(Objects::nonNull)
                .addSink(createStreamTraceEventsSink())
                .name("ClickHouse: stream_trace_events");

        parsedStream
                .filter(e -> "ai_stream_events".equals(e.eventType))
                .map(StreamingEventsToClickHouse::toAiStreamEvent)
                .filter(Objects::nonNull)
                .addSink(createAiStreamEventsSink())
                .name("ClickHouse: ai_stream_events");

        parsedStream
                .filter(e -> "discovery_results".equals(e.eventType))
                .map(StreamingEventsToClickHouse::toDiscoveryResult)
                .filter(Objects::nonNull)
                .addSink(createDiscoveryResultsSink())
                .name("ClickHouse: discovery_results");

        parsedStream
                .filter(e -> "create_new_payment".equals(e.eventType))
                .map(StreamingEventsToClickHouse::toPaymentEvent)
                .filter(Objects::nonNull)
                .addSink(createPaymentEventsSink())
                .name("ClickHouse: payment_events");

        env.execute("Livepeer Streaming Events to ClickHouse");
    }

    // ============================================
    // JSON PARSING
    // ============================================

    private static final ObjectMapper mapper = new ObjectMapper();

    public static StreamingEvent parseEvent(String json) {
        try {
            JsonNode root = mapper.readTree(json);
            StreamingEvent event = new StreamingEvent();
            event.eventId = getTextOrEmpty(root, "id");
            event.eventType = getTextOrEmpty(root, "type");
            event.timestamp = root.has("timestamp") ? root.get("timestamp").asLong() : System.currentTimeMillis();
            event.gateway = getTextOrEmpty(root, "gateway");
            event.rawJson = json;
            return event;
        } catch (Exception e) {
            System.err.println("Failed to parse event: " + e.getMessage());
            return null;
        }
    }

    public static List<NetworkCapability> toNetworkCapabilities(StreamingEvent event) {
        List<NetworkCapability> results = new ArrayList<>();
        try {
            JsonNode root = mapper.readTree(event.rawJson);
            JsonNode dataArray = root.path("data");

            if (!dataArray.isArray()) {
                return results;
            }

            for (JsonNode orchNode : dataArray) {
                JsonNode hardwareArray = orchNode.path("hardware");
                if (!hardwareArray.isArray()) continue;

                for (JsonNode hwNode : hardwareArray) {
                    NetworkCapability nc = new NetworkCapability();
                    nc.eventTimestamp = new Timestamp(event.timestamp);
                    nc.orchestratorAddress = getTextOrNull(orchNode, "address");
                    nc.localAddress = getTextOrNull(orchNode, "local_address");
                    nc.orchUri = getTextOrNull(orchNode, "orch_uri");
                    nc.pipeline = getTextOrNull(hwNode, "pipeline");
                    nc.modelId = getTextOrNull(hwNode, "model_id");
                    nc.orchestratorVersion = getTextOrNull(orchNode.path("capabilities"), "version");

                    // GPU info
                    JsonNode gpuInfo = hwNode.path("gpu_info");
                    if (gpuInfo.isObject()) {
                        JsonNode firstGpu = gpuInfo.elements().next();
                        if (firstGpu != null) {
                            nc.gpuId = getTextOrNull(firstGpu, "id");
                            nc.gpuName = getTextOrNull(firstGpu, "name");
                            nc.gpuMemoryTotal = getLongOrNull(firstGpu, "memory_total");
                            nc.gpuMemoryFree = getLongOrNull(firstGpu, "memory_free");
                            nc.gpuMajor = getIntOrNull(firstGpu, "major");
                        }
                    }

                    // Pricing
                    JsonNode prices = orchNode.path("capabilities_prices");
                    if (prices.isArray() && prices.size() > 0) {
                        nc.pricePerUnit = getIntOrNull(prices.get(0), "pricePerUnit");
                    }

                    nc.rawJson = event.rawJson;
                    results.add(nc);
                }
            }
        } catch (Exception e) {
            System.err.println("Failed to parse network_capabilities: " + e.getMessage());
        }
        return results;
    }

    public static OrchestratorMetadata toOrchestratorMetadata(NetworkCapability nc) {
        OrchestratorMetadata meta = new OrchestratorMetadata();
        meta.orchestratorAddress = nc.orchestratorAddress;
        meta.localAddress = nc.localAddress;
        meta.orchUri = nc.orchUri;
        meta.gpuId = nc.gpuId;
        meta.gpuName = nc.gpuName;
        meta.modelId = nc.modelId;
        meta.lastUpdated = nc.eventTimestamp.getTime();
        return meta;
    }

    public static AiStreamStatus toAiStreamStatusRaw(StreamingEvent event) {
        try {
            JsonNode root = mapper.readTree(event.rawJson);
            JsonNode data = root.path("data");
            JsonNode inferenceStatus = data.path("inference_status");
            JsonNode inputStatus = data.path("input_status");
            JsonNode lastParams = inferenceStatus.path("last_params");
            JsonNode orchInfo = data.path("orchestrator_info");

            AiStreamStatus status = new AiStreamStatus();
            status.eventTimestamp = new Timestamp(event.timestamp);
            status.streamId = getTextOrEmpty(data, "stream_id");
            status.requestId = getTextOrEmpty(data, "request_id");
            status.gateway = event.gateway;
            status.orchestratorAddress = getTextOrNull(orchInfo, "address");
            status.orchestratorUrl = getTextOrNull(orchInfo, "url");
            status.pipeline = getTextOrEmpty(data, "pipeline");
            status.pipelineId = getTextOrEmpty(data, "pipeline_id");
            status.outputFps = (float) inferenceStatus.path("fps").asDouble(0.0);
            status.inputFps = (float) inputStatus.path("fps").asDouble(0.0);
            status.state = getTextOrEmpty(data, "state");
            status.restartCount = inferenceStatus.path("restart_count").asInt(0);
            status.lastError = getTextOrNull(inferenceStatus, "last_error");
            status.promptText = getTextOrNull(lastParams, "prompt");
            status.promptWidth = lastParams.path("width").asInt(0);
            status.promptHeight = lastParams.path("height").asInt(0);
            status.paramsHash = getTextOrEmpty(inferenceStatus, "last_params_hash");

            return status;
        } catch (Exception e) {
            System.err.println("Failed to parse ai_stream_status: " + e.getMessage());
            return null;
        }
    }

    public static StreamIngestMetrics toStreamIngestMetrics(StreamingEvent event) {
        try {
            JsonNode root = mapper.readTree(event.rawJson);
            JsonNode data = root.path("data");
            JsonNode stats = data.path("stats");
            JsonNode trackStats = stats.path("track_stats");

            StreamIngestMetrics metrics = new StreamIngestMetrics();
            metrics.eventTimestamp = new Timestamp(event.timestamp);
            metrics.streamId = getTextOrEmpty(data, "stream_id");
            metrics.requestId = getTextOrEmpty(data, "request_id");
            metrics.gateway = event.gateway;
            metrics.pipelineId = getTextOrEmpty(data, "pipeline_id");
            metrics.connectionQuality = getTextOrEmpty(stats, "conn_quality");

            // Parse track stats array
            if (trackStats.isArray()) {
                for (JsonNode track : trackStats) {
                    String type = getTextOrEmpty(track, "type");
                    if ("video".equals(type)) {
                        metrics.videoJitter = (float) track.path("jitter").asDouble(0.0);
                        metrics.videoPacketsReceived = track.path("packets_received").asLong(0);
                        metrics.videoPacketsLost = track.path("packets_lost").asLong(0);
                        metrics.videoPacketLossPct = (float) track.path("packet_loss_pct").asDouble(0.0);
                    } else if ("audio".equals(type)) {
                        metrics.audioJitter = (float) track.path("jitter").asDouble(0.0);
                        metrics.audioPacketsReceived = track.path("packets_received").asLong(0);
                        metrics.audioPacketsLost = track.path("packets_lost").asLong(0);
                        metrics.audioPacketLossPct = (float) track.path("packet_loss_pct").asDouble(0.0);
                    }
                }
            }

            JsonNode peerStats = stats.path("peer_conn_stats");
            metrics.bytesReceived = peerStats.path("BytesReceived").asLong(0);
            metrics.bytesSent = peerStats.path("BytesSent").asLong(0);

            return metrics;
        } catch (Exception e) {
            System.err.println("Failed to parse stream_ingest_metrics: " + e.getMessage());
            return null;
        }
    }

    public static StreamTraceEvent toStreamTraceEvent(StreamingEvent event) {
        try {
            JsonNode root = mapper.readTree(event.rawJson);
            JsonNode data = root.path("data");
            JsonNode orchInfo = data.path("orchestrator_info");

            StreamTraceEvent trace = new StreamTraceEvent();
            trace.eventTimestamp = new Timestamp(event.timestamp);
            trace.streamId = getTextOrEmpty(data, "stream_id");
            trace.requestId = getTextOrEmpty(data, "request_id");
            trace.gateway = event.gateway;
            trace.orchestratorAddress = getTextOrNull(orchInfo, "address");
            trace.orchestratorUrl = getTextOrNull(orchInfo, "url");
            trace.pipelineId = getTextOrEmpty(data, "pipeline_id");
            trace.traceType = getTextOrEmpty(data, "type");
            trace.dataTimestamp = new Timestamp(data.path("timestamp").asLong(event.timestamp));

            return trace;
        } catch (Exception e) {
            System.err.println("Failed to parse stream_trace: " + e.getMessage());
            return null;
        }
    }

    public static AiStreamEvent toAiStreamEvent(StreamingEvent event) {
        try {
            JsonNode root = mapper.readTree(event.rawJson);
            JsonNode data = root.path("data");

            AiStreamEvent aiEvent = new AiStreamEvent();
            aiEvent.eventTimestamp = new Timestamp(event.timestamp);
            aiEvent.streamId = getTextOrEmpty(data, "stream_id");
            aiEvent.requestId = getTextOrEmpty(data, "request_id");
            aiEvent.gateway = event.gateway;
            aiEvent.pipeline = getTextOrEmpty(data, "pipeline");
            aiEvent.pipelineId = getTextOrEmpty(data, "pipeline_id");
            aiEvent.eventType = getTextOrEmpty(data, "type");
            aiEvent.message = getTextOrEmpty(data, "message");
            aiEvent.capability = getTextOrEmpty(data, "capability");

            return aiEvent;
        } catch (Exception e) {
            System.err.println("Failed to parse ai_stream_events: " + e.getMessage());
            return null;
        }
    }

    public static DiscoveryResult toDiscoveryResult(StreamingEvent event) {
        try {
            JsonNode root = mapper.readTree(event.rawJson);
            JsonNode dataArray = root.path("data");

            if (!dataArray.isArray() || dataArray.size() == 0) {
                return null;
            }

            JsonNode firstResult = dataArray.get(0);
            DiscoveryResult result = new DiscoveryResult();
            result.eventTimestamp = new Timestamp(event.timestamp);
            result.orchestratorAddress = getTextOrEmpty(firstResult, "address");
            result.orchestratorUrl = getTextOrEmpty(firstResult, "url");
            result.latencyMs = Integer.parseInt(getTextOrEmpty(firstResult, "latency_ms").replaceAll("[^0-9]", ""));
            result.gateway = event.gateway;

            return result;
        } catch (Exception e) {
            System.err.println("Failed to parse discovery_results: " + e.getMessage());
            return null;
        }
    }

    public static PaymentEvent toPaymentEvent(StreamingEvent event) {
        try {
            JsonNode root = mapper.readTree(event.rawJson);
            JsonNode data = root.path("data");

            PaymentEvent payment = new PaymentEvent();
            payment.eventTimestamp = new Timestamp(event.timestamp);
            payment.requestId = getTextOrEmpty(data, "requestID");
            payment.sessionId = getTextOrEmpty(data, "sessionID");
            payment.manifestId = getTextOrEmpty(data, "manifestID");
            payment.sender = getTextOrEmpty(data, "sender");
            payment.recipient = getTextOrEmpty(data, "recipient");
            payment.orchestrator = getTextOrEmpty(data, "orchestrator");
            payment.faceValue = getTextOrEmpty(data, "faceValue");
            payment.price = getTextOrEmpty(data, "price");
            payment.numTickets = getTextOrEmpty(data, "numTickets");
            payment.winProb = getTextOrEmpty(data, "winProb");
            payment.gateway = event.gateway;
            payment.clientIp = getTextOrEmpty(data, "clientIP");
            payment.capability = getTextOrEmpty(data, "capability");

            return payment;
        } catch (Exception e) {
            System.err.println("Failed to parse create_new_payment: " + e.getMessage());
            return null;
        }
    }

    // Helper methods for safe JSON extraction
    private static String getTextOrEmpty(JsonNode node, String field) {
        return node.has(field) && !node.get(field).isNull() ? node.get(field).asText() : "";
    }

    private static String getTextOrNull(JsonNode node, String field) {
        return node.has(field) && !node.get(field).isNull() ? node.get(field).asText() : null;
    }

    private static Long getLongOrNull(JsonNode node, String field) {
        return node.has(field) && !node.get(field).isNull() ? node.get(field).asLong() : null;
    }

    private static Integer getIntOrNull(JsonNode node, String field) {
        return node.has(field) && !node.get(field).isNull() ? node.get(field).asInt() : null;
    }

    // ============================================
    // BROADCAST PROCESS FUNCTION
    // ============================================

    public static class EnrichAiStreamStatusFunction extends BroadcastProcessFunction<AiStreamStatus, OrchestratorMetadata, AiStreamStatus> {
        private final MapStateDescriptor<String, OrchestratorMetadata> metadataStateDescriptor;

        public EnrichAiStreamStatusFunction(MapStateDescriptor<String, OrchestratorMetadata> metadataStateDescriptor) {
            this.metadataStateDescriptor = metadataStateDescriptor;
        }

        @Override
        public void processElement(AiStreamStatus status, ReadOnlyContext ctx, Collector<AiStreamStatus> out) throws Exception {
            String lookupKey = status.orchestratorUrl;
            OrchestratorMetadata metadata = ctx.getBroadcastState(metadataStateDescriptor).get(lookupKey);

            if (metadata != null) {
                status.gpuId = metadata.gpuId;
                status.region = "us-east-1"; // Default for now
            }

            out.collect(status);
        }

        @Override
        public void processBroadcastElement(OrchestratorMetadata metadata, Context ctx, Collector<AiStreamStatus> out) throws Exception {
            ctx.getBroadcastState(metadataStateDescriptor).put(metadata.orchUri, metadata);
        }
    }

    // ============================================
    // JDBC SINKS
    // ============================================

    private static org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction<AiStreamStatus> createAiStreamStatusSink() {
        String insertSQL = "INSERT INTO ai_stream_status (" +
                "event_timestamp, stream_id, request_id, gateway, orchestrator_address, orchestrator_url, " +
                "gpu_id, region, pipeline, pipeline_id, output_fps, input_fps, state, restart_count, " +
                "last_error, prompt_text, prompt_width, prompt_height, params_hash" +
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        return JdbcSink.sink(
                insertSQL,
                (PreparedStatement ps, AiStreamStatus e) -> {
                    ps.setTimestamp(1, e.eventTimestamp);
                    ps.setString(2, e.streamId);
                    ps.setString(3, e.requestId);
                    ps.setString(4, e.gateway);
                    ps.setString(5, e.orchestratorAddress);
                    ps.setString(6, e.orchestratorUrl);
                    ps.setString(7, e.gpuId);
                    ps.setString(8, e.region);
                    ps.setString(9, e.pipeline);
                    ps.setString(10, e.pipelineId);
                    ps.setFloat(11, e.outputFps);
                    ps.setFloat(12, e.inputFps);
                    ps.setString(13, e.state);
                    ps.setInt(14, e.restartCount);
                    ps.setString(15, e.lastError);
                    ps.setString(16, e.promptText);
                    ps.setInt(17, e.promptWidth);
                    ps.setInt(18, e.promptHeight);
                    ps.setString(19, e.paramsHash);
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(200L)
                        .withMaxRetries(3)
                        .build(),
                getClickHouseConnectionOptions()
        );
    }

    private static org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction<StreamIngestMetrics> createStreamIngestMetricsSink() {
        String insertSQL = "INSERT INTO stream_ingest_metrics (" +
                "event_timestamp, stream_id, request_id, gateway, orchestrator_address, pipeline_id, " +
                "connection_quality, video_jitter, video_packets_received, video_packets_lost, video_packet_loss_pct, " +
                "audio_jitter, audio_packets_received, audio_packets_lost, audio_packet_loss_pct, " +
                "bytes_received, bytes_sent" +
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        return JdbcSink.sink(
                insertSQL,
                (PreparedStatement ps, StreamIngestMetrics e) -> {
                    ps.setTimestamp(1, e.eventTimestamp);
                    ps.setString(2, e.streamId);
                    ps.setString(3, e.requestId);
                    ps.setString(4, e.gateway);
                    ps.setString(5, e.orchestratorAddress);
                    ps.setString(6, e.pipelineId);
                    ps.setString(7, e.connectionQuality);
                    ps.setFloat(8, e.videoJitter);
                    ps.setLong(9, e.videoPacketsReceived);
                    ps.setLong(10, e.videoPacketsLost);
                    ps.setFloat(11, e.videoPacketLossPct);
                    ps.setFloat(12, e.audioJitter);
                    ps.setLong(13, e.audioPacketsReceived);
                    ps.setLong(14, e.audioPacketsLost);
                    ps.setFloat(15, e.audioPacketLossPct);
                    ps.setLong(16, e.bytesReceived);
                    ps.setLong(17, e.bytesSent);
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(200L)
                        .withMaxRetries(3)
                        .build(),
                getClickHouseConnectionOptions()
        );
    }

    private static org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction<StreamTraceEvent> createStreamTraceEventsSink() {
        String insertSQL = "INSERT INTO stream_trace_events (" +
                "event_timestamp, stream_id, request_id, gateway, " +
                "orchestrator_address, orchestrator_url, pipeline_id, " +
                "trace_type, data_timestamp" +
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

        return JdbcSink.sink(
                insertSQL,
                (PreparedStatement ps, StreamTraceEvent e) -> {
                    ps.setTimestamp(1, e.eventTimestamp);
                    ps.setString(2, e.streamId);
                    ps.setString(3, e.requestId);
                    ps.setString(4, e.gateway);
                    ps.setString(5, e.orchestratorAddress);
                    ps.setString(6, e.orchestratorUrl);
                    ps.setString(7, e.pipelineId);
                    ps.setString(8, e.traceType);
                    ps.setTimestamp(9, e.dataTimestamp);
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(200L)
                        .withMaxRetries(3)
                        .build(),
                getClickHouseConnectionOptions()
        );
    }

    private static org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction<NetworkCapability> createNetworkCapabilitiesSink() {
        String insertSQL = "INSERT INTO network_capabilities (" +
                "event_timestamp, orchestrator_address, local_address, orch_uri, " +
                "gpu_id, gpu_name, gpu_memory_total, gpu_memory_free, gpu_major, " +
                "pipeline, model_id, runner_version, capacity, capacity_in_use, " +
                "price_per_unit, orchestrator_version, raw_json" +
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        return JdbcSink.sink(
                insertSQL,
                (PreparedStatement ps, NetworkCapability e) -> {
                    ps.setTimestamp(1, e.eventTimestamp);
                    ps.setString(2, e.orchestratorAddress);
                    ps.setString(3, e.localAddress);
                    ps.setString(4, e.orchUri);
                    ps.setString(5, e.gpuId);
                    ps.setString(6, e.gpuName);
                    ps.setObject(7, e.gpuMemoryTotal);
                    ps.setObject(8, e.gpuMemoryFree);
                    ps.setObject(9, e.gpuMajor);
                    ps.setString(10, e.pipeline);
                    ps.setString(11, e.modelId);
                    ps.setString(12, e.runnerVersion);
                    ps.setObject(13, e.capacity);
                    ps.setObject(14, e.capacityInUse);
                    ps.setObject(15, e.pricePerUnit);
                    ps.setString(16, e.orchestratorVersion);
                    ps.setString(17, e.rawJson);
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(100)
                        .withBatchIntervalMs(500L)
                        .withMaxRetries(3)
                        .build(),
                getClickHouseConnectionOptions()
        );
    }

    private static org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction<AiStreamEvent> createAiStreamEventsSink() {
        String insertSQL = "INSERT INTO ai_stream_events (" +
                "event_timestamp, stream_id, request_id, gateway, " +
                "pipeline, pipeline_id, event_type, message, capability" +
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

        return JdbcSink.sink(
                insertSQL,
                (PreparedStatement ps, AiStreamEvent e) -> {
                    ps.setTimestamp(1, e.eventTimestamp);
                    ps.setString(2, e.streamId);
                    ps.setString(3, e.requestId);
                    ps.setString(4, e.gateway);
                    ps.setString(5, e.pipeline);
                    ps.setString(6, e.pipelineId);
                    ps.setString(7, e.eventType);
                    ps.setString(8, e.message);
                    ps.setString(9, e.capability);
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(500)
                        .withBatchIntervalMs(200L)
                        .withMaxRetries(3)
                        .build(),
                getClickHouseConnectionOptions()
        );
    }

    private static org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction<DiscoveryResult> createDiscoveryResultsSink() {
        String insertSQL = "INSERT INTO discovery_results (" +
                "event_timestamp, orchestrator_address, orchestrator_url, latency_ms, gateway" +
                ") VALUES (?, ?, ?, ?, ?)";

        return JdbcSink.sink(
                insertSQL,
                (PreparedStatement ps, DiscoveryResult e) -> {
                    ps.setTimestamp(1, e.eventTimestamp);
                    ps.setString(2, e.orchestratorAddress);
                    ps.setString(3, e.orchestratorUrl);
                    ps.setInt(4, e.latencyMs);
                    ps.setString(5, e.gateway);
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(100)
                        .withBatchIntervalMs(500L)
                        .withMaxRetries(3)
                        .build(),
                getClickHouseConnectionOptions()
        );
    }

    private static org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction<PaymentEvent> createPaymentEventsSink() {
        String insertSQL = "INSERT INTO payment_events (" +
                "event_timestamp, request_id, session_id, manifest_id, " +
                "sender, recipient, orchestrator, face_value, price, num_tickets, win_prob, " +
                "gateway, client_ip, capability" +
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        return JdbcSink.sink(
                insertSQL,
                (PreparedStatement ps, PaymentEvent e) -> {
                    ps.setTimestamp(1, e.eventTimestamp);
                    ps.setString(2, e.requestId);
                    ps.setString(3, e.sessionId);
                    ps.setString(4, e.manifestId);
                    ps.setString(5, e.sender);
                    ps.setString(6, e.recipient);
                    ps.setString(7, e.orchestrator);
                    ps.setString(8, e.faceValue);
                    ps.setString(9, e.price);
                    ps.setString(10, e.numTickets);
                    ps.setString(11, e.winProb);
                    ps.setString(12, e.gateway);
                    ps.setString(13, e.clientIp);
                    ps.setString(14, e.capability);
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(500)
                        .withBatchIntervalMs(200L)
                        .withMaxRetries(3)
                        .build(),
                getClickHouseConnectionOptions()
        );
    }

    private static JdbcConnectionOptions getClickHouseConnectionOptions() {
        return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl("jdbc:clickhouse://clickhouse:8123/livepeer_analytics")
                .withDriverName("com.clickhouse.jdbc.ClickHouseDriver")
                .withUsername("analytics_user")
                .withPassword("analytics_password")
                .build();
    }
}