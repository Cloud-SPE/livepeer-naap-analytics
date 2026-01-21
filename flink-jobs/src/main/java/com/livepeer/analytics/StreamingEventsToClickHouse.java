package com.livepeer.analytics;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;
import java.util.Iterator;

public class StreamingEventsToClickHouse {

    private static final ObjectMapper mapper = new ObjectMapper();

    // Dead Letter Queue output tag
    private static final OutputTag<DLQEvent> DLQ_TAG = new OutputTag<DLQEvent>("dlq"){};

    // ============================================
    // POJOs
    // ============================================

    public static class StreamingEvent implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        public String eventId, eventType, gateway, rawJson;
        public long timestamp;
        public StreamingEvent() {}
    }

    public static class DLQEvent implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        public String eventId, eventType, rawJson, errorMessage, errorType;
        public long timestamp;
        public DLQEvent() {}
    }

    public static class AiStreamStatus implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        public Timestamp eventTimestamp;
        public String streamId, requestId, gateway, orchestratorAddress, orchestratorUrl;
        public String pipeline, pipelineId, state, paramsHash, lastError, promptText;
        public float outputFps, inputFps;
        public int restartCount, promptWidth, promptHeight;
        public Timestamp startTime;
        public String rawJson;
    }

    public static class StreamIngestMetrics implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        public Timestamp eventTimestamp;
        public String streamId, requestId, pipelineId, connectionQuality;
        public float videoJitter, audioJitter;
        public int videoPacketsReceived, videoPacketsLost, audioPacketsReceived, audioPacketsLost;
        public float videoPacketLossPct, audioPacketLossPct;
        public float videoRtt, audioRtt, videoLastInputTs, audioLastInputTs, videoLatency, audioLatency;
        public long bytesReceived, bytesSent;
        public String rawJson;
    }

    public static class StreamTraceEvent implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        public Timestamp eventTimestamp, dataTimestamp;
        public String streamId, requestId, pipelineId, orchestratorAddress, orchestratorUrl, traceType;
        public String rawJson;
    }

    public static class NetworkCapability implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        public Timestamp eventTimestamp;
        public String orchestratorAddress, localAddress, orchUri, gpuId, gpuName;
        public String pipeline, modelId, runnerVersion, orchestratorVersion;
        public Long gpuMemoryTotal, gpuMemoryFree;
        public Integer gpuMajor, gpuMinor, capacity, capacityInUse, pricePerUnit, pixelsPerUnit;
        public Integer warm;
        public String rawJson;
    }

    public static class AiStreamEvent implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        public Timestamp eventTimestamp;
        public String streamId, requestId, pipeline, pipelineId, eventType, message, capability;
        public String rawJson;
    }

    public static class DiscoveryResult implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        public Timestamp eventTimestamp;
        public String orchestratorAddress, orchestratorUrl;
        public int latencyMs;
        public String rawJson;
    }

    public static class PaymentEvent implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        public Timestamp eventTimestamp;
        public String requestId, sessionId, manifestId, sender, recipient, orchestrator;
        public String faceValue, price, numTickets, winProb, clientIp, capability;
        public String rawJson;
    }

    // ============================================
    // MAIN JOB
    // ============================================

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60000);

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("kafka:9092")
                .setTopics("streaming_events")
                .setGroupId("flink-java-v200")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // Parse all events from Kafka
        SingleOutputStreamOperator<StreamingEvent> parsedStream = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .map(StreamingEventsToClickHouse::parseEvent)
                .returns(StreamingEvent.class)
                .filter(e -> e != null);

        // Route events by type and handle errors
        SingleOutputStreamOperator<AiStreamStatus> aiStatusStream = parsedStream
                .filter(e -> "ai_stream_status".equals(e.eventType))
                .process(new SafeParser<>(StreamingEventsToClickHouse::parseAiStreamStatus))
                .returns(AiStreamStatus.class);

        SingleOutputStreamOperator<StreamIngestMetrics> ingestMetricsStream = parsedStream
                .filter(e -> "stream_ingest_metrics".equals(e.eventType))
                .process(new SafeParser<>(StreamingEventsToClickHouse::parseStreamIngestMetrics))
                .returns(StreamIngestMetrics.class);

        SingleOutputStreamOperator<StreamTraceEvent> traceStream = parsedStream
                .filter(e -> "stream_trace".equals(e.eventType))
                .process(new SafeParser<>(StreamingEventsToClickHouse::parseStreamTrace))
                .returns(StreamTraceEvent.class);

        SingleOutputStreamOperator<NetworkCapability> networkCapStream = parsedStream
                .filter(e -> "network_capabilities".equals(e.eventType))
                .process(new SafeParser<>(StreamingEventsToClickHouse::parseNetworkCapabilities))
                .returns(NetworkCapability.class);

        SingleOutputStreamOperator<AiStreamEvent> aiEventsStream = parsedStream
                .filter(e -> "ai_stream_events".equals(e.eventType))
                .process(new SafeParser<>(StreamingEventsToClickHouse::parseAiStreamEvent))
                .returns(AiStreamEvent.class);

        SingleOutputStreamOperator<DiscoveryResult> discoveryStream = parsedStream
                .filter(e -> "discovery_results".equals(e.eventType))
                .process(new SafeParser<>(StreamingEventsToClickHouse::parseDiscoveryResults))
                .returns(DiscoveryResult.class);

        SingleOutputStreamOperator<PaymentEvent> paymentStream = parsedStream
                .filter(e -> "create_new_payment".equals(e.eventType))
                .process(new SafeParser<>(StreamingEventsToClickHouse::parsePaymentEvent))
                .returns(PaymentEvent.class);

        // Collect DLQ events from all streams
        DataStream<DLQEvent> dlqStream = aiStatusStream.getSideOutput(DLQ_TAG)
                .union(ingestMetricsStream.getSideOutput(DLQ_TAG))
                .union(traceStream.getSideOutput(DLQ_TAG))
                .union(networkCapStream.getSideOutput(DLQ_TAG))
                .union(aiEventsStream.getSideOutput(DLQ_TAG))
                .union(discoveryStream.getSideOutput(DLQ_TAG))
                .union(paymentStream.getSideOutput(DLQ_TAG));

        // ============================================
        // ClickHouse Sinks
        // ============================================

        JdbcConnectionOptions chOptions = getCHOptions();

        // AI Stream Status
        aiStatusStream.addSink(JdbcSink.sink(
                "INSERT INTO ai_stream_status (event_timestamp, stream_id, request_id, gateway, " +
                "orchestrator_address, orchestrator_url, pipeline, pipeline_id, output_fps, input_fps, " +
                "state, restart_count, last_error, prompt_text, prompt_width, prompt_height, params_hash, " +
                "start_time, raw_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (ps, s) -> {
                    ps.setTimestamp(1, s.eventTimestamp);
                    ps.setString(2, s.streamId);
                    ps.setString(3, s.requestId);
                    ps.setString(4, s.gateway);
                    ps.setString(5, s.orchestratorAddress);
                    ps.setString(6, s.orchestratorUrl);
                    ps.setString(7, s.pipeline);
                    ps.setString(8, s.pipelineId);
                    ps.setFloat(9, s.outputFps);
                    ps.setFloat(10, s.inputFps);
                    ps.setString(11, s.state);
                    ps.setInt(12, s.restartCount);
                    ps.setString(13, s.lastError);
                    ps.setString(14, s.promptText);
                    ps.setInt(15, s.promptWidth);
                    ps.setInt(16, s.promptHeight);
                    ps.setString(17, s.paramsHash);
                    ps.setTimestamp(18, s.startTime);
                    ps.setString(19, s.rawJson);
                },
                JdbcExecutionOptions.builder().withBatchSize(200).build(),
                chOptions
        )).name("CH: AI Status");

        // Stream Ingest Metrics
        ingestMetricsStream.addSink(JdbcSink.sink(
                "INSERT INTO stream_ingest_metrics (event_timestamp, stream_id, request_id, pipeline_id, " +
                "connection_quality, video_jitter, video_packets_received, video_packets_lost, video_packet_loss_pct, " +
                "video_rtt, video_last_input_ts, video_latency, audio_jitter, audio_packets_received, " +
                "audio_packets_lost, audio_packet_loss_pct, audio_rtt, audio_last_input_ts, audio_latency, " +
                "bytes_received, bytes_sent, raw_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (ps, m) -> {
                    ps.setTimestamp(1, m.eventTimestamp);
                    ps.setString(2, m.streamId);
                    ps.setString(3, m.requestId);
                    ps.setString(4, m.pipelineId);
                    ps.setString(5, m.connectionQuality);
                    ps.setFloat(6, m.videoJitter);
                    ps.setInt(7, m.videoPacketsReceived);
                    ps.setInt(8, m.videoPacketsLost);
                    ps.setFloat(9, m.videoPacketLossPct);
                    ps.setFloat(10, m.videoRtt);
                    ps.setFloat(11, m.videoLastInputTs);
                    ps.setFloat(12, m.videoLatency);
                    ps.setFloat(13, m.audioJitter);
                    ps.setInt(14, m.audioPacketsReceived);
                    ps.setInt(15, m.audioPacketsLost);
                    ps.setFloat(16, m.audioPacketLossPct);
                    ps.setFloat(17, m.audioRtt);
                    ps.setFloat(18, m.audioLastInputTs);
                    ps.setFloat(19, m.audioLatency);
                    ps.setLong(20, m.bytesReceived);
                    ps.setLong(21, m.bytesSent);
                    ps.setString(22, m.rawJson);
                },
                JdbcExecutionOptions.builder().withBatchSize(200).build(),
                chOptions
        )).name("CH: Ingest Metrics");

        // Stream Trace Events
        traceStream.addSink(JdbcSink.sink(
                "INSERT INTO stream_trace_events (event_timestamp, stream_id, request_id, pipeline_id, " +
                "orchestrator_address, orchestrator_url, trace_type, data_timestamp, raw_json) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (ps, t) -> {
                    ps.setTimestamp(1, t.eventTimestamp);
                    ps.setString(2, t.streamId);
                    ps.setString(3, t.requestId);
                    ps.setString(4, t.pipelineId);
                    ps.setString(5, t.orchestratorAddress);
                    ps.setString(6, t.orchestratorUrl);
                    ps.setString(7, t.traceType);
                    ps.setTimestamp(8, t.dataTimestamp);
                    ps.setString(9, t.rawJson);
                },
                JdbcExecutionOptions.builder().withBatchSize(200).build(),
                chOptions
        )).name("CH: Trace Events");

        // Network Capabilities
        networkCapStream.addSink(JdbcSink.sink(
                "INSERT INTO network_capabilities (event_timestamp, orchestrator_address, local_address, " +
                "orch_uri, gpu_id, gpu_name, gpu_memory_total, gpu_memory_free, gpu_major, gpu_minor, " +
                "pipeline, model_id, runner_version, capacity, capacity_in_use, warm, price_per_unit, " +
                "pixels_per_unit, orchestrator_version, raw_json) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (ps, n) -> {
                    ps.setTimestamp(1, n.eventTimestamp);
                    ps.setString(2, n.orchestratorAddress);
                    ps.setString(3, n.localAddress);
                    ps.setString(4, n.orchUri);
                    ps.setString(5, n.gpuId);
                    ps.setString(6, n.gpuName);
                    setNullableLong(ps, 7, n.gpuMemoryTotal);
                    setNullableLong(ps, 8, n.gpuMemoryFree);
                    setNullableInt(ps, 9, n.gpuMajor);
                    setNullableInt(ps, 10, n.gpuMinor);
                    ps.setString(11, n.pipeline);
                    ps.setString(12, n.modelId);
                    ps.setString(13, n.runnerVersion);
                    setNullableInt(ps, 14, n.capacity);
                    setNullableInt(ps, 15, n.capacityInUse);
                    setNullableInt(ps, 16, n.warm);
                    setNullableInt(ps, 17, n.pricePerUnit);
                    setNullableInt(ps, 18, n.pixelsPerUnit);
                    ps.setString(19, n.orchestratorVersion);
                    ps.setString(20, n.rawJson);
                },
                JdbcExecutionOptions.builder().withBatchSize(100).build(),
                chOptions
        )).name("CH: Network Caps");

        // AI Stream Events
        aiEventsStream.addSink(JdbcSink.sink(
                "INSERT INTO ai_stream_events (event_timestamp, stream_id, request_id, pipeline, " +
                "pipeline_id, event_type, message, capability, raw_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (ps, e) -> {
                    ps.setTimestamp(1, e.eventTimestamp);
                    ps.setString(2, e.streamId);
                    ps.setString(3, e.requestId);
                    ps.setString(4, e.pipeline);
                    ps.setString(5, e.pipelineId);
                    ps.setString(6, e.eventType);
                    ps.setString(7, e.message);
                    ps.setString(8, e.capability);
                    ps.setString(9, e.rawJson);
                },
                JdbcExecutionOptions.builder().withBatchSize(200).build(),
                chOptions
        )).name("CH: AI Events");

        // Discovery Results
        discoveryStream.addSink(JdbcSink.sink(
                "INSERT INTO discovery_results (event_timestamp, orchestrator_address, orchestrator_url, " +
                "latency_ms, raw_json) VALUES (?, ?, ?, ?, ?)",
                (ps, d) -> {
                    ps.setTimestamp(1, d.eventTimestamp);
                    ps.setString(2, d.orchestratorAddress);
                    ps.setString(3, d.orchestratorUrl);
                    ps.setInt(4, d.latencyMs);
                    ps.setString(5, d.rawJson);
                },
                JdbcExecutionOptions.builder().withBatchSize(200).build(),
                chOptions
        )).name("CH: Discovery");

        // Payment Events
        paymentStream.addSink(JdbcSink.sink(
                "INSERT INTO payment_events (event_timestamp, request_id, session_id, manifest_id, " +
                "sender, recipient, orchestrator, face_value, price, num_tickets, win_prob, " +
                "client_ip, capability, raw_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (ps, p) -> {
                    ps.setTimestamp(1, p.eventTimestamp);
                    ps.setString(2, p.requestId);
                    ps.setString(3, p.sessionId);
                    ps.setString(4, p.manifestId);
                    ps.setString(5, p.sender);
                    ps.setString(6, p.recipient);
                    ps.setString(7, p.orchestrator);
                    ps.setString(8, p.faceValue);
                    ps.setString(9, p.price);
                    ps.setString(10, p.numTickets);
                    ps.setString(11, p.winProb);
                    ps.setString(12, p.clientIp);
                    ps.setString(13, p.capability);
                    ps.setString(14, p.rawJson);
                },
                JdbcExecutionOptions.builder().withBatchSize(100).build(),
                chOptions
        )).name("CH: Payments");

        // DLQ Sink
        dlqStream.addSink(JdbcSink.sink(
                "INSERT INTO streaming_events_dlq (event_id, event_type, event_timestamp, raw_json, " +
                "error_message, error_type) VALUES (?, ?, ?, ?, ?, ?)",
                (ps, d) -> {
                    ps.setString(1, d.eventId);
                    ps.setString(2, d.eventType);
                    ps.setTimestamp(3, new Timestamp(d.timestamp));
                    ps.setString(4, d.rawJson);
                    ps.setString(5, d.errorMessage);
                    ps.setString(6, d.errorType);
                },
                JdbcExecutionOptions.builder().withBatchSize(100).build(),
                chOptions
        )).name("CH: DLQ");

        env.execute("Livepeer Analytics v2.0");
    }

    // ============================================
    // PARSING FUNCTIONS
    // ============================================

    public static StreamingEvent parseEvent(String json) {
        try {
            JsonNode n = mapper.readTree(json);
            StreamingEvent e = new StreamingEvent();
            e.eventId = n.path("id").asText("");
            e.eventType = n.path("type").asText("");
            e.timestamp = n.path("timestamp").asLong(System.currentTimeMillis());
            e.gateway = n.path("gateway").asText("");
            e.rawJson = json;
            return e;
        } catch (Exception ex) {
            return null;
        }
    }

    public static AiStreamStatus parseAiStreamStatus(StreamingEvent e) throws Exception {
        JsonNode data = mapper.readTree(e.rawJson).get("data");
        AiStreamStatus s = new AiStreamStatus();

        s.eventTimestamp = new Timestamp(e.timestamp);
        s.streamId = data.path("stream_id").asText("");
        s.requestId = data.path("request_id").asText("");
        s.gateway = e.gateway;

        JsonNode orchInfo = data.path("orchestrator_info");
        s.orchestratorAddress = orchInfo.path("address").asText("");
        s.orchestratorUrl = orchInfo.path("url").asText("");

        s.pipeline = data.path("pipeline").asText("");
        s.pipelineId = data.path("pipeline_id").asText("");
        s.state = data.path("state").asText("");

        JsonNode inferStatus = data.path("inference_status");
        s.outputFps = (float) inferStatus.path("fps").asDouble(0.0);
        s.restartCount = inferStatus.path("restart_count").asInt(0);
        s.lastError = inferStatus.path("last_error").asText(null);
        s.paramsHash = inferStatus.path("last_params_hash").asText("");

        JsonNode lastParams = inferStatus.path("last_params");
        s.promptText = lastParams.path("prompt").asText(null);
        s.promptWidth = lastParams.path("width").asInt(0);
        s.promptHeight = lastParams.path("height").asInt(0);

        JsonNode inputStatus = data.path("input_status");
        s.inputFps = (float) inputStatus.path("fps").asDouble(0.0);

        s.startTime = new Timestamp(data.path("start_time").asLong(e.timestamp));
        s.rawJson = e.rawJson;

        return s;
    }

    public static StreamIngestMetrics parseStreamIngestMetrics(StreamingEvent e) throws Exception {
        JsonNode data = mapper.readTree(e.rawJson).get("data");
        StreamIngestMetrics m = new StreamIngestMetrics();

        m.eventTimestamp = new Timestamp(e.timestamp);
        m.streamId = data.path("stream_id").asText("");
        m.requestId = data.path("request_id").asText("");
        m.pipelineId = data.path("pipeline_id").asText("");

        JsonNode stats = data.path("stats");
        m.connectionQuality = stats.path("conn_quality").asText("");

        JsonNode peerStats = stats.path("peer_conn_stats");
        m.bytesReceived = peerStats.path("BytesReceived").asLong(0);
        m.bytesSent = peerStats.path("BytesSent").asLong(0);

        JsonNode trackStats = stats.path("track_stats");
        if (trackStats.isArray()) {
            for (JsonNode track : trackStats) {
                String type = track.path("type").asText("");
                if ("video".equals(type)) {
                    m.videoJitter = (float) track.path("jitter").asDouble(0.0);
                    m.videoPacketsReceived = track.path("packets_received").asInt(0);
                    m.videoPacketsLost = track.path("packets_lost").asInt(0);
                    m.videoPacketLossPct = (float) track.path("packet_loss_pct").asDouble(0.0);
                    m.videoRtt = (float) track.path("rtt").asDouble(0.0);
                    m.videoLastInputTs = (float) track.path("last_input_ts").asDouble(0.0);
                    m.videoLatency = (float) track.path("latency").asDouble(0.0);
                } else if ("audio".equals(type)) {
                    m.audioJitter = (float) track.path("jitter").asDouble(0.0);
                    m.audioPacketsReceived = track.path("packets_received").asInt(0);
                    m.audioPacketsLost = track.path("packets_lost").asInt(0);
                    m.audioPacketLossPct = (float) track.path("packet_loss_pct").asDouble(0.0);
                    m.audioRtt = (float) track.path("rtt").asDouble(0.0);
                    m.audioLastInputTs = (float) track.path("last_input_ts").asDouble(0.0);
                    m.audioLatency = (float) track.path("latency").asDouble(0.0);
                }
            }
        }

        m.rawJson = e.rawJson;
        return m;
    }

    public static StreamTraceEvent parseStreamTrace(StreamingEvent e) throws Exception {
        JsonNode data = mapper.readTree(e.rawJson).get("data");
        StreamTraceEvent t = new StreamTraceEvent();

        t.eventTimestamp = new Timestamp(e.timestamp);
        t.streamId = data.path("stream_id").asText("");
        t.requestId = data.path("request_id").asText("");
        t.pipelineId = data.path("pipeline_id").asText("");
        t.traceType = data.path("type").asText("");

        JsonNode orchInfo = data.path("orchestrator_info");
        t.orchestratorAddress = orchInfo.path("address").asText("");
        t.orchestratorUrl = orchInfo.path("url").asText("");

        t.dataTimestamp = new Timestamp(data.path("timestamp").asLong(e.timestamp));
        t.rawJson = e.rawJson;

        return t;
    }

    public static NetworkCapability parseNetworkCapabilities(StreamingEvent e) throws Exception {
        JsonNode dataArray = mapper.readTree(e.rawJson).get("data");
        NetworkCapability result = null;

        // Process first orchestrator in array (Flink will emit one record per orchestrator)
        if (dataArray.isArray() && dataArray.size() > 0) {
            JsonNode orch = dataArray.get(0);

            // Process first hardware entry
            JsonNode hwArray = orch.path("hardware");
            if (hwArray.isArray() && hwArray.size() > 0) {
                JsonNode hw = hwArray.get(0);
                NetworkCapability n = new NetworkCapability();

                n.eventTimestamp = new Timestamp(e.timestamp);
                n.orchestratorAddress = orch.path("address").asText("");
                n.localAddress = orch.path("local_address").asText("");
                n.orchUri = orch.path("orch_uri").asText("");

                JsonNode gpuInfo = hw.path("gpu_info").path("0");
                n.gpuId = gpuInfo.path("id").asText(null);
                n.gpuName = gpuInfo.path("name").asText(null);
                n.gpuMemoryTotal = gpuInfo.path("memory_total").asLong(0L);
                n.gpuMemoryFree = gpuInfo.path("memory_free").asLong(0L);
                n.gpuMajor = gpuInfo.path("major").asInt(0);
                n.gpuMinor = gpuInfo.path("minor").asInt(0);

                n.pipeline = hw.path("pipeline").asText("");
                n.modelId = hw.path("model_id").asText("");

                JsonNode caps = orch.path("capabilities");
                n.orchestratorVersion = caps.path("version").asText("");

                JsonNode constraints = caps.path("constraints").path("PerCapability").path("35").path("models");
                Iterator<String> modelNames = constraints.fieldNames();
                if (modelNames.hasNext()) {
                    String modelName = modelNames.next();
                    JsonNode modelCfg = constraints.path(modelName);
                    n.runnerVersion = modelCfg.path("runnerVersion").asText(null);
                    n.capacity = modelCfg.path("capacity").asInt(0);
                    n.capacityInUse = modelCfg.path("capacityInUse").asInt(0);
                    n.warm = modelCfg.path("warm").asBoolean(false) ? 1 : 0;
                }

                JsonNode priceArray = orch.path("capabilities_prices");
                if (priceArray.isArray() && priceArray.size() > 0) {
                    JsonNode priceInfo = priceArray.get(0);
                    n.pricePerUnit = priceInfo.path("pricePerUnit").asInt(0);
                    n.pixelsPerUnit = priceInfo.path("pixelsPerUnit").asInt(0);
                }

                n.rawJson = orch.toString();
                result = n;
            }
        }

        if (result == null) {
            throw new Exception("No valid orchestrator data found");
        }
        return result;
    }

    public static AiStreamEvent parseAiStreamEvent(StreamingEvent e) throws Exception {
        JsonNode data = mapper.readTree(e.rawJson).get("data");
        AiStreamEvent evt = new AiStreamEvent();

        evt.eventTimestamp = new Timestamp(e.timestamp);
        evt.streamId = data.path("stream_id").asText("");
        evt.requestId = data.path("request_id").asText("");
        evt.pipeline = data.path("pipeline").asText("");
        evt.pipelineId = data.path("pipeline_id").asText("");
        evt.eventType = data.path("type").asText("");
        evt.message = data.path("message").asText("");
        evt.capability = data.path("capability").asText("");
        evt.rawJson = e.rawJson;

        return evt;
    }

    public static DiscoveryResult parseDiscoveryResults(StreamingEvent e) throws Exception {
        JsonNode dataArray = mapper.readTree(e.rawJson).get("data");
        DiscoveryResult result = null;

        if (dataArray.isArray() && dataArray.size() > 0) {
            JsonNode disc = dataArray.get(0);
            DiscoveryResult d = new DiscoveryResult();

            d.eventTimestamp = new Timestamp(e.timestamp);
            d.orchestratorAddress = disc.path("address").asText("");
            d.orchestratorUrl = disc.path("url").asText("");
            d.latencyMs = disc.path("latency_ms").asInt(0);
            d.rawJson = disc.toString();

            result = d;
        }

        if (result == null) {
            throw new Exception("No valid discovery data found");
        }
        return result;
    }

    public static PaymentEvent parsePaymentEvent(StreamingEvent e) throws Exception {
        JsonNode data = mapper.readTree(e.rawJson).get("data");
        PaymentEvent p = new PaymentEvent();

        p.eventTimestamp = new Timestamp(e.timestamp);
        p.requestId = data.path("requestID").asText("");
        p.sessionId = data.path("sessionID").asText("");
        p.manifestId = data.path("manifestID").asText("");
        p.sender = data.path("sender").asText("");
        p.recipient = data.path("recipient").asText("");
        p.orchestrator = data.path("orchestrator").asText("");
        p.faceValue = data.path("faceValue").asText("");
        p.price = data.path("price").asText("");
        p.numTickets = data.path("numTickets").asText("");
        p.winProb = data.path("winProb").asText("");
        p.clientIp = data.path("clientIP").asText("");
        p.capability = data.path("capability").asText("");
        p.rawJson = e.rawJson;

        return p;
    }

    // ============================================
    // HELPER CLASSES & METHODS
    // ============================================

    @FunctionalInterface
    interface Parser<T> extends java.io.Serializable {
        T parse(StreamingEvent event) throws Exception;
    }

    static class SafeParser<T> extends org.apache.flink.streaming.api.functions.ProcessFunction<StreamingEvent, T> {
        private static final long serialVersionUID = 1L;
        private final Parser<T> parser;

        SafeParser(Parser<T> parser) {
            this.parser = parser;
        }

        @Override
        public void processElement(StreamingEvent event, Context ctx, org.apache.flink.util.Collector<T> out) {
            try {
                T result = parser.parse(event);
                out.collect(result);
            } catch (Exception ex) {
                DLQEvent dlq = new DLQEvent();
                dlq.eventId = event.eventId;
                dlq.eventType = event.eventType;
                dlq.timestamp = event.timestamp;
                dlq.rawJson = event.rawJson;
                dlq.errorMessage = ex.getMessage();
                dlq.errorType = ex.getClass().getSimpleName();
                ctx.output(DLQ_TAG, dlq);
            }
        }
    }

    private static void setNullableInt(java.sql.PreparedStatement ps, int index, Integer value) throws java.sql.SQLException {
        if (value == null) {
            ps.setNull(index, java.sql.Types.INTEGER);
        } else {
            ps.setInt(index, value);
        }
    }

    private static void setNullableLong(java.sql.PreparedStatement ps, int index, Long value) throws java.sql.SQLException {
        if (value == null) {
            ps.setNull(index, java.sql.Types.BIGINT);
        } else {
            ps.setLong(index, value);
        }
    }

    private static JdbcConnectionOptions getCHOptions() {
        return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl("jdbc:clickhouse://clickhouse:8123/livepeer_analytics")
                .withDriverName("com.clickhouse.jdbc.ClickHouseDriver")
                .withUsername("analytics_user")
                .withPassword("analytics_password")
                .build();
    }
}