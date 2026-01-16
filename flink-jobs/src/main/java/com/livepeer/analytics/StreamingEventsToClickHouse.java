package com.livepeer.analytics;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.AvroParquetWriters;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class StreamingEventsToClickHouse {

    private static final ObjectMapper mapper = new ObjectMapper();

    // ============================================
    // POJOs (Replacing Case Classes)
    // ============================================
    public static class StreamingEvent {
        public String eventId, eventType, gateway, rawJson;
        public long timestamp;
        public StreamingEvent() {}
    }

    public static class OrchestratorMetadata {
        public String orchestratorAddress, localAddress, orchUri, modelId, gpuId, gpuName;
        public long lastUpdated;
        public OrchestratorMetadata() {}
    }

    public static class AiStreamStatus {
        public Timestamp eventTimestamp;
        public String streamId, requestId, gateway, orchestratorAddress, orchestratorUrl, gpuId, region, pipeline, pipelineId, state, paramsHash, lastError, promptText;
        public float outputFps, inputFps;
        public int restartCount, promptWidth, promptHeight;

        public AiStreamStatus copy() {
            AiStreamStatus n = new AiStreamStatus();
            n.eventTimestamp = this.eventTimestamp; n.streamId = this.streamId; n.requestId = this.requestId;
            n.gateway = this.gateway; n.orchestratorAddress = this.orchestratorAddress;
            n.orchestratorUrl = this.orchestratorUrl; n.gpuId = this.gpuId; n.region = this.region;
            n.pipeline = this.pipeline; n.pipelineId = this.pipelineId; n.outputFps = this.outputFps;
            n.inputFps = this.inputFps; n.state = this.state; n.restartCount = this.restartCount;
            n.lastError = this.lastError; n.promptText = this.promptText; n.promptWidth = this.promptWidth;
            n.promptHeight = this.promptHeight; n.paramsHash = this.paramsHash;
            return n;
        }
    }

    public static class NetworkCapability {
        public Timestamp eventTimestamp;
        public String orchestratorAddress, localAddress, orchUri, gpuId, gpuName, pipeline, modelId, runnerVersion, orchestratorVersion, rawJson;
        public Long gpuMemoryTotal, gpuMemoryFree;
        public Integer gpuMajor, capacity, capacityInUse, pricePerUnit;
    }

    // ============================================
    // MAIN JOB
    // ============================================
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60000);

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("kafka:9092")
                .setTopics("streaming-events")
                .setGroupId("flink-java-v120")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<StreamingEvent> parsedStream = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .map(StreamingEventsToClickHouse::parseEvent)
                .returns(StreamingEvent.class)
                .filter(e -> e != null);

        // 1. Network Capabilities & Broadcast
        DataStream<NetworkCapability> netCaps = parsedStream
                .filter(e -> "network_capabilities".equals(e.eventType))
                .flatMap((StreamingEvent e, Collector<NetworkCapability> out) -> toNetworkCapabilities(e).forEach(out::collect))
                .returns(NetworkCapability.class);

        MapStateDescriptor<String, OrchestratorMetadata> metaDesc =
                new MapStateDescriptor<>("orch-meta", Types.STRING, TypeInformation.of(OrchestratorMetadata.class));

        BroadcastStream<OrchestratorMetadata> broadcast = netCaps
                .map(StreamingEventsToClickHouse::toMeta)
                .returns(OrchestratorMetadata.class)
                .broadcast(metaDesc);

        // 2. ClickHouse Sink for Network Capabilities
        netCaps.addSink(JdbcSink.sink(
                "INSERT INTO network_capabilities (event_timestamp, orchestrator_address, gpu_id, model_id, raw_json) VALUES (?, ?, ?, ?, ?)",
                (ps, n) -> {
                    ps.setTimestamp(1, n.eventTimestamp);
                    ps.setString(2, n.orchestratorAddress);
                    ps.setString(3, n.gpuId);
                    ps.setString(4, n.modelId);
                    ps.setString(5, n.rawJson);
                },
                JdbcExecutionOptions.builder().withBatchSize(100).build(),
                getCHOptions()
        )).name("CH: Network Caps");

        // 3. AI Stream Status Enrichment
        DataStream<AiStreamStatus> enrichedAi = parsedStream
                .filter(e -> "ai_stream_status".equals(e.eventType))
                .map(StreamingEventsToClickHouse::toAiRaw)
                .returns(AiStreamStatus.class)
                .filter(s -> s != null)
                .connect(broadcast)
                .process(new EnrichAiFunction(metaDesc))
                .returns(AiStreamStatus.class);

        enrichedAi.addSink(JdbcSink.sink(
                "INSERT INTO ai_stream_status (event_timestamp, stream_id, region, gpu_id) VALUES (?, ?, ?, ?)",
                (ps, s) -> {
                    ps.setTimestamp(1, s.eventTimestamp);
                    ps.setString(2, s.streamId);
                    ps.setString(3, s.region);
                    ps.setString(4, s.gpuId);
                },
                JdbcExecutionOptions.builder().withBatchSize(200).build(),
                getCHOptions()
        )).name("CH: AI Status");

        // 4. Parquet Archive (Modern FileSink)
//         FileSink<GenericRecord> parquetSink = FileSink
//                 .forBulkFormat(new Path("s3://livepeer-events/raw/"),
//                                AvroParquetWriters.forGenericRecord(createAvroSchema()))
//                 .withRollingPolicy(OnCheckpointRollingPolicy.build())
//                 .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy/MM/dd"))
//                 .build();
//
//         parsedStream.map(StreamingEventsToClickHouse::toAvro)
//                 .returns(GenericRecord.class)
//                 .sinkTo(parquetSink)
//                 .name("S3 Archive");

        env.execute("Livepeer Analytics Java 1.20");
    }

    // ============================================
    // BROADCAST LOGIC
    // ============================================
    public static class EnrichAiFunction extends BroadcastProcessFunction<AiStreamStatus, OrchestratorMetadata, AiStreamStatus> {
        private final MapStateDescriptor<String, OrchestratorMetadata> desc;
        public EnrichAiFunction(MapStateDescriptor<String, OrchestratorMetadata> d) { this.desc = d; }

        @Override
        public void processElement(AiStreamStatus status, ReadOnlyContext ctx, Collector<AiStreamStatus> out) throws Exception {
            String key = status.orchestratorAddress + ":" + status.orchestratorUrl + ":" + status.pipeline;
            OrchestratorMetadata meta = ctx.getBroadcastState(desc).get(key);
            AiStreamStatus enriched = status.copy();
            if (meta != null) {
                enriched.gpuId = meta.gpuId;
                enriched.region = inferRegion(status.orchestratorUrl).orElse("unknown");
            }
            out.collect(enriched);
        }

        @Override
        public void processBroadcastElement(OrchestratorMetadata meta, Context ctx, Collector<AiStreamStatus> out) throws Exception {
            String key = meta.orchestratorAddress + ":" + meta.orchUri + ":" + meta.modelId;
            ctx.getBroadcastState(desc).put(key, meta);
        }
    }

    private static Optional<String> inferRegion(String url) {
        if (url == null) return Optional.empty();
        String l = url.toLowerCase();
        if (l.contains("us-east")) return Optional.of("us-east-1");
        if (l.contains("eu-west")) return Optional.of("eu-west-1");
        return Optional.of("other");
    }

    // ============================================
    // MAPPING HELPERS
    // ============================================
    public static StreamingEvent parseEvent(String json) {
        try {
            JsonNode n = mapper.readTree(json);
            StreamingEvent e = new StreamingEvent();
            e.eventId = n.get("id").asText();
            e.eventType = n.get("type").asText();
            e.timestamp = n.get("timestamp").asLong();
            e.gateway = n.path("gateway").asText("");
            e.rawJson = json;
            return e;
        } catch (Exception ex) { return null; }
    }

    public static List<NetworkCapability> toNetworkCapabilities(StreamingEvent event) {
        List<NetworkCapability> list = new ArrayList<>();
        try {
            JsonNode data = mapper.readTree(event.rawJson).get("data");
            for (JsonNode orch : data) {
                for (JsonNode hw : orch.get("hardware")) {
                    NetworkCapability nc = new NetworkCapability();
                    nc.eventTimestamp = new Timestamp(event.timestamp);
                    nc.orchestratorAddress = orch.path("address").asText("");
                    nc.orchUri = orch.path("orch_uri").asText("");
                    nc.gpuId = hw.path("gpu_info").path("0").path("id").asText(null);
                    nc.modelId = hw.path("model_id").asText("");
                    nc.rawJson = orch.toString();
                    list.add(nc);
                }
            }
        } catch (Exception ignored) {}
        return list;
    }

    public static OrchestratorMetadata toMeta(NetworkCapability nc) {
        OrchestratorMetadata m = new OrchestratorMetadata();
        m.orchestratorAddress = nc.orchestratorAddress;
        m.orchUri = nc.orchUri;
        m.gpuId = nc.gpuId;
        m.modelId = nc.modelId;
        m.lastUpdated = System.currentTimeMillis();
        return m;
    }

    public static AiStreamStatus toAiRaw(StreamingEvent e) {
        try {
            JsonNode data = mapper.readTree(e.rawJson).get("data");
            AiStreamStatus s = new AiStreamStatus();
            s.eventTimestamp = new Timestamp(e.timestamp);
            s.streamId = data.path("stream_id").asText("");
            s.orchestratorAddress = data.path("orchestrator_info").path("address").asText("");
            s.orchestratorUrl = data.path("orchestrator_info").path("url").asText("");
            s.pipeline = data.path("pipeline").asText("");
            return s;
        } catch (Exception ex) { return null; }
    }

    public static Schema createAvroSchema() {
        return new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"StreamingEvent\",\"namespace\":\"com.livepeer.analytics\",\"fields\":[{\"name\":\"event_id\",\"type\":\"string\"},{\"name\":\"event_type\",\"type\":\"string\"},{\"name\":\"timestamp\",\"type\":\"long\"},{\"name\":\"gateway\",\"type\":\"string\"},{\"name\":\"raw_json\",\"type\":\"string\"}]}");
    }

    public static GenericRecord toAvro(StreamingEvent e) {
        GenericRecord r = new GenericData.Record(createAvroSchema());
        r.put("event_id", e.eventId); r.put("event_type", e.eventType);
        r.put("timestamp", e.timestamp); r.put("gateway", e.gateway);
        r.put("raw_json", e.rawJson);
        return r;
    }

    private static JdbcConnectionOptions getCHOptions() {
        return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl("jdbc:clickhouse://clickhouse:8123/livepeer_analytics")
                .withDriverName("com.clickhouse.jdbc.ClickHouseDriver")
                .withUsername("analytics_user").withPassword("analytics_password").build();
    }
}