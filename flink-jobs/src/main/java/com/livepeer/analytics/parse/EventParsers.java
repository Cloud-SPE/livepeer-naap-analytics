package com.livepeer.analytics.parse;

import com.fasterxml.jackson.databind.JsonNode;

import com.livepeer.analytics.model.EventPayloads;
import com.livepeer.analytics.quality.ValidatedEvent;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Parses validated events into typed payloads for ClickHouse ingestion.
 */
public final class EventParsers {
    private EventParsers() {}

    public static List<EventPayloads.AiStreamStatus> parseAiStreamStatus(ValidatedEvent e) throws Exception {
        JsonNode data = requireData(e, "ai_stream_status");
        EventPayloads.AiStreamStatus s = new EventPayloads.AiStreamStatus();

        s.eventTimestamp = e.event.timestamp;
        s.streamId = data.path("stream_id").asText("");
        s.requestId = data.path("request_id").asText("");
        s.gateway = e.event.gateway;

        JsonNode orchInfo = data.path("orchestrator_info");
        s.orchestratorAddress = orchInfo.path("address").asText("");
        s.orchestratorUrl = orchInfo.path("url").asText("");

        s.pipeline = data.path("pipeline").asText("");
        s.pipelineId = data.path("pipeline_id").asText("");
        s.state = data.path("state").asText("");

        JsonNode inferStatus = data.path("inference_status");
        s.outputFps = (float) inferStatus.path("fps").asDouble(0.0);
        s.restartCount = inferStatus.path("restart_count").asInt(0);
        s.lastError = JsonNodeUtils.asNullableText(inferStatus.path("last_error"));
        s.paramsHash = inferStatus.path("last_params_hash").asText("");

        JsonNode lastParams = inferStatus.path("last_params");
        s.promptText = JsonNodeUtils.asNullableText(lastParams.path("prompt"));
        s.promptWidth = lastParams.path("width").asInt(0);
        s.promptHeight = lastParams.path("height").asInt(0);

        JsonNode inputStatus = data.path("input_status");
        s.inputFps = (float) inputStatus.path("fps").asDouble(0.0);

        s.startTime = JsonNodeUtils.parseTimestampMillis(data.path("start_time"));
        s.rawJson = e.event.rawJson;

        List<EventPayloads.AiStreamStatus> results = new ArrayList<>(1);
        results.add(s);
        return results;
    }

    public static List<EventPayloads.StreamIngestMetrics> parseStreamIngestMetrics(ValidatedEvent e) throws Exception {
        JsonNode data = requireData(e, "stream_ingest_metrics");
        EventPayloads.StreamIngestMetrics m = new EventPayloads.StreamIngestMetrics();

        m.eventTimestamp = e.event.timestamp;
        m.streamId = data.path("stream_id").asText("");
        m.requestId = data.path("request_id").asText("");
        m.pipelineId = data.path("pipeline_id").asText("");

        JsonNode stats = data.path("stats");
        m.connectionQuality = stats.path("conn_quality").asText("");

        JsonNode peerStats = stats.path("peer_conn_stats");
        m.bytesReceived = JsonNodeUtils.asLongOrDefault(peerStats.path("BytesReceived"), 0L);
        m.bytesSent = JsonNodeUtils.asLongOrDefault(peerStats.path("BytesSent"), 0L);

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

        m.rawJson = e.event.rawJson;
        List<EventPayloads.StreamIngestMetrics> results = new ArrayList<>(1);
        results.add(m);
        return results;
    }

    public static List<EventPayloads.StreamTraceEvent> parseStreamTrace(ValidatedEvent e) throws Exception {
        JsonNode data = requireData(e, "stream_trace");
        EventPayloads.StreamTraceEvent t = new EventPayloads.StreamTraceEvent();

        t.eventTimestamp = e.event.timestamp;
        t.streamId = data.path("stream_id").asText("");
        t.requestId = data.path("request_id").asText("");
        t.pipelineId = data.path("pipeline_id").asText("");
        t.traceType = data.path("type").asText("");

        JsonNode orchInfo = data.path("orchestrator_info");
        t.orchestratorAddress = orchInfo.path("address").asText("");
        t.orchestratorUrl = orchInfo.path("url").asText("");

        t.dataTimestamp = JsonNodeUtils.parseTimestampMillisOrDefault(data.path("timestamp"), e.event.timestamp);
        t.rawJson = e.event.rawJson;

        List<EventPayloads.StreamTraceEvent> results = new ArrayList<>(1);
        results.add(t);
        return results;
    }

    public static List<EventPayloads.NetworkCapability> parseNetworkCapabilities(ValidatedEvent e) throws Exception {
        JsonNode dataArray = e.data();
        if (dataArray == null || !dataArray.isArray() || dataArray.size() == 0) {
            throw new Exception("No valid orchestrator data found");
        }

        List<EventPayloads.NetworkCapability> results = new ArrayList<>();

        // Emit one row per orchestrator × hardware entry × GPU (if present).
        for (JsonNode orch : dataArray) {
            String orchestratorAddress = orch.path("address").asText("");
            String localAddress = orch.path("local_address").asText("");
            String orchUri = orch.path("orch_uri").asText("");
            String orchestratorVersion = orch.path("capabilities").path("version").asText("");

            JsonNode constraintsByCap = orch.path("capabilities").path("constraints").path("PerCapability");
            JsonNode priceArray = orch.path("capabilities_prices");
            JsonNode hardwareArray = orch.path("hardware");

            if (!hardwareArray.isArray() || hardwareArray.size() == 0) {
                continue;
            }

            for (JsonNode hw : hardwareArray) {
                String pipeline = hw.path("pipeline").asText("");
                String modelId = hw.path("model_id").asText("");

                ModelSelection modelSelection = selectModelConfig(constraintsByCap, modelId);
                JsonNode modelCfg = modelSelection.modelCfg;
                Integer capabilityId = modelSelection.capabilityId;

                JsonNode priceInfo = selectPrice(priceArray, capabilityId, modelId);

                Integer warm = null;
                if (modelCfg != null && !modelCfg.isMissingNode() && !modelCfg.isNull()) {
                    JsonNode warmNode = modelCfg.path("warm");
                    if (warmNode.isBoolean()) {
                        warm = warmNode.asBoolean(false) ? 1 : 0;
                    } else {
                        warm = JsonNodeUtils.asNullableInt(warmNode);
                    }
                }

                JsonNode gpuInfo = hw.path("gpu_info");
                if (gpuInfo.isObject() && gpuInfo.size() > 0) {
                    Iterator<String> gpuKeys = gpuInfo.fieldNames();
                    while (gpuKeys.hasNext()) {
                        String gpuKey = gpuKeys.next();
                        JsonNode gpu = gpuInfo.path(gpuKey);
                        EventPayloads.NetworkCapability n = new EventPayloads.NetworkCapability();
                        n.eventTimestamp = e.event.timestamp;
                        n.orchestratorAddress = orchestratorAddress;
                        n.localAddress = localAddress;
                        n.orchUri = orchUri;
                        n.gpuId = JsonNodeUtils.asNullableText(gpu.path("id"));
                        n.gpuName = JsonNodeUtils.asNullableText(gpu.path("name"));
                        n.gpuMemoryTotal = JsonNodeUtils.asNullableLong(gpu.path("memory_total"));
                        n.gpuMemoryFree = JsonNodeUtils.asNullableLong(gpu.path("memory_free"));
                        n.gpuMajor = JsonNodeUtils.asNullableInt(gpu.path("major"));
                        n.gpuMinor = JsonNodeUtils.asNullableInt(gpu.path("minor"));
                        n.pipeline = pipeline;
                        n.modelId = modelId;
                        n.runnerVersion = modelCfg == null ? null : JsonNodeUtils.asNullableText(modelCfg.path("runnerVersion"));
                        n.capacity = modelCfg == null ? null : JsonNodeUtils.asNullableInt(modelCfg.path("capacity"));
                        n.capacityInUse = modelCfg == null ? null : JsonNodeUtils.asNullableInt(modelCfg.path("capacityInUse"));
                        n.warm = warm;
                        n.pricePerUnit = priceInfo == null ? null : JsonNodeUtils.asNullableInt(priceInfo.path("pricePerUnit"));
                        n.pixelsPerUnit = priceInfo == null ? null : JsonNodeUtils.asNullableInt(priceInfo.path("pixelsPerUnit"));
                        n.orchestratorVersion = orchestratorVersion;
                        n.rawJson = orch.toString();
                        results.add(n);
                    }
                } else {
                    EventPayloads.NetworkCapability n = new EventPayloads.NetworkCapability();
                    n.eventTimestamp = e.event.timestamp;
                    n.orchestratorAddress = orchestratorAddress;
                    n.localAddress = localAddress;
                    n.orchUri = orchUri;
                    n.pipeline = pipeline;
                    n.modelId = modelId;
                    n.runnerVersion = modelCfg == null ? null : JsonNodeUtils.asNullableText(modelCfg.path("runnerVersion"));
                    n.capacity = modelCfg == null ? null : JsonNodeUtils.asNullableInt(modelCfg.path("capacity"));
                    n.capacityInUse = modelCfg == null ? null : JsonNodeUtils.asNullableInt(modelCfg.path("capacityInUse"));
                    n.warm = warm;
                    n.pricePerUnit = priceInfo == null ? null : JsonNodeUtils.asNullableInt(priceInfo.path("pricePerUnit"));
                    n.pixelsPerUnit = priceInfo == null ? null : JsonNodeUtils.asNullableInt(priceInfo.path("pixelsPerUnit"));
                    n.orchestratorVersion = orchestratorVersion;
                    n.rawJson = orch.toString();
                    results.add(n);
                }
            }
        }

        if (results.isEmpty()) {
            throw new Exception("No valid orchestrator data found");
        }
        return results;
    }

    public static List<EventPayloads.AiStreamEvent> parseAiStreamEvent(ValidatedEvent e) throws Exception {
        JsonNode data = requireData(e, "ai_stream_events");
        EventPayloads.AiStreamEvent evt = new EventPayloads.AiStreamEvent();

        evt.eventTimestamp = e.event.timestamp;
        evt.streamId = data.path("stream_id").asText("");
        evt.requestId = data.path("request_id").asText("");
        evt.pipeline = data.path("pipeline").asText("");
        evt.pipelineId = data.path("pipeline_id").asText("");
        evt.eventType = data.path("type").asText("");
        evt.message = data.path("message").asText("");
        evt.capability = data.path("capability").asText("");
        evt.rawJson = e.event.rawJson;

        List<EventPayloads.AiStreamEvent> results = new ArrayList<>(1);
        results.add(evt);
        return results;
    }

    public static List<EventPayloads.DiscoveryResult> parseDiscoveryResults(ValidatedEvent e) throws Exception {
        JsonNode dataArray = e.data();
        if (dataArray == null || !dataArray.isArray() || dataArray.size() == 0) {
            throw new Exception("No valid discovery data found");
        }

        List<EventPayloads.DiscoveryResult> results = new ArrayList<>();
        for (JsonNode disc : dataArray) {
            EventPayloads.DiscoveryResult d = new EventPayloads.DiscoveryResult();
            d.eventTimestamp = e.event.timestamp;
            d.orchestratorAddress = disc.path("address").asText("");
            d.orchestratorUrl = disc.path("url").asText("");
            d.latencyMs = JsonNodeUtils.asIntOrDefault(disc.path("latency_ms"), 0);
            d.rawJson = disc.toString();
            results.add(d);
        }

        if (results.isEmpty()) {
            throw new Exception("No valid discovery data found");
        }
        return results;
    }

    public static List<EventPayloads.PaymentEvent> parsePaymentEvent(ValidatedEvent e) throws Exception {
        JsonNode data = requireData(e, "create_new_payment");
        EventPayloads.PaymentEvent p = new EventPayloads.PaymentEvent();

        p.eventTimestamp = e.event.timestamp;
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
        p.rawJson = e.event.rawJson;

        List<EventPayloads.PaymentEvent> results = new ArrayList<>(1);
        results.add(p);
        return results;
    }

    private static JsonNode requireData(ValidatedEvent e, String eventType) throws Exception {
        JsonNode data = e.data();
        if (data == null || data.isMissingNode() || data.isNull()) {
            throw new Exception("Missing data payload for " + eventType);
        }
        return data;
    }

    private static ModelSelection selectModelConfig(JsonNode constraintsByCap, String modelId) {
        if (constraintsByCap != null && constraintsByCap.isObject()) {
            Iterator<String> capIds = constraintsByCap.fieldNames();
            while (capIds.hasNext()) {
                String capId = capIds.next();
                JsonNode models = constraintsByCap.path(capId).path("models");
                if (models.isObject()) {
                    if (modelId != null && !modelId.isEmpty() && models.has(modelId)) {
                        return new ModelSelection(models.path(modelId), parseCapId(capId));
                    }
                }
            }
            // fallback to first model in first capability
            capIds = constraintsByCap.fieldNames();
            if (capIds.hasNext()) {
                String capId = capIds.next();
                JsonNode models = constraintsByCap.path(capId).path("models");
                if (models.isObject()) {
                    Iterator<String> modelNames = models.fieldNames();
                    if (modelNames.hasNext()) {
                        String modelName = modelNames.next();
                        return new ModelSelection(models.path(modelName), parseCapId(capId));
                    }
                }
            }
        }
        return new ModelSelection(null, null);
    }

    private static Integer parseCapId(String capId) {
        if (capId == null) {
            return null;
        }
        try {
            return Integer.parseInt(capId);
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    private static JsonNode selectPrice(JsonNode priceArray, Integer capabilityId, String modelId) {
        if (priceArray == null || !priceArray.isArray() || priceArray.size() == 0) {
            return null;
        }
        JsonNode fallback = priceArray.get(0);
        for (JsonNode price : priceArray) {
            Integer cap = JsonNodeUtils.asNullableInt(price.path("capability"));
            String constraint = price.path("constraint").asText(null);
            boolean capMatches = capabilityId == null || capabilityId.equals(cap);
            boolean modelMatches = modelId == null || modelId.isEmpty() || modelId.equals(constraint);
            if (capMatches && modelMatches) {
                return price;
            }
        }
        return fallback;
    }

    private static final class ModelSelection {
        private final JsonNode modelCfg;
        private final Integer capabilityId;

        private ModelSelection(JsonNode modelCfg, Integer capabilityId) {
            this.modelCfg = modelCfg;
            this.capabilityId = capabilityId;
        }
    }
}
