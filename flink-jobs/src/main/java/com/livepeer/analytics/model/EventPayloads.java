package com.livepeer.analytics.model;

public final class EventPayloads {
    private EventPayloads() {}

    public static class AiStreamStatus implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        public long eventTimestamp;
        public String streamId, requestId, gateway, orchestratorAddress, orchestratorUrl;
        public String pipeline, pipelineId, state, paramsHash, lastError, promptText;
        public float outputFps, inputFps;
        public int restartCount, promptWidth, promptHeight;
        public Long startTime;
        public String rawJson;
    }

    public static class StreamIngestMetrics implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        public long eventTimestamp;
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
        public long eventTimestamp;
        public long dataTimestamp;
        public String streamId, requestId, pipelineId, orchestratorAddress, orchestratorUrl, traceType;
        public String rawJson;
    }

    public static class NetworkCapability implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        public long eventTimestamp;
        public String sourceEventId;
        public String orchestratorAddress, localAddress, orchUri, gpuId, gpuName;
        public String pipeline, modelId, runnerVersion, orchestratorVersion;
        public Integer capabilityId;
        public String capabilityName, capabilityGroup, capabilityCatalogVersion;
        public Long gpuMemoryTotal, gpuMemoryFree;
        public Integer gpuMajor, gpuMinor, capacity, capacityInUse, pricePerUnit, pixelsPerUnit;
        public Integer warm;
        public String rawJson;
    }

    public static class NetworkCapabilityAdvertised implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        public long eventTimestamp;
        public String sourceEventId;
        public String orchestratorAddress, localAddress, orchUri;
        public Integer capabilityId, capacity;
        public String capabilityName, capabilityGroup, capabilityCatalogVersion;
        public String rawJson;
    }

    public static class NetworkCapabilityModelConstraint implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        public long eventTimestamp;
        public String sourceEventId;
        public String orchestratorAddress, localAddress, orchUri;
        public Integer capabilityId, capacity, capacityInUse, warm;
        public String capabilityName, capabilityGroup, capabilityCatalogVersion, modelId, runnerVersion;
        public String rawJson;
    }

    public static class NetworkCapabilityPrice implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        public long eventTimestamp;
        public String sourceEventId;
        public String orchestratorAddress, localAddress, orchUri;
        public Integer capabilityId, pricePerUnit, pixelsPerUnit;
        public String capabilityName, capabilityGroup, capabilityCatalogVersion, constraint;
        public String rawJson;
    }

    public static class AiStreamEvent implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        public long eventTimestamp;
        public String streamId, requestId, pipeline, pipelineId, eventType, message, capability;
        public String rawJson;
    }

    public static class DiscoveryResult implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        public long eventTimestamp;
        public String orchestratorAddress, orchestratorUrl;
        public int latencyMs;
        public String rawJson;
    }

    public static class PaymentEvent implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        public long eventTimestamp;
        public String requestId, sessionId, manifestId, sender, recipient, orchestrator;
        public String faceValue, price, numTickets, winProb, clientIp, capability;
        public String rawJson;
    }

}
