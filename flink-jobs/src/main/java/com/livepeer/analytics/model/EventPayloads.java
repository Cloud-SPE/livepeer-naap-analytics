package com.livepeer.analytics.model;

public final class EventPayloads {
    private EventPayloads() {}

    public static class AiStreamStatus implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        public long eventTimestamp;
        public String streamId, requestId, gateway, orchestratorAddress, orchestratorUrl;
        public String pipeline, state, paramsHash, lastError, promptText;
        public float outputFps, inputFps;
        public int restartCount, promptWidth, promptHeight;
        public Long startTime, lastErrorTime;
        public String rawJson;
    }

    public static class StreamIngestMetrics implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        public long eventTimestamp;
        public String streamId, requestId, connectionQuality;
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
        public String streamId, requestId, orchestratorAddress, orchestratorUrl, traceType;
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
        public String streamId, requestId, pipeline, eventType, message, capability;
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

    /**
     * Stateful lifecycle fact at workflow-session grain.
     */
    public static class FactWorkflowSession implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        public String workflowSessionId, workflowType, workflowId;
        public String streamId, requestId, sessionId, pipeline;
        public String gateway, orchestratorAddress, orchestratorUrl;
        public String modelId, gpuId, region;
        public String attributionMethod;
        public float attributionConfidence;

        public long sessionStartTs;
        public Long sessionEndTs;
        public int knownStream, startupSuccess, startupExcused, startupUnexcused;
        public int swapCount;
        public long errorCount, excusableErrorCount;
        public Long firstStreamRequestTs, firstProcessedTs, firstPlayableTs;
        public long eventCount, version;
        public String sourceFirstEventUid, sourceLastEventUid;
    }

    /**
     * Stateful lifecycle segment fact at workflow-session + segment-index grain.
     */
    public static class FactWorkflowSessionSegment implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        public String workflowSessionId;
        public int segmentIndex;
        public long segmentStartTs;
        public Long segmentEndTs;

        public String gateway;
        public String orchestratorAddress;
        public String orchestratorUrl;
        public String workerId;
        public String gpuId;
        public String modelId;
        public String region;
        public String attributionMethod;
        public float attributionConfidence;

        public String reason;
        public String sourceTraceType;
        public String sourceEventUid;
        public long version;
    }

    /**
     * Lifecycle marker for parameter updates (prompt/controlnet changes).
     */
    public static class FactWorkflowParamUpdate implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        public long updateTs;
        public String workflowSessionId;
        public String streamId;
        public String requestId;
        public String pipeline;
        public String gateway;
        public String orchestratorAddress;
        public String orchestratorUrl;
        public String modelId;
        public String gpuId;
        public String attributionMethod;
        public float attributionConfidence;
        public String updateType;
        public String message;
        public String sourceEventUid;
        public long version;
    }

    /**
     * Lifecycle edge-coverage diagnostic fact at workflow-session signal grain.
     *
     * <p>Purpose:
     * - Surface unmatched/missing edge states for correlation observability.
     * - Enable operational queries to track edge-pair completeness over time.
     * </p>
     */
    public static class FactLifecycleEdgeCoverage implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        public long signalTs;
        public String workflowSessionId;
        public String streamId;
        public String requestId;
        public String pipeline;
        public String gateway;
        public String orchestratorAddress;
        public String traceType;
        public String sourceEventUid;

        public int knownStream;
        public int hasFirstProcessedEdge;
        public int hasFirstPlayableEdge;
        public int startupEdgeMatched;
        public int playableEdgeMatched;
        public int isTerminalSignal;
        public String unmatchedReason;
        public long version;
    }

    /**
     * Derived latency KPI fact at workflow-session snapshot grain.
     *
     * <p>Produced by Flink from deterministic session-edge timestamps so pairing semantics
     * are versioned in code, not ad hoc SQL.</p>
     */
    public static class FactWorkflowLatencySample implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        public long sampleTs;
        public String workflowSessionId;
        public String streamId;
        public String requestId;
        public String gateway;
        public String orchestratorAddress;
        public String pipeline;
        public String modelId;
        public String gpuId;
        public String region;

        public Double promptToFirstFrameMs;
        public Double startupTimeMs;
        public Double e2eLatencyMs;

        public int hasPromptToFirstFrame;
        public int hasStartupTime;
        public int hasE2eLatency;

        public String edgeSemanticsVersion;
        public long version;
    }

}
