package com.livepeer.analytics.lifecycle;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * Mutable workflow-session accumulator persisted in Flink keyed state.
 */
public class WorkflowSessionAccumulator implements Serializable {
    private static final long serialVersionUID = 1L;

    public String workflowSessionId;
    public String streamId = "";
    public String requestId = "";
    public String sessionId = "";
    public String pipeline = "";
    public String workflowId = "";
    public String workflowType = "ai_live_video_stream";
    public String gateway = "";
    public String orchestratorAddress = "";
    public String orchestratorUrl = "";
    public String modelId;
    public String gpuId;
    public String region;

    public String attributionMethod = "none";
    public float attributionConfidence = 0.0f;

    public long sessionStartTs = Long.MAX_VALUE;
    public long sessionEndTs = Long.MIN_VALUE;
    public Long firstStreamRequestTs;
    public Long firstProcessedTs;
    public Long firstPlayableTs;

    public boolean knownStream;
    public boolean startupSuccess;
    public boolean noOrchestratorsAvailable;
    public long errorCount;
    public long excusableErrorCount;
    public long explicitSwapCount;
    public long eventCount;
    public long version;
    public long paramsUpdateCount;

    public String sourceFirstEventUid = "";
    public String sourceLastEventUid = "";

    public final Set<String> orchestratorsSeen = new HashSet<>();
    public final Set<String> canonicalOrchestratorsSeen = new HashSet<>();
}
