package com.livepeer.analytics.lifecycle;

import java.io.Serializable;

/**
 * Mutable keyed state for workflow session segment emission.
 */
public class WorkflowSessionSegmentAccumulator implements Serializable {
    private static final long serialVersionUID = 1L;

    public String workflowSessionId = "";
    public int segmentIndex = 0;
    public boolean openSegment;

    public long segmentStartTs;
    public Long segmentEndTs;
    public String gateway = "";
    public String orchestratorAddress = "";
    public String orchestratorUrl = "";
    public String workerId = "";
    public String gpuId;
    public String modelId;
    public String region;
    public String attributionMethod = "none";
    public float attributionConfidence = 0.0f;
    public String reason = "initial";
    public String sourceTraceType = "";
    public String sourceEventUid = "";
    public long version = 0L;
}

