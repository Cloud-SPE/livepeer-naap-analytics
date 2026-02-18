package com.livepeer.analytics.lifecycle;

import java.io.Serializable;

/**
 * Keyed context used to emit parameter-update lifecycle markers with stream/session attribution.
 */
public class WorkflowParamUpdateAccumulator implements Serializable {
    private static final long serialVersionUID = 1L;

    public String workflowSessionId = "";
    public String streamId = "";
    public String requestId = "";
    public String pipeline = "";
    public String pipelineId = "";
    public String gateway = "";
    public String orchestratorAddress = "";
    public String orchestratorUrl = "";
    public String modelId;
    public String gpuId;
    public String attributionMethod = "none";
    public float attributionConfidence = 0.0f;
    public long version = 0L;
}
