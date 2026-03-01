package com.livepeer.analytics.lifecycle;

import java.io.Serializable;

/**
 * Lookup record used to enrich lifecycle facts with canonical orchestrator identity and capability metadata.
 */
public class CapabilitySnapshotRef implements Serializable {
    private static final long serialVersionUID = 1L;

    public long snapshotTs;
    public String canonicalOrchestratorAddress;
    public String orchestratorUrl;
    public String pipeline;
    public String modelId;
    public String gpuId;
}
