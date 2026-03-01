package com.livepeer.analytics.lifecycle;

import com.livepeer.analytics.model.StreamingEvent;

import java.io.Serializable;

/**
 * Normalized lifecycle signal consumed by workflow sessionization.
 */
public class LifecycleSignal implements Serializable {
    private static final long serialVersionUID = 1L;

    public enum SignalType {
        STREAM_STATUS,
        STREAM_TRACE,
        AI_STREAM_EVENT
    }

    public SignalType signalType;
    public String workflowSessionId;
    public long signalTimestamp;
    public long ingestTimestamp;

    public String streamId;
    public String requestId;
    // Canonical workflow pipeline label used by downstream lifecycle facts.
    public String pipeline;
    // Raw/transport hint fields used by compatibility resolver.
    public String pipelineHint;
    public String modelHint;
    public String gateway;
    public String orchestratorAddress;
    public String orchestratorUrl;
    public String traceType;
    public String aiEventType;
    public String message;
    public Long startTimeMs;

    public String sourceEventUid;
    public StreamingEvent sourceEvent;
}
