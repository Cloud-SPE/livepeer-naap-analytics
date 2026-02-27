package com.livepeer.analytics.lifecycle;

import com.livepeer.analytics.model.EventPayloads;

/**
 * Deterministic latency-derivation rules from workflow-session edge timestamps.
 *
 * <p>Semantics (v1):
 * - prompt_to_first_frame_ms: first_stream_request_ts -> first_processed_ts
 * - startup_time_ms: first_processed_ts -> first_playable_ts
 * - e2e_latency_ms: first_stream_request_ts -> first_playable_ts
 *
 * <p>Invalid intervals (negative deltas) are emitted as null and flagged as missing.
 */
public final class WorkflowLatencyDerivation {
    public static final String EDGE_SEMANTICS_VERSION_V1 = "v1";

    private WorkflowLatencyDerivation() {}

    public static EventPayloads.FactWorkflowLatencySample fromSession(EventPayloads.FactWorkflowSession session) {
        EventPayloads.FactWorkflowLatencySample sample = new EventPayloads.FactWorkflowLatencySample();
        sample.sampleTs = session.sessionStartTs;
        sample.workflowSessionId = session.workflowSessionId;
        sample.streamId = session.streamId;
        sample.requestId = session.requestId;
        sample.gateway = session.gateway;
        sample.orchestratorAddress = session.orchestratorAddress;
        sample.pipeline = session.pipeline;
        sample.modelId = session.modelId;
        sample.gpuId = session.gpuId;
        sample.region = session.region;

        sample.promptToFirstFrameMs = deriveMs(session.firstStreamRequestTs, session.firstProcessedTs);
        sample.startupTimeMs = deriveMs(session.firstProcessedTs, session.firstPlayableTs);
        sample.e2eLatencyMs = deriveMs(session.firstStreamRequestTs, session.firstPlayableTs);
        sample.hasPromptToFirstFrame = sample.promptToFirstFrameMs == null ? 0 : 1;
        sample.hasStartupTime = sample.startupTimeMs == null ? 0 : 1;
        sample.hasE2eLatency = sample.e2eLatencyMs == null ? 0 : 1;
        sample.edgeSemanticsVersion = EDGE_SEMANTICS_VERSION_V1;
        sample.version = session.version;
        return sample;
    }

    static Double deriveMs(Long startTs, Long endTs) {
        if (startTs == null || endTs == null) {
            return null;
        }
        long delta = endTs - startTs;
        if (delta < 0L) {
            return null;
        }
        return (double) delta;
    }
}
