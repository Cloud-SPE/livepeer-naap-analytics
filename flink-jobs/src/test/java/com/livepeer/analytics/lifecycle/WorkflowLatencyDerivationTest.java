package com.livepeer.analytics.lifecycle;

import com.livepeer.analytics.model.EventPayloads;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class WorkflowLatencyDerivationTest {

    @Test
    void derivesLatencyIntervalsFromSessionEdges() {
        EventPayloads.FactWorkflowSession s = new EventPayloads.FactWorkflowSession();
        s.workflowSessionId = "stream|request";
        s.streamId = "stream";
        s.requestId = "request";
        s.gateway = "gateway-1";
        s.orchestratorAddress = "0xorch";
        s.pipeline = "live-video-to-video";
        s.modelId = "streamdiffusion-sdxl";
        s.gpuId = "GPU-1";
        s.sessionStartTs = 1000L;
        s.firstStreamRequestTs = 1000L;
        s.firstProcessedTs = 1200L;
        s.firstPlayableTs = 4300L;
        s.version = 7L;

        EventPayloads.FactWorkflowLatencySample out = WorkflowLatencyDerivation.fromSession(s);

        assertEquals(200.0, out.promptToFirstFrameMs);
        assertEquals(3100.0, out.startupTimeMs);
        assertEquals(3300.0, out.e2eLatencyMs);
        assertEquals(1, out.hasPromptToFirstFrame);
        assertEquals(1, out.hasStartupTime);
        assertEquals(1, out.hasE2eLatency);
        assertEquals("v1", out.edgeSemanticsVersion);
        assertEquals(7L, out.version);
    }

    @Test
    void nullsInvalidOrMissingIntervals() {
        EventPayloads.FactWorkflowSession s = new EventPayloads.FactWorkflowSession();
        s.workflowSessionId = "stream|request";
        s.sessionStartTs = 1000L;
        s.firstStreamRequestTs = 2000L;
        s.firstProcessedTs = 1000L; // negative delta for prompt_to_first_frame
        s.firstPlayableTs = null;

        EventPayloads.FactWorkflowLatencySample out = WorkflowLatencyDerivation.fromSession(s);

        assertNull(out.promptToFirstFrameMs);
        assertNull(out.startupTimeMs);
        assertNull(out.e2eLatencyMs);
        assertEquals(0, out.hasPromptToFirstFrame);
        assertEquals(0, out.hasStartupTime);
        assertEquals(0, out.hasE2eLatency);
    }
}
