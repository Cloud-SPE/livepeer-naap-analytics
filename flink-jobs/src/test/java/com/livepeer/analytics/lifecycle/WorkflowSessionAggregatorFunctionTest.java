package com.livepeer.analytics.lifecycle;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkflowSessionAggregatorFunctionTest {

    @Test
    void appliesResolvedPipelineToSignal_preventsModelLabelLeakIntoSessionPipeline() {
        LifecycleSignal signal = new LifecycleSignal();
        signal.workflowSessionId = "s|r";
        signal.signalType = LifecycleSignal.SignalType.STREAM_STATUS;
        signal.pipeline = "streamdiffusion-sdxl-v2v";
        signal.pipelineHint = "streamdiffusion-sdxl-v2v";
        signal.modelHint = "streamdiffusion-sdxl-v2v";

        PipelineModelResolver.Resolution resolved = PipelineModelResolver.resolve(
                PipelineModelResolver.Mode.LEGACY_MISNAMED,
                signal,
                "",
                "",
                null);

        WorkflowSessionAggregatorFunction.applyResolvedPipelineToSignal(signal, resolved);

        WorkflowSessionAccumulator state = new WorkflowSessionAccumulator();
        WorkflowSessionStateMachine.applySignal(state, signal);

        assertEquals("", state.pipeline);
        assertEquals("streamdiffusion-sdxl-v2v", state.modelId);
    }

    @Test
    void capabilityLookupKeyFallsBackToStateOrchestratorAddress() {
        String key = WorkflowSessionAggregatorFunction.resolveCapabilityLookupKey("", "0xAbCd");
        assertTrue(key.equals("0xabcd"));
    }
}
