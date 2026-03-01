package com.livepeer.analytics.lifecycle;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PipelineModelResolverTest {

    @Test
    void legacyModeUsesCapabilityPipelineAndModel() {
        LifecycleSignal signal = new LifecycleSignal();
        signal.pipelineHint = "streamdiffusion-sdxl";
        signal.modelHint = "streamdiffusion-sdxl";

        CapabilitySnapshotRef snapshot = new CapabilitySnapshotRef();
        snapshot.pipeline = "live-video-to-video";
        snapshot.modelId = "streamdiffusion-sdxl";

        PipelineModelResolver.Resolution resolved = PipelineModelResolver.resolve(
                PipelineModelResolver.Mode.LEGACY_MISNAMED,
                signal,
                "",
                "",
                snapshot);

        assertEquals("live-video-to-video", resolved.pipeline);
        assertEquals("streamdiffusion-sdxl", resolved.modelId);
        assertEquals("streamdiffusion-sdxl", resolved.compatibilityModelHint);
    }

    @Test
    void nativeModePrefersSignalPipelineButUsesHintForModelCompatibility() {
        LifecycleSignal signal = new LifecycleSignal();
        signal.pipeline = "text-to-image";
        signal.modelHint = "SG161222/RealVisXL_V4.0_Lightning";

        CapabilitySnapshotRef snapshot = new CapabilitySnapshotRef();
        snapshot.pipeline = "live-video-to-video";
        snapshot.modelId = "streamdiffusion-sdxl";

        PipelineModelResolver.Resolution resolved = PipelineModelResolver.resolve(
                PipelineModelResolver.Mode.NATIVE_CORRECT,
                signal,
                "",
                "",
                snapshot);

        assertEquals("text-to-image", resolved.pipeline);
        assertEquals("SG161222/RealVisXL_V4.0_Lightning", resolved.modelId);
        assertEquals("SG161222/RealVisXL_V4.0_Lightning", resolved.compatibilityModelHint);
    }
}
