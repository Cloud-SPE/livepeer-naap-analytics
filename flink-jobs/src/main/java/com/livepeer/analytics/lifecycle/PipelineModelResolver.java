package com.livepeer.analytics.lifecycle;

import com.livepeer.analytics.util.StringSemantics;

import java.io.Serializable;
import java.util.Locale;

/**
 * Resolves canonical pipeline/model semantics from raw signal hints and capability snapshots.
 *
 * <p>Default mode (`legacy_misnamed`) expects signal `pipeline` to carry model-like labels.
 * Canonical pipeline/model fields are then sourced from matched capability snapshots.</p>
 */
public final class PipelineModelResolver {
    private PipelineModelResolver() {}

    public enum Mode {
        LEGACY_MISNAMED,
        NATIVE_CORRECT;

        static Mode fromEnv(String raw) {
            if (raw == null) {
                return LEGACY_MISNAMED;
            }
            String normalized = raw.trim().toLowerCase(Locale.ROOT);
            if ("native_correct".equals(normalized) || "native".equals(normalized)) {
                return NATIVE_CORRECT;
            }
            return LEGACY_MISNAMED;
        }
    }

    public static final class Resolution implements Serializable {
        private static final long serialVersionUID = 1L;
        public final String pipeline;
        public final String modelId;
        public final String compatibilityModelHint;

        private Resolution(String pipeline, String modelId, String compatibilityModelHint) {
            this.pipeline = pipeline;
            this.modelId = modelId;
            this.compatibilityModelHint = compatibilityModelHint;
        }
    }

    public static Mode modeFromEnvironment() {
        return Mode.fromEnv(System.getenv("LIFECYCLE_PIPELINE_MODEL_MODE"));
    }

    public static Resolution resolve(
            Mode mode,
            LifecycleSignal signal,
            String existingPipeline,
            String existingModelId,
            CapabilitySnapshotRef snapshot) {
        String signalPipelineHint = StringSemantics.firstNonBlank(signal.pipelineHint, signal.pipeline);
        String signalModelHint = StringSemantics.firstNonBlank(signal.modelHint, signalPipelineHint);
        String snapshotPipeline = snapshot == null ? "" : snapshot.pipeline;
        String snapshotModelId = snapshot == null ? "" : snapshot.modelId;

        if (mode == Mode.NATIVE_CORRECT) {
            String canonicalPipeline = StringSemantics.firstNonBlank(signal.pipeline, existingPipeline, snapshotPipeline);
            String canonicalModelId = StringSemantics.firstNonBlank(signalModelHint, existingModelId, snapshotModelId);
            String compatibilityHint = StringSemantics.firstNonBlank(signalModelHint, canonicalModelId, existingModelId);
            return new Resolution(canonicalPipeline, canonicalModelId, compatibilityHint);
        }

        String canonicalPipeline = StringSemantics.firstNonBlank(snapshotPipeline, existingPipeline);
        String canonicalModelId = StringSemantics.firstNonBlank(snapshotModelId, existingModelId, signalModelHint);
        String compatibilityHint = StringSemantics.firstNonBlank(signalModelHint, existingModelId);
        return new Resolution(canonicalPipeline, canonicalModelId, compatibilityHint);
    }
}
