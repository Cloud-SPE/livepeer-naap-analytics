package com.livepeer.analytics.lifecycle;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CapabilityAttributionSelectorTest {
    private static final long TTL_MS = 24L * 60L * 60L * 1000L;

    @Test
    void selectsCompatibleCandidateInsteadOfIncompatibleLatest() {
        CapabilitySnapshotBucket bucket = new CapabilitySnapshotBucket();

        CapabilityAttributionSelector.upsertCandidate(
                bucket, ref(1_000L, "0xcanonical", "meta-llama/Meta-Llama-3.1-8B-Instruct", null), 1_000L, TTL_MS, 8);
        CapabilityAttributionSelector.upsertCandidate(
                bucket, ref(2_000L, "0xcanonical", "streamdiffusion-sdxl", "GPU-1"), 2_000L, TTL_MS, 8);

        CapabilitySnapshotRef selected =
                CapabilityAttributionSelector.selectBestCandidate(bucket, "streamdiffusion-sdxl", 2_500L, TTL_MS);

        assertNotNull(selected);
        assertEquals("streamdiffusion-sdxl", selected.modelId);
        assertEquals("GPU-1", selected.gpuId);
    }

    @Test
    void prefersNearestPriorWithinTtlOverNearestFutureWithinTtl() {
        CapabilitySnapshotBucket bucket = new CapabilitySnapshotBucket();

        CapabilityAttributionSelector.upsertCandidate(
                bucket, ref(9_000L, "0xcanonical", "streamdiffusion-sdxl", "GPU-prior"), 9_000L, TTL_MS, 8);
        CapabilityAttributionSelector.upsertCandidate(
                bucket, ref(11_000L, "0xcanonical", "streamdiffusion-sdxl", "GPU-future"), 11_000L, TTL_MS, 8);

        CapabilitySnapshotRef selected =
                CapabilityAttributionSelector.selectBestCandidate(bucket, "streamdiffusion-sdxl", 10_000L, TTL_MS);

        assertNotNull(selected);
        assertEquals("GPU-prior", selected.gpuId);
    }

    @Test
    void returnsNullWhenNoCompatibleCandidateExists() {
        CapabilitySnapshotBucket bucket = new CapabilitySnapshotBucket();
        CapabilityAttributionSelector.upsertCandidate(
                bucket, ref(1_000L, "0xcanonical", "meta-llama/Meta-Llama-3.1-8B-Instruct", null), 1_000L, TTL_MS, 8);

        CapabilitySnapshotRef selected =
                CapabilityAttributionSelector.selectBestCandidate(bucket, "streamdiffusion-sdxl", 2_000L, TTL_MS);
        assertNull(selected);
    }

    @Test
    void enforcesBucketBoundAndPrunesVeryStaleCandidates() {
        CapabilitySnapshotBucket bucket = new CapabilitySnapshotBucket();

        CapabilityAttributionSelector.upsertCandidate(
                bucket, ref(0L, "0xcanonical", "old-model", null), 5L * TTL_MS, TTL_MS, 3);
        CapabilityAttributionSelector.upsertCandidate(
                bucket, ref(5L * TTL_MS - 20L, "0xcanonical", "model-a", null), 5L * TTL_MS, TTL_MS, 3);
        CapabilityAttributionSelector.upsertCandidate(
                bucket, ref(5L * TTL_MS - 10L, "0xcanonical", "model-b", null), 5L * TTL_MS, TTL_MS, 3);
        CapabilityAttributionSelector.upsertCandidate(
                bucket, ref(5L * TTL_MS, "0xcanonical", "model-c", null), 5L * TTL_MS, TTL_MS, 3);
        CapabilityAttributionSelector.upsertCandidate(
                bucket, ref(5L * TTL_MS + 10L, "0xcanonical", "model-d", null), 5L * TTL_MS + 10L, TTL_MS, 3);

        assertTrue(bucket.byModelKey.size() <= 3);
        assertTrue(bucket.byModelKey.keySet().stream().noneMatch(k -> k.contains("old-model")));
    }

    @Test
    void prodRegression_streamdiffusionSignalMustNotSelectLlamaCandidate() {
        // Why this test exists:
        // In production (2026-02-20 UTC window), hot wallet 0x52cf... emitted interleaved
        // capability snapshots for streamdiffusion and llama models. Session rows later showed
        // pipeline=streamdiffusion-sdxl with model_id=meta-llama. This test locks the intended
        // selector outcome when pipeline context is streamdiffusion: pick streamdiffusion only.
        CapabilitySnapshotBucket bucket = new CapabilitySnapshotBucket();

        CapabilityAttributionSelector.upsertCandidate(
                bucket,
                ref(1_739_040_506_612L, "0xd00354656922168815fcd1e51cbddb9e359e3c7f",
                        "streamdiffusion-sdxl", "GPU-6f1bacf5-d5be-9cad-aa38-e35f905daebe"),
                1_739_040_506_612L,
                TTL_MS,
                32);
        CapabilityAttributionSelector.upsertCandidate(
                bucket,
                ref(1_739_040_509_903L, "0xd00354656922168815fcd1e51cbddb9e359e3c7f",
                        "meta-llama/Meta-Llama-3.1-8B-Instruct", null),
                1_739_040_509_903L,
                TTL_MS,
                32);
        CapabilityAttributionSelector.upsertCandidate(
                bucket,
                ref(1_739_040_519_000L, "0xd00354656922168815fcd1e51cbddb9e359e3c7f",
                        "streamdiffusion-sdxl", "GPU-6f1bacf5-d5be-9cad-aa38-e35f905daebe"),
                1_739_040_519_000L,
                TTL_MS,
                32);

        CapabilitySnapshotRef selected = CapabilityAttributionSelector.selectBestCandidate(
                bucket,
                "streamdiffusion-sdxl",
                1_739_041_800_304L,
                TTL_MS);

        assertNotNull(selected);
        assertEquals("streamdiffusion-sdxl", selected.modelId);
        assertEquals("GPU-6f1bacf5-d5be-9cad-aa38-e35f905daebe", selected.gpuId);
    }

    @Test
    void prodRegression_reproducesPrefixDriftAndValidatesPipelineFallbackFix() {
        // Why this test exists:
        // Before hardening, aggregator selection used signal.pipeline directly. For many trace
        // signals this is empty in prod, which let selector choose the nearest (often llama)
        // model from the same hot-wallet candidate bucket. After hardening, selection uses
        // firstNonEmpty(signal.pipeline, existingSessionPipeline), which should keep
        // streamdiffusion attribution stable for streamdiffusion sessions.
        CapabilitySnapshotBucket bucket = new CapabilitySnapshotBucket();

        // Prod-derived pattern: same hot wallet, mixed models in near snapshots.
        CapabilityAttributionSelector.upsertCandidate(
                bucket,
                ref(1_739_040_506_612L, "0xd00354656922168815fcd1e51cbddb9e359e3c7f",
                        "streamdiffusion-sdxl", "GPU-6f1bacf5-d5be-9cad-aa38-e35f905daebe"),
                1_739_040_506_612L,
                TTL_MS,
                32);
        CapabilityAttributionSelector.upsertCandidate(
                bucket,
                ref(1_739_040_509_903L, "0xd00354656922168815fcd1e51cbddb9e359e3c7f",
                        "meta-llama/Meta-Llama-3.1-8B-Instruct", null),
                1_739_040_509_903L,
                TTL_MS,
                32);

        long traceSignalTs = 1_739_040_510_000L;

        // Simulates pre-fix behavior: signal pipeline is empty for this signal type.
        CapabilitySnapshotRef selectedPreFix = CapabilityAttributionSelector.selectBestCandidate(
                bucket, "", traceSignalTs, TTL_MS);
        assertNotNull(selectedPreFix);
        assertEquals("meta-llama/Meta-Llama-3.1-8B-Instruct", selectedPreFix.modelId);

        // Simulates hardened behavior: fallback to existing session pipeline.
        CapabilitySnapshotRef selectedWithFix = CapabilityAttributionSelector.selectBestCandidate(
                bucket, "streamdiffusion-sdxl", traceSignalTs, TTL_MS);
        assertNotNull(selectedWithFix);
        assertEquals("streamdiffusion-sdxl", selectedWithFix.modelId);
        assertEquals("GPU-6f1bacf5-d5be-9cad-aa38-e35f905daebe", selectedWithFix.gpuId);
    }

    @Test
    void prodRegression_incompatibleFreshSnapshotMustNotBeSelected() {
        // Why this test exists:
        // Another failure mode from the same prod pattern is "freshness wins over compatibility":
        // a near-in-time llama snapshot could be picked for an existing streamdiffusion session.
        // This test enforces compatibility-first behavior even when incompatible snapshot is newer.
        CapabilitySnapshotBucket bucket = new CapabilitySnapshotBucket();

        CapabilityAttributionSelector.upsertCandidate(
                bucket,
                ref(1_739_040_509_903L, "0xd00354656922168815fcd1e51cbddb9e359e3c7f",
                        "meta-llama/Meta-Llama-3.1-8B-Instruct", null),
                1_739_040_509_903L,
                TTL_MS,
                32);

        CapabilitySnapshotRef selected = CapabilityAttributionSelector.selectBestCandidate(
                bucket,
                "streamdiffusion-sdxl",
                1_739_040_510_000L,
                TTL_MS);

        assertNull(selected);
    }

    private static CapabilitySnapshotRef ref(
            long snapshotTs,
            String canonicalAddress,
            String modelId,
            String gpuId) {
        CapabilitySnapshotRef ref = new CapabilitySnapshotRef();
        ref.snapshotTs = snapshotTs;
        ref.canonicalOrchestratorAddress = canonicalAddress;
        ref.modelId = modelId;
        ref.gpuId = gpuId;
        return ref;
    }
}
