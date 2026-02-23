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
