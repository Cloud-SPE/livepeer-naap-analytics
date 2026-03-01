package com.livepeer.analytics.lifecycle;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;

import com.livepeer.analytics.util.StringSemantics;

/**
 * Utility for bounded capability-candidate maintenance and deterministic attribution selection.
 *
 * <p>Selection contract (do not weaken without regression updates):
 * - Compatibility first: when model-hint context is available, incompatible model candidates must
 *   be ignored, even if they are temporally closer/fresher.
 * - Freshness second: among compatible candidates, prefer exact timestamp, then nearest prior
 *   within TTL, then nearest within TTL.
 * - Safety fallback: if no compatible candidate exists, return null so callers keep prior
 *   attribution state instead of force-applying an incompatible snapshot.
 *
 * <p>Context:
 * Production incidents showed mixed-model capability snapshots (for example streamdiffusion and
 * llama on the same hot wallet) causing wrong model attribution when compatibility context was
 * missing. Regression tests in CapabilityAttributionSelectorTest lock this behavior.</p>
 */
public final class CapabilityAttributionSelector {
    private static final String EMPTY_MODEL_KEY = "__empty__";

    public static final int DEFAULT_MAX_CANDIDATES_PER_WALLET = 32;

    private CapabilityAttributionSelector() {}

    public static void upsertCandidate(
            CapabilitySnapshotBucket bucket,
            CapabilitySnapshotRef candidate,
            long referenceTs,
            long ttlMs,
            int maxCandidatesPerWallet) {
        if (bucket == null || candidate == null) {
            return;
        }

        bucket.byModelKey.put(candidateKey(candidate), candidate);
        pruneStale(bucket, referenceTs, ttlMs);
        enforceMaxCandidates(bucket, Math.max(1, maxCandidatesPerWallet));
    }

    public static CapabilitySnapshotRef selectBestCandidate(
            CapabilitySnapshotBucket bucket,
            String modelHint,
            long signalTs,
            long ttlMs) {
        if (bucket == null || bucket.byModelKey.isEmpty()) {
            return null;
        }

        List<CapabilitySnapshotRef> candidates = new ArrayList<>(bucket.byModelKey.values());
        CapabilitySnapshotRef best = null;
        CandidateScore bestScore = null;
        for (CapabilitySnapshotRef candidate : candidates) {
            if (candidate == null || !isSnapshotModelCompatible(modelHint, candidate.modelId)) {
                continue;
            }
            CandidateScore score = CandidateScore.of(candidate, modelHint, signalTs, ttlMs);
            if (bestScore == null || score.compareTo(bestScore) < 0) {
                best = candidate;
                bestScore = score;
            }
        }
        return best;
    }

    private static void pruneStale(CapabilitySnapshotBucket bucket, long referenceTs, long ttlMs) {
        long maxDelta = ttlMs * 2L;
        bucket.byModelKey.entrySet().removeIf(entry ->
                entry.getValue() == null || Math.abs(referenceTs - entry.getValue().snapshotTs) > maxDelta);
    }

    private static void enforceMaxCandidates(CapabilitySnapshotBucket bucket, int maxCandidatesPerWallet) {
        int overflow = bucket.byModelKey.size() - maxCandidatesPerWallet;
        if (overflow <= 0) {
            return;
        }

        List<String> keysByOldest = new ArrayList<>(bucket.byModelKey.keySet());
        keysByOldest.sort(Comparator.comparingLong(key -> {
            CapabilitySnapshotRef ref = bucket.byModelKey.get(key);
            return ref == null ? Long.MIN_VALUE : ref.snapshotTs;
        }));
        for (int i = 0; i < overflow; i++) {
            bucket.byModelKey.remove(keysByOldest.get(i));
        }
    }

    private static String modelKey(String modelId) {
        if (StringSemantics.isBlank(modelId)) {
            return EMPTY_MODEL_KEY;
        }
        return modelId.trim().toLowerCase(Locale.ROOT);
    }

    private static String candidateKey(CapabilitySnapshotRef candidate) {
        return modelKey(candidate.modelId) + "|" + candidate.snapshotTs;
    }

    private static boolean isSnapshotModelCompatible(String modelHint, String snapshotModelId) {
        if (StringSemantics.isBlank(modelHint) || StringSemantics.isBlank(snapshotModelId)) {
            return true;
        }
        return modelHint.trim().equalsIgnoreCase(snapshotModelId.trim());
    }

    private static final class CandidateScore implements Comparable<CandidateScore> {
        private final int compatibilityRank;
        private final int freshnessRank;
        private final long deltaMs;
        private final long negativeSnapshotTs;
        private final String modelKey;

        private CandidateScore(
                int compatibilityRank,
                int freshnessRank,
                long deltaMs,
                long negativeSnapshotTs,
                String modelKey) {
            this.compatibilityRank = compatibilityRank;
            this.freshnessRank = freshnessRank;
            this.deltaMs = deltaMs;
            this.negativeSnapshotTs = negativeSnapshotTs;
            this.modelKey = modelKey;
        }

        static CandidateScore of(CapabilitySnapshotRef candidate, String modelHint, long signalTs, long ttlMs) {
            long delta = Math.abs(signalTs - candidate.snapshotTs);
            int compatibility = (!StringSemantics.isBlank(modelHint) && !StringSemantics.isBlank(candidate.modelId)
                    && modelHint.trim().equalsIgnoreCase(candidate.modelId.trim())) ? 0 : 1;
            int freshness;
            if (delta == 0) {
                freshness = 0;
            } else if (candidate.snapshotTs <= signalTs && delta <= ttlMs) {
                freshness = 1;
            } else if (delta <= ttlMs) {
                freshness = 2;
            } else if (candidate.snapshotTs <= signalTs) {
                freshness = 3;
            } else {
                freshness = 4;
            }
            return new CandidateScore(
                    compatibility,
                    freshness,
                    delta,
                    -candidate.snapshotTs,
                    modelKey(candidate.modelId));
        }

        @Override
        public int compareTo(CandidateScore other) {
            int cmp = Integer.compare(this.compatibilityRank, other.compatibilityRank);
            if (cmp != 0) {
                return cmp;
            }
            cmp = Integer.compare(this.freshnessRank, other.freshnessRank);
            if (cmp != 0) {
                return cmp;
            }
            cmp = Long.compare(this.deltaMs, other.deltaMs);
            if (cmp != 0) {
                return cmp;
            }
            cmp = Long.compare(this.negativeSnapshotTs, other.negativeSnapshotTs);
            if (cmp != 0) {
                return cmp;
            }
            return this.modelKey.compareTo(other.modelKey);
        }
    }
}
