package com.livepeer.analytics.lifecycle;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import com.livepeer.analytics.util.AddressNormalizer;
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
 * - Ambiguity guard: if compatible in-window candidates map to multiple canonical orchestrators,
 *   return null to avoid unsafe proxy-bucket attribution fanout.
 *
 * <p>Context:
 * Production incidents showed mixed-model capability snapshots (for example streamdiffusion and
 * llama on the same hot wallet) causing wrong model attribution when compatibility context was
 * missing. Regression tests in CapabilityAttributionSelectorTest lock this behavior.</p>
 */
public final class CapabilityAttributionSelector {
    private static final String EMPTY_MODEL_KEY = "__empty__";
    private static final String EMPTY_KEY = "__empty__";

    public static final int DEFAULT_MAX_CANDIDATES_PER_WALLET = 32;

    private CapabilityAttributionSelector() {}

    /**
     * Upserts one candidate snapshot and keeps bucket state bounded.
     *
     * <p>State management policy:
     * - index by model-key + snapshot timestamp for deterministic replacement,
     * - prune stale entries first,
     * - then enforce max candidates to keep keyed state bounded.</p>
     */
    public static void upsertCandidate(
            CapabilitySnapshotBucket bucket,
            CapabilitySnapshotRef candidate,
            long referenceTs,
            long ttlMs,
            int maxCandidatesPerWallet) {
        if (bucket == null || candidate == null) {
            return;
        }

        bucket.byCandidateKey.put(candidateKey(candidate), candidate);
        pruneStale(bucket, referenceTs, ttlMs);
        enforceMaxCandidates(bucket, Math.max(1, maxCandidatesPerWallet));
    }

    /**
     * Selects the safest best candidate for attribution at a signal timestamp.
     *
     * <p>Returns {@code null} when ambiguity or incompatibility makes attribution unsafe.</p>
     */
    public static CapabilitySnapshotRef selectBestCandidate(
            CapabilitySnapshotBucket bucket,
            String modelHint,
            long signalTs,
            long ttlMs) {
        return selectBestCandidate(bucket, SelectionContext.of(modelHint, null, null, signalTs, ttlMs));
    }

    public static CapabilitySnapshotRef selectBestCandidate(
            CapabilitySnapshotBucket bucket,
            SelectionContext context) {
        if (bucket == null || bucket.byCandidateKey.isEmpty() || context == null) {
            return null;
        }

        List<CapabilitySnapshotRef> candidates = new ArrayList<>(bucket.byCandidateKey.values());
        if (hasAmbiguousCanonicalCandidates(candidates, context.modelHint, context.signalTs, context.ttlMs)) {
            return null;
        }

        CapabilitySnapshotRef best = null;
        CandidateScore bestScore = null;
        for (CapabilitySnapshotRef candidate : candidates) {
            if (candidate == null || !isSnapshotModelCompatible(context.modelHint, candidate.modelId)) {
                continue;
            }
            CandidateScore score = CandidateScore.of(candidate, context);
            if (bestScore == null || score.compareTo(bestScore) < 0) {
                best = candidate;
                bestScore = score;
            }
        }
        return best;
    }

    /**
     * Ambiguity guard: if compatible in-window candidates map to multiple canonical orchestrators,
     * attribution is rejected to prevent proxy-key fanout misattribution.
     */
    private static boolean hasAmbiguousCanonicalCandidates(
            List<CapabilitySnapshotRef> candidates,
            String modelHint,
            long signalTs,
            long ttlMs) {
        Set<String> canonicals = new HashSet<>();
        for (CapabilitySnapshotRef candidate : candidates) {
            if (candidate == null || !isSnapshotModelCompatible(modelHint, candidate.modelId)) {
                continue;
            }
            long delta = Math.abs(signalTs - candidate.snapshotTs);
            if (delta > ttlMs) {
                continue;
            }
            String canonical = AddressNormalizer.normalizeOrEmpty(candidate.canonicalOrchestratorAddress);
            if (StringSemantics.isBlank(canonical)) {
                continue;
            }
            canonicals.add(canonical);
            if (canonicals.size() > 1) {
                return true;
            }
        }
        return false;
    }

    private static void pruneStale(CapabilitySnapshotBucket bucket, long referenceTs, long ttlMs) {
        long maxDelta = ttlMs * 2L;
        bucket.byCandidateKey.entrySet().removeIf(entry ->
                entry.getValue() == null || Math.abs(referenceTs - entry.getValue().snapshotTs) > maxDelta);
    }

    private static void enforceMaxCandidates(CapabilitySnapshotBucket bucket, int maxCandidatesPerWallet) {
        int overflow = bucket.byCandidateKey.size() - maxCandidatesPerWallet;
        if (overflow <= 0) {
            return;
        }

        List<String> keysByOldest = new ArrayList<>(bucket.byCandidateKey.keySet());
        keysByOldest.sort(Comparator.comparingLong(key -> {
            CapabilitySnapshotRef ref = bucket.byCandidateKey.get(key);
            return ref == null ? Long.MIN_VALUE : ref.snapshotTs;
        }));
        for (int i = 0; i < overflow; i++) {
            bucket.byCandidateKey.remove(keysByOldest.get(i));
        }
    }

    private static String modelKey(String modelId) {
        if (StringSemantics.isBlank(modelId)) {
            return EMPTY_MODEL_KEY;
        }
        return modelId.trim().toLowerCase(Locale.ROOT);
    }

    private static String candidateKey(CapabilitySnapshotRef candidate) {
        return modelKey(candidate.modelId)
                + "|"
                + normalizeKey(candidate.canonicalOrchestratorAddress)
                + "|"
                + normalizeKey(candidate.orchestratorUrl)
                + "|"
                + normalizeKey(candidate.gpuId)
                + "|"
                + candidate.snapshotTs
                + "|"
                + normalizeKey(candidate.sourceEventId);
    }

    private static String normalizeKey(String value) {
        if (StringSemantics.isBlank(value)) {
            return EMPTY_KEY;
        }
        return value.trim().toLowerCase(Locale.ROOT);
    }

    private static boolean isSnapshotModelCompatible(String modelHint, String snapshotModelId) {
        if (StringSemantics.isBlank(modelHint) || StringSemantics.isBlank(snapshotModelId)) {
            return true;
        }
        return modelHint.trim().equalsIgnoreCase(snapshotModelId.trim());
    }

    private static final class CandidateScore implements Comparable<CandidateScore> {
        private final int compatibilityRank;
        private final int uriRank;
        private final int gpuRank;
        private final int freshnessRank;
        private final long deltaMs;
        private final long negativeSnapshotTs;
        private final String modelKey;
        private final String uriKey;
        private final String gpuKey;

        private CandidateScore(
                int compatibilityRank,
                int uriRank,
                int gpuRank,
                int freshnessRank,
                long deltaMs,
                long negativeSnapshotTs,
                String modelKey,
                String uriKey,
                String gpuKey) {
            this.compatibilityRank = compatibilityRank;
            this.uriRank = uriRank;
            this.gpuRank = gpuRank;
            this.freshnessRank = freshnessRank;
            this.deltaMs = deltaMs;
            this.negativeSnapshotTs = negativeSnapshotTs;
            this.modelKey = modelKey;
            this.uriKey = uriKey;
            this.gpuKey = gpuKey;
        }

        static CandidateScore of(CapabilitySnapshotRef candidate, SelectionContext context) {
            long delta = Math.abs(context.signalTs - candidate.snapshotTs);
            int compatibility = (!StringSemantics.isBlank(context.modelHint) && !StringSemantics.isBlank(candidate.modelId)
                    && context.modelHint.trim().equalsIgnoreCase(candidate.modelId.trim())) ? 0 : 1;
            int uriRank = equalityRank(context.orchestratorUrlHint, candidate.orchestratorUrl);
            int gpuRank = equalityRank(context.gpuHint, candidate.gpuId);
            int freshness;
            if (delta == 0) {
                freshness = 0;
            } else if (candidate.snapshotTs <= context.signalTs && delta <= context.ttlMs) {
                freshness = 1;
            } else if (delta <= context.ttlMs) {
                freshness = 2;
            } else if (candidate.snapshotTs <= context.signalTs) {
                freshness = 3;
            } else {
                freshness = 4;
            }
            return new CandidateScore(
                    compatibility,
                    uriRank,
                    gpuRank,
                    freshness,
                    delta,
                    -candidate.snapshotTs,
                    modelKey(candidate.modelId),
                    normalizeKey(candidate.orchestratorUrl),
                    normalizeKey(candidate.gpuId));
        }

        private static int equalityRank(String hint, String candidate) {
            if (StringSemantics.isBlank(hint)) {
                return 1;
            }
            return normalizeKey(hint).equals(normalizeKey(candidate)) ? 0 : 1;
        }

        @Override
        public int compareTo(CandidateScore other) {
            int cmp = Integer.compare(this.compatibilityRank, other.compatibilityRank);
            if (cmp != 0) {
                return cmp;
            }
            cmp = Integer.compare(this.uriRank, other.uriRank);
            if (cmp != 0) {
                return cmp;
            }
            cmp = Integer.compare(this.gpuRank, other.gpuRank);
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
            cmp = this.modelKey.compareTo(other.modelKey);
            if (cmp != 0) {
                return cmp;
            }
            cmp = this.uriKey.compareTo(other.uriKey);
            if (cmp != 0) {
                return cmp;
            }
            return this.gpuKey.compareTo(other.gpuKey);
        }
    }

    public static final class SelectionContext {
        public final String modelHint;
        public final String orchestratorUrlHint;
        public final String gpuHint;
        public final long signalTs;
        public final long ttlMs;

        private SelectionContext(
                String modelHint,
                String orchestratorUrlHint,
                String gpuHint,
                long signalTs,
                long ttlMs) {
            this.modelHint = modelHint;
            this.orchestratorUrlHint = orchestratorUrlHint;
            this.gpuHint = gpuHint;
            this.signalTs = signalTs;
            this.ttlMs = ttlMs;
        }

        public static SelectionContext of(
                String modelHint,
                String orchestratorUrlHint,
                String gpuHint,
                long signalTs,
                long ttlMs) {
            return new SelectionContext(modelHint, orchestratorUrlHint, gpuHint, signalTs, ttlMs);
        }
    }
}
