package com.livepeer.analytics.lifecycle;

/**
 * Canonical attribution confidence mapping used by lifecycle facts.
 */
public final class AttributionSemantics {
    private AttributionSemantics() {}

    public static Decision fromSnapshot(long signalTs, long snapshotTs, long ttlMs) {
        long delta = Math.abs(signalTs - snapshotTs);
        if (delta == 0L) {
            return new Decision("exact_orchestrator_time_match", 1.0f);
        }
        if (snapshotTs <= signalTs && delta <= ttlMs) {
            return new Decision("nearest_prior_snapshot", 0.9f);
        }
        if (delta <= ttlMs) {
            return new Decision("nearest_snapshot_within_ttl", 0.7f);
        }
        return new Decision("proxy_address_join", 0.4f);
    }

    public static final class Decision {
        public final String method;
        public final float confidence;

        private Decision(String method, float confidence) {
            this.method = method;
            this.confidence = confidence;
        }
    }
}
