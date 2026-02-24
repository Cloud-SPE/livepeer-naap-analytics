package com.livepeer.analytics.lifecycle;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AttributionSemanticsTest {

    @Test
    void exactSnapshotHasHighestConfidence() {
        AttributionSemantics.Decision d = AttributionSemantics.fromSnapshot(1_000L, 1_000L, 10_000L);
        assertEquals("exact_orchestrator_time_match", d.method);
        assertEquals(1.0f, d.confidence);
    }

    @Test
    void nearestPriorWithinTtlUsesPriorSemantics() {
        AttributionSemantics.Decision d = AttributionSemantics.fromSnapshot(2_000L, 1_500L, 1_000L);
        assertEquals("nearest_prior_snapshot", d.method);
        assertEquals(0.9f, d.confidence);
    }

    @Test
    void nearestWithinTtlFutureSnapshotUsesMidConfidence() {
        AttributionSemantics.Decision d = AttributionSemantics.fromSnapshot(2_000L, 2_500L, 1_000L);
        assertEquals("nearest_snapshot_within_ttl", d.method);
        assertEquals(0.7f, d.confidence);
    }

    @Test
    void staleSnapshotFallsBackToProxyJoin() {
        AttributionSemantics.Decision d = AttributionSemantics.fromSnapshot(2_000L, 20_000L, 1_000L);
        assertEquals("proxy_address_join", d.method);
        assertEquals(0.4f, d.confidence);
    }
}
