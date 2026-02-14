package com.livepeer.analytics.lifecycle;

import org.apache.flink.api.common.state.MapStateDescriptor;

/**
 * Shared Flink broadcast state descriptors for lifecycle enrichment.
 */
public final class CapabilityBroadcastState {
    private CapabilityBroadcastState() {}

    public static final MapStateDescriptor<String, CapabilitySnapshotRef> CAPABILITY_STATE_DESCRIPTOR =
            new MapStateDescriptor<>("capability-by-hot-wallet", String.class, CapabilitySnapshotRef.class);
}

