package com.livepeer.analytics.lifecycle;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Bounded per-hot-wallet capability candidates keyed by normalized model identifier.
 */
public class CapabilitySnapshotBucket implements Serializable {
    private static final long serialVersionUID = 1L;

    public final Map<String, CapabilitySnapshotRef> byModelKey = new HashMap<>();
}
