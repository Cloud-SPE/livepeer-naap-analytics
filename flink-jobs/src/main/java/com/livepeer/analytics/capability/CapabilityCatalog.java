package com.livepeer.analytics.capability;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * In-memory lookup for capability metadata loaded from a versioned seed catalog.
 */
public final class CapabilityCatalog implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String version;
    private final Map<Integer, CapabilityDescriptor> byId;

    public CapabilityCatalog(String version, Map<Integer, CapabilityDescriptor> byId) {
        this.version = version == null || version.isBlank() ? "unknown" : version;
        this.byId = Collections.unmodifiableMap(new HashMap<>(byId));
    }

    public String version() {
        return version;
    }

    public CapabilityDescriptor resolve(Integer capabilityId) {
        if (capabilityId == null) {
            return CapabilityDescriptor.UNKNOWN;
        }
        return byId.getOrDefault(capabilityId, new CapabilityDescriptor(capabilityId, "unknown", "unknown"));
    }

    public int size() {
        return byId.size();
    }
}
