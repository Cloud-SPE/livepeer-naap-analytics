package com.livepeer.analytics.capability;

import java.io.Serializable;

/**
 * Immutable capability metadata entry resolved from the seed catalog.
 */
public final class CapabilityDescriptor implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final CapabilityDescriptor UNKNOWN = new CapabilityDescriptor(-1, "unknown", "unknown");

    private final int id;
    private final String name;
    private final String group;

    public CapabilityDescriptor(int id, String name, String group) {
        this.id = id;
        this.name = name == null || name.isBlank() ? "unknown" : name;
        this.group = group == null || group.isBlank() ? "unknown" : group;
    }

    public int id() {
        return id;
    }

    public String name() {
        return name;
    }

    public String group() {
        return group;
    }
}
