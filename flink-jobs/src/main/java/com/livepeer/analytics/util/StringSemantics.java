package com.livepeer.analytics.util;

/**
 * Shared string semantics for blank handling and deterministic fallbacks.
 */
public final class StringSemantics {
    private StringSemantics() {}

    public static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    public static String blankToNull(String value) {
        return isBlank(value) ? null : value;
    }

    public static String firstNonBlank(String... values) {
        for (String value : values) {
            if (!isBlank(value)) {
                return value;
            }
        }
        return "";
    }
}
