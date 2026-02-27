package com.livepeer.analytics.util;

import java.util.Locale;

/**
 * Shared address normalization contract for wallet/orchestrator identifiers.
 */
public final class AddressNormalizer {
    private AddressNormalizer() {}

    public static String normalizeOrEmpty(String value) {
        if (StringSemantics.isBlank(value)) {
            return "";
        }
        return value.trim().toLowerCase(Locale.ROOT);
    }
}
