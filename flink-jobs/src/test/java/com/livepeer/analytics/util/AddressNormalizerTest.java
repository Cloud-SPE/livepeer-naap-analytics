package com.livepeer.analytics.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AddressNormalizerTest {

    @Test
    void normalizeOrEmptyTrimsAndLowerCases() {
        assertEquals("", AddressNormalizer.normalizeOrEmpty(null));
        assertEquals("", AddressNormalizer.normalizeOrEmpty("   "));
        assertEquals("0xabc123", AddressNormalizer.normalizeOrEmpty(" 0xAbC123 "));
    }
}
