package com.livepeer.analytics.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StringSemanticsTest {

    @Test
    void isBlankCoversNullWhitespaceAndNonBlank() {
        assertTrue(StringSemantics.isBlank(null));
        assertTrue(StringSemantics.isBlank(""));
        assertTrue(StringSemantics.isBlank("   "));
        assertFalse(StringSemantics.isBlank("a"));
    }

    @Test
    void firstNonBlankReturnsFirstUsefulValueElseEmptyString() {
        assertEquals("x", StringSemantics.firstNonBlank(" ", null, "x", "y"));
        assertEquals("", StringSemantics.firstNonBlank(" ", null, "\t"));
    }

    @Test
    void blankToNullConvertsOnlyBlankValues() {
        assertNull(StringSemantics.blankToNull(null));
        assertNull(StringSemantics.blankToNull(" "));
        assertEquals(" value ", StringSemantics.blankToNull(" value "));
    }
}
