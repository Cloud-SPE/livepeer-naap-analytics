package com.livepeer.analytics.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class HashingTest {

    @Test
    void sha256HexIsStable() {
        String hash1 = Hashing.sha256Hex("hello");
        String hash2 = Hashing.sha256Hex("hello");

        assertEquals(hash1, hash2);
        assertEquals(64, hash1.length());
    }
}
