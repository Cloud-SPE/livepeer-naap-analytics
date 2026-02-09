package com.livepeer.analytics.util;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

public final class Hashing {
    private Hashing() {}

    public static String sha256Hex(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(hash.length * 2);
            for (byte b : hash) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to compute hash", ex);
        }
    }
}
