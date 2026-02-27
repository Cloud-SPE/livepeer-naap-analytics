package com.livepeer.analytics.sink;

import java.nio.charset.StandardCharsets;

/**
 * Shared UTF-8 row-size helper for sink guards.
 */
public final class RowSizeUtil {
    private RowSizeUtil() {}

    public static int utf8Bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8).length;
    }
}
