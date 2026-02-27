package com.livepeer.analytics.util;

/**
 * Utility for deterministic workflow session identity generation.
 */
public final class WorkflowSessionId {
    private WorkflowSessionId() {}

    /**
     * Build a deterministic workflow session identifier with stable fallbacks.
     */
    public static String from(String streamId, String requestId, String fallbackEventId) {
        String s = streamId == null ? "" : streamId.trim();
        String r = requestId == null ? "" : requestId.trim();
        String e = fallbackEventId == null ? "" : fallbackEventId.trim();

        if (!s.isEmpty() && !r.isEmpty()) {
            return s + "|" + r;
        }
        if (!s.isEmpty()) {
            return s + "|_missing_request";
        }
        if (!r.isEmpty()) {
            return "_missing_stream|" + r;
        }
        if (!e.isEmpty()) {
            return "_missing_stream|_missing_request|" + e;
        }
        return "_missing_stream|_missing_request|_missing_event";
    }
}
