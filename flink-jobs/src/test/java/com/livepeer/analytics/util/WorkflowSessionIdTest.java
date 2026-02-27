package com.livepeer.analytics.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class WorkflowSessionIdTest {

    @Test
    void buildsCompositeWhenStreamAndRequestExist() {
        assertEquals("stream-1|req-1", WorkflowSessionId.from("stream-1", "req-1", "evt-1"));
    }

    @Test
    void fallsBackToStreamOnly() {
        assertEquals("stream-1|_missing_request", WorkflowSessionId.from("stream-1", "", "evt-1"));
    }

    @Test
    void fallsBackToRequestOnly() {
        assertEquals("_missing_stream|req-1", WorkflowSessionId.from("", "req-1", "evt-1"));
    }

    @Test
    void fallsBackToEventId() {
        assertEquals("_missing_stream|_missing_request|evt-1", WorkflowSessionId.from("", "", "evt-1"));
    }
}
