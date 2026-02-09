package com.livepeer.analytics.model;

import java.io.Serializable;

/**
 * Wraps a parsed payload with its original StreamingEvent context.
 */
public class ParsedEvent<T> implements Serializable {
    private static final long serialVersionUID = 1L;

    public StreamingEvent event;
    public T payload;

    public ParsedEvent() {}

    public ParsedEvent(StreamingEvent event, T payload) {
        this.event = event;
        this.payload = payload;
    }
}
