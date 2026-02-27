package com.livepeer.analytics.model;

import java.io.Serializable;

/**
 * Normalized event metadata derived from a raw Kafka record.
 */
public class StreamingEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    public String eventId;
    public String eventType;
    public String eventVersion;
    public String gateway;
    public String rawJson;
    public long timestamp;
    public String dedupKey;
    public String dedupStrategy;
    public boolean replay;
    public RejectedEventEnvelope.SourcePointer source;
    public RejectedEventEnvelope.EventDimensions dimensions;

    public StreamingEvent() {}
}
