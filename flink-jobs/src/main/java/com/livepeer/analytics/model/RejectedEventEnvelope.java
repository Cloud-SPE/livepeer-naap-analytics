package com.livepeer.analytics.model;

import java.io.Serializable;

/**
 * DLQ/quarantine envelope containing failure metadata and the original payload.
 */
public class RejectedEventEnvelope implements Serializable {
    private static final long serialVersionUID = 1L;

    public String schemaVersion;
    public SourcePointer source;
    public FailureDetails failure;
    public Identity identity;
    public EventDimensions dimensions;
    public Payload payload;
    public String dedupKey;
    public String dedupStrategy;
    public boolean replay;
    public long ingestionTimestamp;
    public Long eventTimestamp;

    public RejectedEventEnvelope() {}

    public static class SourcePointer implements Serializable {
        private static final long serialVersionUID = 1L;

        public String topic;
        public int partition;
        public long offset;
        public long recordTimestamp;

        public SourcePointer() {}
    }

    public static class FailureDetails implements Serializable {
        private static final long serialVersionUID = 1L;

        public String stage;
        public String failureClass;
        public String reason;
        public String details;

        public FailureDetails() {}
    }

    public static class Identity implements Serializable {
        private static final long serialVersionUID = 1L;

        public String eventId;
        public String eventType;
        public String eventVersion;

        public Identity() {}
    }

    public static class EventDimensions implements Serializable {
        private static final long serialVersionUID = 1L;

        public String orchestrator;
        public String broadcaster;
        public String region;

        public EventDimensions() {}
    }

    public static class Payload implements Serializable {
        private static final long serialVersionUID = 1L;

        public String encoding;
        public String body;
        public String canonicalJson;

        public Payload() {}
    }
}
