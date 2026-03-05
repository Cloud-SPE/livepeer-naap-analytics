package com.livepeer.analytics.util;

import com.livepeer.analytics.model.StreamingEvent;

public final class EventUids {
    private EventUids() {}

    public static String rawEventUid(StreamingEvent event) {
        if (event == null) {
            return "";
        }
        if (!StringSemantics.isBlank(event.rawEventUid)) {
            return event.rawEventUid;
        }
        if (!StringSemantics.isBlank(event.eventId)) {
            return event.eventId;
        }
        return Hashing.sha256Hex(event.rawJson == null ? "" : event.rawJson);
    }
}
