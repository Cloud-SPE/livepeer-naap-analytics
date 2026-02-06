package com.livepeer.analytics.quality;

import com.fasterxml.jackson.databind.JsonNode;

import com.livepeer.analytics.model.StreamingEvent;
import com.livepeer.analytics.util.JsonSupport;

import java.io.Serializable;

/**
 * Holds a validated StreamingEvent with its parsed JSON tree for downstream parsing.
 */
public class ValidatedEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    public StreamingEvent event;
    // Transient to avoid Kryo/ReflectASM instantiation issues with JsonNode on Java 17+.
    public transient JsonNode root;
    public transient JsonNode data;

    public ValidatedEvent() {}

    public ValidatedEvent(StreamingEvent event, JsonNode root) {
        this.event = event;
        this.root = root;
        this.data = root == null ? null : root.path("data");
    }

    public JsonNode root() {
        if (root == null) {
            root = parseRoot();
            data = root == null ? null : root.path("data");
        }
        return root;
    }

    public JsonNode data() {
        if (data == null) {
            JsonNode parsedRoot = root();
            data = parsedRoot == null ? null : parsedRoot.path("data");
        }
        return data;
    }

    private JsonNode parseRoot() {
        if (event == null || event.rawJson == null) {
            return null;
        }
        try {
            return JsonSupport.MAPPER.readTree(event.rawJson);
        } catch (Exception ex) {
            return null;
        }
    }
}
