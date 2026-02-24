package com.livepeer.analytics.quality;

import com.fasterxml.jackson.databind.JsonNode;

import com.livepeer.analytics.model.KafkaInboundRecord;
import com.livepeer.analytics.model.RejectedEventEnvelope;
import com.livepeer.analytics.parse.JsonNodeUtils;
import com.livepeer.analytics.util.StringSemantics;

/**
 * Shared extraction helpers for DLQ/quarantine envelope construction.
 */
public final class RejectedEventEnvelopeSupport {
    private RejectedEventEnvelopeSupport() {}

    public static RejectedEventEnvelope.SourcePointer buildSourcePointer(KafkaInboundRecord record) {
        RejectedEventEnvelope.SourcePointer source = new RejectedEventEnvelope.SourcePointer();
        if (record != null) {
            source.topic = record.topic;
            source.partition = record.partition;
            source.offset = record.offset;
            source.recordTimestamp = record.recordTimestamp;
        }
        return source;
    }

    public static RejectedEventEnvelope.EventDimensions extractDimensions(JsonNode root) {
        if (root == null) {
            return null;
        }
        JsonNode data = root.path("data");
        RejectedEventEnvelope.EventDimensions dims = new RejectedEventEnvelope.EventDimensions();
        String orchestrator = null;
        JsonNode orchInfo = data.path("orchestrator_info");
        if (orchInfo.isObject()) {
            orchestrator = JsonNodeUtils.asNullableText(orchInfo.path("address"));
        }
        if (StringSemantics.isBlank(orchestrator)) {
            orchestrator = JsonNodeUtils.asNullableText(data.path("orchestrator"));
        }
        dims.orchestrator = orchestrator;
        dims.broadcaster = JsonNodeUtils.asNullableText(data.path("broadcaster"));
        dims.region = JsonNodeUtils.asNullableText(data.path("region"));
        return dims;
    }
}
