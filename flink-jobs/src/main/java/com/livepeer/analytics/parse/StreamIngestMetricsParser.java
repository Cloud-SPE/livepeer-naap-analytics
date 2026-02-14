package com.livepeer.analytics.parse;

import com.fasterxml.jackson.databind.JsonNode;

import com.livepeer.analytics.model.EventPayloads;
import com.livepeer.analytics.quality.ValidatedEvent;

import java.util.ArrayList;
import java.util.List;

/**
 * Parser for `stream_ingest_metrics` events.
 */
final class StreamIngestMetricsParser {
    private StreamIngestMetricsParser() {}

    static List<EventPayloads.StreamIngestMetrics> parse(ValidatedEvent event) throws Exception {
        JsonNode data = ParseSupport.requireData(event, "stream_ingest_metrics");
        EventPayloads.StreamIngestMetrics metrics = new EventPayloads.StreamIngestMetrics();

        metrics.eventTimestamp = event.event.timestamp;
        metrics.streamId = data.path("stream_id").asText("");
        metrics.requestId = data.path("request_id").asText("");
        metrics.pipelineId = data.path("pipeline_id").asText("");

        JsonNode stats = data.path("stats");
        metrics.connectionQuality = stats.path("conn_quality").asText("");

        JsonNode peerStats = stats.path("peer_conn_stats");
        metrics.bytesReceived = JsonNodeUtils.asLongOrDefault(peerStats.path("BytesReceived"), 0L);
        metrics.bytesSent = JsonNodeUtils.asLongOrDefault(peerStats.path("BytesSent"), 0L);

        JsonNode trackStats = stats.path("track_stats");
        if (trackStats.isArray()) {
            for (JsonNode track : trackStats) {
                String type = track.path("type").asText("");
                if ("video".equals(type)) {
                    metrics.videoJitter = (float) track.path("jitter").asDouble(0.0);
                    metrics.videoPacketsReceived = track.path("packets_received").asInt(0);
                    metrics.videoPacketsLost = track.path("packets_lost").asInt(0);
                    metrics.videoPacketLossPct = (float) track.path("packet_loss_pct").asDouble(0.0);
                    metrics.videoRtt = (float) track.path("rtt").asDouble(0.0);
                    metrics.videoLastInputTs = (float) track.path("last_input_ts").asDouble(0.0);
                    metrics.videoLatency = (float) track.path("latency").asDouble(0.0);
                } else if ("audio".equals(type)) {
                    metrics.audioJitter = (float) track.path("jitter").asDouble(0.0);
                    metrics.audioPacketsReceived = track.path("packets_received").asInt(0);
                    metrics.audioPacketsLost = track.path("packets_lost").asInt(0);
                    metrics.audioPacketLossPct = (float) track.path("packet_loss_pct").asDouble(0.0);
                    metrics.audioRtt = (float) track.path("rtt").asDouble(0.0);
                    metrics.audioLastInputTs = (float) track.path("last_input_ts").asDouble(0.0);
                    metrics.audioLatency = (float) track.path("latency").asDouble(0.0);
                }
            }
        }

        metrics.rawJson = event.event.rawJson;
        List<EventPayloads.StreamIngestMetrics> results = new ArrayList<>(1);
        results.add(metrics);
        return results;
    }
}
