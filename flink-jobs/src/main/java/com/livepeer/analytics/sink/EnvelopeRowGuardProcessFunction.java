package com.livepeer.analytics.sink;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.livepeer.analytics.model.RejectedEventEnvelope;

import java.nio.charset.StandardCharsets;

/**
 * Guards DLQ/quarantine rows by dropping oversized envelopes to avoid recursive DLQ loops.
 */
public class EnvelopeRowGuardProcessFunction extends ProcessFunction<RejectedEventEnvelope, String> {
    private static final Logger LOG = LoggerFactory.getLogger(EnvelopeRowGuardProcessFunction.class);

    private final RowMapper<RejectedEventEnvelope> mapper;
    private final int sizeLimitBytes;
    private transient org.apache.flink.metrics.Counter oversizeCounter;

    public EnvelopeRowGuardProcessFunction(RowMapper<RejectedEventEnvelope> mapper, int sizeLimitBytes) {
        this.mapper = mapper;
        this.sizeLimitBytes = sizeLimitBytes;
    }

    @Override
    public void processElement(RejectedEventEnvelope value, Context ctx, org.apache.flink.util.Collector<String> out) {
        if (value == null) {
            return;
        }
        String row = mapper.map(value);
        int sizeBytes = rowSizeBytes(row);
        if (sizeBytes > sizeLimitBytes) {
            if (oversizeCounter != null) {
                oversizeCounter.inc();
            }
            String eventId = value.identity == null ? null : value.identity.eventId;
            String eventType = value.identity == null ? null : value.identity.eventType;
            LOG.warn("Dropping oversized DLQ row ({} bytes) for sink guard (id={}, type={})",
                    sizeBytes, eventId, eventType);
            return;
        }
        out.collect(row);
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) {
        org.apache.flink.metrics.MetricGroup metrics = getRuntimeContext().getMetricGroup()
                .addGroup("quality_gate")
                .addGroup("sink_guard");
        oversizeCounter = metrics.counter("oversize_drops");
    }

    private static int rowSizeBytes(String row) {
        return row.getBytes(StandardCharsets.UTF_8).length;
    }
}
