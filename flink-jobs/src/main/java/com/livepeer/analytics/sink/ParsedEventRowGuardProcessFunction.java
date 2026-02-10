package com.livepeer.analytics.sink;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.livepeer.analytics.model.ParsedEvent;
import com.livepeer.analytics.model.RejectedEventEnvelope;
import com.livepeer.analytics.quality.RejectedEventEnvelopeFactory;

import java.nio.charset.StandardCharsets;

/**
 * Guards mapped ClickHouse rows produced from parsed events by enforcing a max size.
 * Oversized rows are either routed to the DLQ or dropped, depending on configuration.
 */
public class ParsedEventRowGuardProcessFunction<T> extends ProcessFunction<ParsedEvent<T>, String> {
    private static final Logger LOG = LoggerFactory.getLogger(ParsedEventRowGuardProcessFunction.class);

    private final RowMapper<T> mapper;
    private final OutputTag<RejectedEventEnvelope> dlqTag;
    private final int sizeLimitBytes;
    private final boolean emitDlqOnGuardFailure;
    private transient org.apache.flink.metrics.Counter oversizeCounter;

    public ParsedEventRowGuardProcessFunction(
            RowMapper<T> mapper,
            OutputTag<RejectedEventEnvelope> dlqTag,
            int sizeLimitBytes,
            boolean emitDlqOnGuardFailure) {
        this.mapper = mapper;
        this.dlqTag = dlqTag;
        this.sizeLimitBytes = sizeLimitBytes;
        this.emitDlqOnGuardFailure = emitDlqOnGuardFailure;
    }

    @Override
    public void processElement(ParsedEvent<T> value, Context ctx, org.apache.flink.util.Collector<String> out) {
        if (value == null || value.payload == null) {
            return;
        }
        String row = mapper.map(value.payload);
        int sizeBytes = rowSizeBytes(row);
        if (sizeBytes > sizeLimitBytes) {
            if (oversizeCounter != null) {
                oversizeCounter.inc();
            }
            String details = "Row size " + sizeBytes + " exceeds limit " + sizeLimitBytes;
            if (emitDlqOnGuardFailure) {
                if (value.event != null) {
                    ctx.output(dlqTag, RejectedEventEnvelopeFactory.forSinkGuardFailure(value.event, "record_too_large", details));
                } else {
                    LOG.warn("Dropping oversized row with no event context ({} bytes): {}", sizeBytes, details);
                }
            } else {
                String eventId = value.event == null ? null : value.event.eventId;
                String eventType = value.event == null ? null : value.event.eventType;
                String dedupKey = value.event == null ? null : value.event.dedupKey;
                LOG.warn("Dropping oversized row ({} bytes) for sink guard: {} (id={}, type={}, dedupKey={})",
                        sizeBytes, details, eventId, eventType, dedupKey);
            }
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
