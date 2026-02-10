package com.livepeer.analytics.quality;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.livepeer.analytics.model.RejectedEventEnvelope;

/**
 * Deduplicates validated events using keyed state with TTL and emits duplicates to quarantine.
 */
public class DeduplicationProcessFunction extends KeyedProcessFunction<String, ValidatedEvent, ValidatedEvent> {
    private static final Logger LOG = LoggerFactory.getLogger(DeduplicationProcessFunction.class);

    private final QualityGateConfig config;
    private final OutputTag<RejectedEventEnvelope> quarantineTag;
    private transient ValueState<Long> seenState;
    private transient Counter duplicateCounter;
    private transient Counter acceptedCounter;
    private transient Meter duplicateRate;

    public DeduplicationProcessFunction(QualityGateConfig config, OutputTag<RejectedEventEnvelope> quarantineTag) {
        this.config = config;
        this.quarantineTag = quarantineTag;
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) {
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.minutes(config.dedupTtlMinutes))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();

        ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>("dedup-seen", Long.class);
        descriptor.enableTimeToLive(ttlConfig);
        seenState = getRuntimeContext().getState(descriptor);

        org.apache.flink.metrics.MetricGroup metrics = getRuntimeContext().getMetricGroup().addGroup("quality_gate").addGroup("dedup");
        duplicateCounter = metrics.counter("duplicates");
        acceptedCounter = metrics.counter("accepted");
        duplicateRate = metrics.meter("duplicate_rate", new MeterView(duplicateCounter, (int) config.metricsRateWindow.getSeconds()));
        LOG.info("Deduplication initialized (ttlMinutes={})", config.dedupTtlMinutes);
    }

    @Override
    public void processElement(ValidatedEvent validated, Context ctx, org.apache.flink.util.Collector<ValidatedEvent> out) throws Exception {
        if (validated == null || validated.event == null) {
            return;
        }

        Long seen = seenState.value();
        if (seen != null) {
            duplicateCounter.inc();
            LOG.debug("Duplicate detected (id={}, type={}, key={})",
                    validated.event.eventId, validated.event.eventType, validated.event.dedupKey);
            ctx.output(quarantineTag, RejectedEventEnvelopeFactory.forDedupFailure(validated.event, "duplicate_event"));
            return;
        }

        seenState.update(System.currentTimeMillis());
        acceptedCounter.inc();
        out.collect(validated);
    }
}
