package com.livepeer.analytics.quality;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.livepeer.analytics.model.KafkaInboundRecord;
import com.livepeer.analytics.model.RejectedEventEnvelope;
import com.livepeer.analytics.model.StreamingEvent;
import com.livepeer.analytics.parse.JsonNodeUtils;
import com.livepeer.analytics.util.Hashing;
import com.livepeer.analytics.util.JsonSupport;

import java.nio.charset.StandardCharsets;

/**
 * Quality gate that deserializes raw Kafka records, validates schema, and emits
 * either a ValidatedEvent or a DLQ envelope describing the failure.
 */
public class QualityGateProcessFunction extends ProcessFunction<KafkaInboundRecord, ValidatedEvent> {
    private static final Logger LOG = LoggerFactory.getLogger(QualityGateProcessFunction.class);

    private final QualityGateConfig config;
    private final OutputTag<RejectedEventEnvelope> dlqTag;
    private transient SchemaValidator validator;
    private transient Counter inputCounter;
    private transient Counter acceptedCounter;
    private transient Counter dlqCounter;
    private transient Meter inputRate;
    private transient Meter dlqRate;
    private transient long lastDlqLogMs;
    private transient long dlqSinceLastLog;

    public QualityGateProcessFunction(QualityGateConfig config, OutputTag<RejectedEventEnvelope> dlqTag) {
        this.config = config;
        this.dlqTag = dlqTag;
    }

    /**
     * Initializes resources and metrics used by this process function.
     *
     * - Instantiates a SchemaValidator using the supported versions from the job config.
     * - Creates a MetricGroup named "quality_gate" and registers three counters: "input", "accepted", and "dlq".
     * - Creates two meters ("input_rate" and "dlq_rate") backed by MeterView instances that compute rates
     *   over the window length specified by config.metricsRateWindow.getSeconds(), using the corresponding counters.
     *
     * Notes on Flink metrics:
     * - Metrics registered via getRuntimeContext().getMetricGroup() are exposed to Flink's internal metrics subsystem
     *   and therefore are available to be collected by Flink.
     * - To export these metrics to an external backend (Prometheus, JMX, Graphite, etc.), you must configure
     *   the appropriate metric reporters in Flink's configuration (e.g., flink-conf.yaml or cluster/job configuration).
     * - MeterView computes the rate locally in the task; the values seen by reporters are those provided by Flink's metric system.
     *
     * @param parameters the Flink configuration provided to the function during initialization
     */
    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) {
        this.validator = new SchemaValidator(config.supportedVersions);
        org.apache.flink.metrics.MetricGroup metrics = getRuntimeContext().getMetricGroup().addGroup("quality_gate");
        this.inputCounter = metrics.counter("input");
        this.acceptedCounter = metrics.counter("accepted");
        this.dlqCounter = metrics.counter("dlq");
        this.inputRate = metrics.meter("input_rate", new MeterView(inputCounter, (int) config.metricsRateWindow.getSeconds()));
        this.dlqRate = metrics.meter("dlq_rate", new MeterView(dlqCounter, (int) config.metricsRateWindow.getSeconds()));
        this.lastDlqLogMs = System.currentTimeMillis();
        this.dlqSinceLastLog = 0;
        LOG.info("Quality gate initialized (supportedVersions={}, metricsWindowSec={})",
                config.supportedVersions, config.metricsRateWindow.getSeconds());
    }

    @Override
    public void processElement(KafkaInboundRecord record, Context ctx, org.apache.flink.util.Collector<ValidatedEvent> out) {
        inputCounter.inc();

        if (record == null || record.value == null) {
            LOG.debug("Rejecting record with null payload (topic={}, partition={}, offset={})",
                    record == null ? null : record.topic,
                    record == null ? null : record.partition,
                    record == null ? null : record.offset);
            emitDlq(ctx, RejectedEventEnvelopeFactory.forRawRecord(
                    record,
                    "DESERIALIZATION_FAILED",
                    "DESERIALIZATION",
                    "null_payload",
                    "Record value is null",
                    null,
                    null,
                    null));
            return;
        }

        String rawJson = new String(record.value, StandardCharsets.UTF_8);
        JsonNode root;
        try {
            root = JsonSupport.MAPPER.readTree(rawJson);
        } catch (Exception ex) {
            LOG.warn("JSON parse failed (topic={}, partition={}, offset={}): {}",
                    record.topic, record.partition, record.offset, ex.getMessage());
            emitDlq(ctx, RejectedEventEnvelopeFactory.forRawRecord(
                    record,
                    "DESERIALIZATION_FAILED",
                    "DESERIALIZATION",
                    "json_parse_error",
                    ex.getMessage(),
                    rawJson,
                    null,
                    record.value));
            return;
        }

        SchemaValidator.ValidationResult validation = validator.validate(root);
        if (!validation.valid) {
            LOG.debug("Schema validation failed (type={}, reason={}, details={})",
                    root.path("type").asText(""), validation.reason, validation.details);
            emitDlq(ctx, RejectedEventEnvelopeFactory.forSchemaValidationFailure(
                    record,
                    root,
                    validation.failureClass,
                    "SCHEMA_VALIDATION",
                    validation.reason,
                    validation.details,
                    rawJson));
            return;
        }

        StreamingEvent event = new StreamingEvent();
        event.eventId = root.path("id").asText("");
        event.eventType = root.path("type").asText("");
        event.eventVersion = validation.eventVersion;
        event.timestamp = JsonNodeUtils.parseTimestampMillisOrDefault(root.path("timestamp"), System.currentTimeMillis());
        event.gateway = root.path("gateway").asText("");
        event.rawJson = rawJson;
        event.replay = root.path("__replay").asBoolean(false);
        event.source = RejectedEventEnvelopeSupport.buildSourcePointer(record);
        event.dimensions = RejectedEventEnvelopeSupport.extractDimensions(root);

        DedupKey dedupKey = DedupKey.build(event, root);
        event.dedupKey = dedupKey.key;
        event.dedupStrategy = dedupKey.strategy;

        LOG.trace("Accepted event (id={}, type={}, dedupKey={})", event.eventId, event.eventType, event.dedupKey);
        acceptedCounter.inc();
        out.collect(new ValidatedEvent(event, root));
    }

    private void emitDlq(Context ctx, RejectedEventEnvelope envelope) {
        dlqCounter.inc();
        dlqSinceLastLog++;
        long now = System.currentTimeMillis();
        if (now - lastDlqLogMs >= 60000) {
            LOG.warn("DLQ rate summary: {} rejects in last 60s", dlqSinceLastLog);
            lastDlqLogMs = now;
            dlqSinceLastLog = 0;
        }
        ctx.output(dlqTag, envelope);
    }

    public static class DedupKey {
        public final String key;
        public final String strategy;

        private DedupKey(String key, String strategy) {
            this.key = key;
            this.strategy = strategy;
        }

        public static DedupKey build(StreamingEvent event, JsonNode root) {
            if (event.eventId != null && !event.eventId.isEmpty()) {
                return new DedupKey(event.eventId, "event_id");
            }
            // Fall back to a canonical payload hash, stripping replay metadata to keep replayed events idempotent.
            String canonical;
            try {
                JsonNode normalized = root;
                if (root != null && root.isObject()) {
                    ObjectNode copy = ((ObjectNode) root).deepCopy();
                    copy.remove("__replay");
                    copy.remove("__replay_ts");
                    copy.remove("__replay_source");
                    copy.remove("__replay_failure_class");
                    normalized = copy;
                }
                canonical = JsonSupport.CANONICAL_MAPPER.writeValueAsString(normalized);
            } catch (Exception ex) {
                canonical = event.rawJson;
            }
            String dims = "";
            if (event.dimensions != null) {
                dims = String.valueOf(event.dimensions.orchestrator) + "|" + event.dimensions.broadcaster + "|" + event.dimensions.region;
            }
            String hash = Hashing.sha256Hex(event.eventType + "|" + dims + "|" + canonical);
            return new DedupKey(hash, "payload_hash");
        }
    }
}
