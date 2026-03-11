package com.livepeer.analytics.quality;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Centralized configuration for the quality gate pipeline, sourced from environment variables.
 */
public class QualityGateConfig implements java.io.Serializable {
    private static final long serialVersionUID = 1L;
    public final String kafkaBootstrap;
    public final List<String> inputTopics;
    /** Maps Kafka topic name to logical org label (e.g. "streaming_events" -> "cloud_spe"). */
    public final Map<String, String> topicOrgMap;
    /**
     * Maps Kafka topic name to offset reset strategy ("earliest" or "latest").
     * Used when no committed offset exists for a topic's partitions.
     * Defaults to "earliest" if a topic is not listed.
     * Set "latest" for newly added topics to avoid replaying historical data on first deployment.
     */
    public final Map<String, String> topicResetStrategyMap;
    public final String dlqTopic;
    public final String quarantineTopic;
    public final String kafkaGroupId;
    public final int dedupTtlMinutes;
    public final Set<String> supportedVersions;

    public final String clickhouseUrl;
    public final String clickhouseUser;
    public final String clickhousePassword;
    public final String clickhouseDatabase;

    public final int clickhouseSinkMaxBatchSize;
    public final int clickhouseSinkMaxInFlightRequests;
    public final int clickhouseSinkMaxBufferedRequests;
    public final int clickhouseSinkMaxBatchSizeBytes;
    public final long clickhouseSinkMaxTimeInBufferMs;
    public final int clickhouseSinkMaxRecordSizeBytes;

    public final Duration metricsRateWindow;

    private QualityGateConfig(
            String kafkaBootstrap,
            List<String> inputTopics,
            Map<String, String> topicOrgMap,
            Map<String, String> topicResetStrategyMap,
            String dlqTopic,
            String quarantineTopic,
            String kafkaGroupId,
            int dedupTtlMinutes,
            Set<String> supportedVersions,
            String clickhouseUrl,
            String clickhouseUser,
            String clickhousePassword,
            String clickhouseDatabase,
            int clickhouseSinkMaxBatchSize,
            int clickhouseSinkMaxInFlightRequests,
            int clickhouseSinkMaxBufferedRequests,
            int clickhouseSinkMaxBatchSizeBytes,
            long clickhouseSinkMaxTimeInBufferMs,
            int clickhouseSinkMaxRecordSizeBytes,
            Duration metricsRateWindow) {
        this.kafkaBootstrap = kafkaBootstrap;
        this.inputTopics = inputTopics;
        this.topicOrgMap = topicOrgMap;
        this.topicResetStrategyMap = topicResetStrategyMap;
        this.dlqTopic = dlqTopic;
        this.quarantineTopic = quarantineTopic;
        this.kafkaGroupId = kafkaGroupId;
        this.dedupTtlMinutes = dedupTtlMinutes;
        this.supportedVersions = supportedVersions;
        this.clickhouseUrl = clickhouseUrl;
        this.clickhouseUser = clickhouseUser;
        this.clickhousePassword = clickhousePassword;
        this.clickhouseDatabase = clickhouseDatabase;
        this.clickhouseSinkMaxBatchSize = clickhouseSinkMaxBatchSize;
        this.clickhouseSinkMaxInFlightRequests = clickhouseSinkMaxInFlightRequests;
        this.clickhouseSinkMaxBufferedRequests = clickhouseSinkMaxBufferedRequests;
        this.clickhouseSinkMaxBatchSizeBytes = clickhouseSinkMaxBatchSizeBytes;
        this.clickhouseSinkMaxTimeInBufferMs = clickhouseSinkMaxTimeInBufferMs;
        this.clickhouseSinkMaxRecordSizeBytes = clickhouseSinkMaxRecordSizeBytes;
        this.metricsRateWindow = metricsRateWindow;
    }

    public static QualityGateConfig fromEnv() {
        String kafkaBootstrap = env("QUALITY_KAFKA_BOOTSTRAP", "kafka:9092");
        // QUALITY_INPUT_TOPICS takes precedence; falls back to QUALITY_INPUT_TOPIC for backward compat.
        String inputTopicsRaw = env("QUALITY_INPUT_TOPICS", env("QUALITY_INPUT_TOPIC", "streaming_events"));
        List<String> inputTopics = Arrays.asList(inputTopicsRaw.split("\\s*,\\s*"));
        // Format: topic1=org1,topic2=org2  e.g. streaming_events=cloud_spe,network_events=daydream
        Map<String, String> topicOrgMap = envTopicOrgMap("QUALITY_TOPIC_ORG_MAP");
        // Format: topic1=earliest,topic2=latest  Defaults to "earliest" if topic not listed.
        // Set "latest" for newly added topics to avoid flooding the pipeline with historical data.
        Map<String, String> topicResetStrategyMap = envTopicOrgMap("QUALITY_TOPIC_RESET_STRATEGY_MAP");
        String dlqTopic = env("QUALITY_DLQ_TOPIC", "events.dlq.streaming_events.v1");
        String quarantineTopic = env("QUALITY_QUARANTINE_TOPIC", "events.quarantine.streaming_events.v1");
        String kafkaGroupId = env("QUALITY_GROUP_ID", "flink-quality-gate-v1");
        int dedupTtlMinutes = envInt("QUALITY_DEDUP_TTL_MINUTES", 1440);
        Set<String> supportedVersions = envSet("QUALITY_SUPPORTED_VERSIONS", "1,v1");

        String clickhouseUrl = env("CLICKHOUSE_URL", "http://clickhouse:8123");
        String clickhouseUser = env("CLICKHOUSE_USER", "analytics_user");
        String clickhousePassword = env("CLICKHOUSE_PASSWORD", "analytics_password");
        String clickhouseDatabase = env("CLICKHOUSE_DATABASE", extractDatabase(clickhouseUrl, "livepeer_analytics"));

        int clickhouseSinkMaxBatchSize = envInt("CLICKHOUSE_SINK_MAX_BATCH_SIZE", 1000);
        int clickhouseSinkMaxInFlightRequests = envInt("CLICKHOUSE_SINK_MAX_IN_FLIGHT", 2);
        int clickhouseSinkMaxBufferedRequests = envInt("CLICKHOUSE_SINK_MAX_BUFFERED", 10000);
        int clickhouseSinkMaxBatchSizeBytes = envInt("CLICKHOUSE_SINK_MAX_BATCH_BYTES", 5_000_000);
        long clickhouseSinkMaxTimeInBufferMs = envLong("CLICKHOUSE_SINK_MAX_TIME_MS", 1000L);
        int clickhouseSinkMaxRecordSizeBytes = envInt("CLICKHOUSE_SINK_MAX_RECORD_BYTES", 1_000_000);

        Duration metricsRateWindow = Duration.ofSeconds(envInt("QUALITY_METRICS_RATE_WINDOW_SEC", 60));

        return new QualityGateConfig(
                kafkaBootstrap,
                inputTopics,
                topicOrgMap,
                topicResetStrategyMap,
                dlqTopic,
                quarantineTopic,
                kafkaGroupId,
                dedupTtlMinutes,
                supportedVersions,
                clickhouseUrl,
                clickhouseUser,
                clickhousePassword,
                clickhouseDatabase,
                clickhouseSinkMaxBatchSize,
                clickhouseSinkMaxInFlightRequests,
                clickhouseSinkMaxBufferedRequests,
                clickhouseSinkMaxBatchSizeBytes,
                clickhouseSinkMaxTimeInBufferMs,
                clickhouseSinkMaxRecordSizeBytes,
                metricsRateWindow);
    }

    private static String env(String key, String defaultValue) {
        String value = System.getenv(key);
        return value == null || value.isEmpty() ? defaultValue : value;
    }

    private static int envInt(String key, int defaultValue) {
        String value = System.getenv(key);
        if (value == null || value.isEmpty()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException ex) {
            return defaultValue;
        }
    }

    private static long envLong(String key, long defaultValue) {
        String value = System.getenv(key);
        if (value == null || value.isEmpty()) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException ex) {
            return defaultValue;
        }
    }

    private static Set<String> envSet(String key, String defaultValue) {
        String value = System.getenv(key);
        String raw = value == null || value.isEmpty() ? defaultValue : value;
        if (raw == null || raw.trim().isEmpty()) {
            return Collections.emptySet();
        }
        return new HashSet<>(Arrays.asList(raw.split("\\s*,\\s*")));
    }

    private static Map<String, String> envTopicOrgMap(String key) {
        String value = System.getenv(key);
        if (value == null || value.trim().isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, String> map = new HashMap<>();
        for (String pair : value.split("\\s*,\\s*")) {
            int eq = pair.indexOf('=');
            if (eq > 0 && eq < pair.length() - 1) {
                map.put(pair.substring(0, eq).trim(), pair.substring(eq + 1).trim());
            }
        }
        return Collections.unmodifiableMap(map);
    }

    private static String extractDatabase(String url, String fallback) {
        if (url == null) {
            return fallback;
        }
        int slash = url.lastIndexOf('/');
        if (slash > 0 && slash < url.length() - 1) {
            String candidate = url.substring(slash + 1);
            if (!candidate.contains("?") && !candidate.contains(":")) {
                return candidate;
            }
        }
        return fallback;
    }

}
