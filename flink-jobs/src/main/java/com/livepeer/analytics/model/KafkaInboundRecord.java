package com.livepeer.analytics.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Minimal Kafka record wrapper used by the Flink source.
 */
public class KafkaInboundRecord implements Serializable {
    private static final long serialVersionUID = 1L;

    public String topic;
    public int partition;
    public long offset;
    public long recordTimestamp;
    public byte[] value;
    public String key;
    public Map<String, String> headers = new HashMap<>();

    public KafkaInboundRecord() {}
}
