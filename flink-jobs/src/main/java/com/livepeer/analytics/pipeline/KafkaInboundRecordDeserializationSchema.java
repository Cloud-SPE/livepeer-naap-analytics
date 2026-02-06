package com.livepeer.analytics.pipeline;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import com.livepeer.analytics.model.KafkaInboundRecord;

import java.nio.charset.StandardCharsets;

public class KafkaInboundRecordDeserializationSchema implements KafkaRecordDeserializationSchema<KafkaInboundRecord> {
    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<KafkaInboundRecord> out) {
        KafkaInboundRecord inbound = new KafkaInboundRecord();
        inbound.topic = record.topic();
        inbound.partition = record.partition();
        inbound.offset = record.offset();
        inbound.recordTimestamp = record.timestamp();
        inbound.value = record.value();
        inbound.key = record.key() == null ? null : new String(record.key(), StandardCharsets.UTF_8);
        if (record.headers() != null) {
            for (Header header : record.headers()) {
                if (header.value() != null) {
                    inbound.headers.put(header.key(), new String(header.value(), StandardCharsets.UTF_8));
                }
            }
        }
        out.collect(inbound);
    }

    @Override
    public TypeInformation<KafkaInboundRecord> getProducedType() {
        return TypeInformation.of(KafkaInboundRecord.class);
    }
}
