-- Migration 003: Kafka engine tables
--
-- One Kafka engine table per topic. These are ephemeral read buffers —
-- they do not store data. Data flows: Kafka table → MV → events table.
--
-- Consumer groups (verified non-overlapping with Python pipeline group
-- 'naap-analytics-pipeline'):
--   clickhouse-naap-network   → network_events topic
--   clickhouse-naap-streaming → streaming_events topic
--
-- kafka_skip_broken_messages = 1: malformed JSON is skipped with a warning
-- rather than stalling the consumer. Broken messages are counted in
-- system.kafka_consumers for monitoring.
--
-- num_consumers: network_events is high-volume (~33M backlog, 16 partitions).
-- streaming_events is low-volume (~250K backlog). Consumers ≤ partitions.
--
-- kafka_consumer_config: auto.offset.reset=earliest ensures the full topic
-- history is consumed when the consumer group has no committed offsets (i.e.
-- on first run against a new broker). Without this, librdkafka defaults to
-- 'latest' and all historical data is skipped.
--
-- KAFKA_BROKER_LIST is substituted by the init script from the environment.
-- To change the broker: ALTER TABLE naap.kafka_network_events MODIFY SETTING
--   kafka_broker_list = 'new-broker:9092';

CREATE TABLE IF NOT EXISTS naap.kafka_network_events
(
    -- Envelope fields from network_events topic messages.
    -- data is stored as String to handle both object and array payloads
    -- (network_capabilities data is a JSON array; others are objects).
    id        String,
    type      String,
    timestamp String,
    gateway   String,
    data      String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list      = '${KAFKA_BROKER_LIST}',
    kafka_topic_list       = 'network_events',
    kafka_group_name       = 'clickhouse-naap-network',
    kafka_format           = 'JSONEachRow',
    kafka_skip_broken_messages = 100,
    kafka_num_consumers    = 2,
    kafka_consumer_config  = 'auto.offset.reset=earliest';

CREATE TABLE IF NOT EXISTS naap.kafka_streaming_events
(
    id        String,
    type      String,
    timestamp String,
    gateway   String,
    data      String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list      = '${KAFKA_BROKER_LIST}',
    kafka_topic_list       = 'streaming_events',
    kafka_group_name       = 'clickhouse-naap-streaming',
    kafka_format           = 'JSONEachRow',
    kafka_skip_broken_messages = 100,
    kafka_num_consumers    = 1,
    kafka_consumer_config  = 'auto.offset.reset=earliest';
