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
-- num_consumers: network_events is high-volume (~4.6M events/day, 16 partitions).
-- streaming_events is low-volume (~35K events/day). Consumers ≤ partitions.
--
-- ClickHouse Kafka Engine does not support ALTER TABLE MODIFY SETTING, so all
-- connection and behaviour settings must be supplied at CREATE TABLE time.
-- Both are substituted from environment variables by the init script:
--   KAFKA_BROKER_LIST       — broker address (e.g. infra2.cloudspe.com:9092)
--   KAFKA_AUTO_OFFSET_RESET — earliest (full history) | latest (new data only)
--
-- To change these on a running instance, drop and recreate the tables (and
-- their dependent MVs). See infra/clickhouse/README.md for the procedure.

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
    kafka_num_consumers    = 2;

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
    kafka_num_consumers    = 1;
