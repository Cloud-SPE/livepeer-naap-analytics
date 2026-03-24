"""Kafka sampling — reads the last N messages from each topic partition.

Connects to kafka.livepeer.cloud:9092 (plaintext) and samples
network_events and streaming_events topics going back up to `lookback_hours`.
Falls back to a flat per-partition message limit if timestamp-based seeking fails.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Any

from confluent_kafka import Consumer, KafkaError, TopicPartition
from confluent_kafka.admin import AdminClient

BROKER = "infra1.livepeer.cloud:9092"
TOPICS = ["network_events", "streaming_events"]

# How far back to look and hard ceiling per topic
DEFAULT_LOOKBACK_HOURS = 3
DEFAULT_MAX_MESSAGES = 10_000
POLL_TIMEOUT_SEC = 2.0
IDLE_ROUNDS_BEFORE_STOP = 5  # stop polling after this many empty rounds


@dataclass
class RawMessage:
    topic: str
    partition: int
    offset: int
    timestamp_ms: int
    value: dict[str, Any]


@dataclass
class SampleResult:
    topic: str
    messages: list[RawMessage] = field(default_factory=list)
    parse_errors: int = 0
    partitions_sampled: int = 0


def _build_consumer(broker: str = BROKER) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": broker,
            "group.id": "naap-inspector-" + str(int(time.time())),
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
            "socket.timeout.ms": 10_000,
            "session.timeout.ms": 30_000,
        }
    )


def _get_partitions(topic: str, broker: str = BROKER) -> list[int]:
    admin = AdminClient({"bootstrap.servers": broker, "socket.timeout.ms": 10_000})
    meta = admin.list_topics(topic=topic, timeout=15)
    if topic not in meta.topics:
        return []
    return list(meta.topics[topic].partitions.keys())


def _seek_to_timestamp(consumer: Consumer, tps: list[TopicPartition], lookback_ms: int) -> list[TopicPartition]:
    """Return TopicPartitions seeked to `lookback_ms` ago, falling back to end - chunk."""
    target_ts = int(time.time() * 1000) - lookback_ms
    ts_tps = [TopicPartition(tp.topic, tp.partition, target_ts) for tp in tps]
    resolved = consumer.offsets_for_times(ts_tps, timeout=15)
    result = []
    for i, rtp in enumerate(resolved):
        if rtp.offset >= 0:
            result.append(rtp)
        else:
            # Timestamp is before the earliest available offset — use end minus a chunk.
            # This handles topics with short retention or sparse partitions.
            lo, hi = consumer.get_watermark_offsets(tps[i], timeout=10)
            chunk = max(0, hi - (DEFAULT_MAX_MESSAGES // max(len(tps), 1)))
            result.append(TopicPartition(tps[i].topic, tps[i].partition, max(lo, chunk)))
    return result


def sample_topic(
    topic: str,
    broker: str = BROKER,
    lookback_hours: float = DEFAULT_LOOKBACK_HOURS,
    max_messages: int = DEFAULT_MAX_MESSAGES,
) -> SampleResult:
    result = SampleResult(topic=topic)
    partition_ids = _get_partitions(topic, broker=broker)
    if not partition_ids:
        return result

    result.partitions_sampled = len(partition_ids)
    consumer = _build_consumer(broker=broker)

    try:
        tps = [TopicPartition(topic, p) for p in partition_ids]
        lookback_ms = int(lookback_hours * 3600 * 1000)
        seeked = _seek_to_timestamp(consumer, tps, lookback_ms)
        consumer.assign(seeked)

        idle_rounds = 0
        while len(result.messages) < max_messages and idle_rounds < IDLE_ROUNDS_BEFORE_STOP:
            msg = consumer.poll(timeout=POLL_TIMEOUT_SEC)
            if msg is None:
                idle_rounds += 1
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:  # noqa: SLF001
                    idle_rounds += 1
                continue

            idle_rounds = 0
            raw = msg.value()
            if not raw:
                continue

            try:
                value = json.loads(raw)
            except (json.JSONDecodeError, UnicodeDecodeError):
                result.parse_errors += 1
                continue

            result.messages.append(
                RawMessage(
                    topic=msg.topic(),
                    partition=msg.partition(),
                    offset=msg.offset(),
                    timestamp_ms=msg.timestamp()[1] if msg.timestamp()[0] else 0,
                    value=value,
                )
            )
    finally:
        consumer.close()

    return result
