"""Kafka client provider — cross-cutting concern injected at the runtime layer.

This module must not be imported by types, config, repo, or service layers.
"""

from __future__ import annotations

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from src.config.settings import Settings


class KafkaProvider:
    """Wraps Kafka consumer and producer construction with project-standard config."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._brokers = ",".join(settings.brokers)

    def new_consumer(self, extra: dict[str, object] | None = None) -> Consumer:
        """Return a new consumer configured for this service."""
        cfg: dict[str, object] = {
            "bootstrap.servers": self._brokers,
            "group.id": self._settings.kafka_group_id,
            "auto.offset.reset": self._settings.kafka_auto_offset_reset,
            "session.timeout.ms": self._settings.kafka_session_timeout_ms,
            # Never commit offsets automatically — commit only after successful processing.
            "enable.auto.commit": False,
        }
        if extra:
            cfg.update(extra)
        return Consumer(cfg)

    def new_producer(self, extra: dict[str, object] | None = None) -> Producer:
        """Return a new producer configured for this service."""
        cfg: dict[str, object] = {
            "bootstrap.servers": self._brokers,
            # Wait for all in-sync replicas to acknowledge — prioritise durability.
            "acks": "all",
            "enable.idempotence": True,
            "retries": 5,
            "retry.backoff.ms": 200,
        }
        if extra:
            cfg.update(extra)
        return Producer(cfg)

    def ensure_topics(self, topics: list[str], num_partitions: int = 1, replication_factor: int = 1) -> None:
        """Create topics if they do not already exist. Idempotent."""
        admin = AdminClient({"bootstrap.servers": self._brokers})
        new_topics = [NewTopic(t, num_partitions=num_partitions, replication_factor=replication_factor) for t in topics]
        futures = admin.create_topics(new_topics)
        for topic, future in futures.items():
            try:
                future.result()
            except Exception as exc:
                # Topic already exists — not an error.
                if "TOPIC_ALREADY_EXISTS" not in str(exc):
                    raise RuntimeError(f"Failed to create topic {topic!r}: {exc}") from exc
