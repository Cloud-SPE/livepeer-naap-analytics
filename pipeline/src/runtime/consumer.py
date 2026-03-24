"""Kafka consumer runtime — entry point for the pipeline.

Layer 5 — wires together config, providers, repo, and service.
This is the only layer permitted to import from all other layers.
"""

from __future__ import annotations

import json
import signal
import sys
from types import FrameType

import structlog
from confluent_kafka import KafkaError, KafkaException, Message
from pydantic import ValidationError

from src.config.settings import Settings
from src.providers.kafka import KafkaProvider
from src.providers.telemetry import TelemetryProvider
from src.service.processor import EventProcessor
from src.types.models import RawEvent

logger = structlog.get_logger(__name__)


class KafkaConsumer:
    """Consumes raw events from Kafka, validates, processes, and commits offsets.

    Design decisions:
    - Manual offset commit after successful processing (no data loss on crash).
    - Graceful shutdown on SIGINT/SIGTERM — in-flight message is completed first.
    - Pydantic validation at the boundary — invalid messages are logged and skipped,
      not propagated to the service layer.
    """

    def __init__(
        self,
        settings: Settings,
        kafka: KafkaProvider,
        processor: EventProcessor,
        telemetry: TelemetryProvider,
    ) -> None:
        self._settings = settings
        self._kafka = kafka
        self._processor = processor
        self._telemetry = telemetry
        self._running = False

    def run(self) -> None:
        """Blocking consumer loop. Exits on SIGINT/SIGTERM."""
        self._running = True
        self._register_signals()

        self._kafka.ensure_topics([self._settings.topic_raw_events])
        consumer = self._kafka.new_consumer()
        consumer.subscribe([self._settings.topic_raw_events])

        logger.info("consumer_started", topic=self._settings.topic_raw_events)

        try:
            while self._running:
                msg: Message | None = consumer.poll(timeout=1.0)

                if msg is None:
                    continue
                if msg.error():
                    self._handle_error(msg)
                    continue

                self._handle_message(consumer, msg)
        except KafkaException as exc:
            logger.error("kafka_exception", error=str(exc))
            sys.exit(1)
        finally:
            consumer.close()
            self._telemetry.shutdown()
            logger.info("consumer_stopped")

    def _handle_message(self, consumer: object, msg: Message) -> None:
        log = logger.bind(
            topic=msg.topic(),
            partition=msg.partition(),
            offset=msg.offset(),
        )

        raw_value = msg.value()
        if raw_value is None:
            log.warning("null_message_value_skipped")
            consumer.commit(message=msg, asynchronous=False)  # type: ignore[union-attr]
            return

        try:
            data = json.loads(raw_value)
            event = RawEvent.model_validate(data)
        except (json.JSONDecodeError, ValidationError) as exc:
            # Invalid messages are skipped and committed — not re-queued.
            # This prevents a bad message from blocking the partition forever.
            # If data loss is a concern for your SLA, route invalid messages
            # to a dead-letter topic instead. See docs/exec-plans/ for that work.
            log.warning("invalid_message_skipped", error=str(exc))
            consumer.commit(message=msg, asynchronous=False)  # type: ignore[union-attr]
            return

        log = log.bind(event_id=str(event.event_id), event_type=event.event_type)
        log.debug("event_received")

        result = self._processor.process(event)
        if result is not None:
            log.info("window_produced", window_start=str(result.window_start))

        # Commit only after successful processing.
        consumer.commit(message=msg, asynchronous=False)  # type: ignore[union-attr]

    @staticmethod
    def _handle_error(msg: Message) -> None:
        err = msg.error()
        if err.code() == KafkaError._PARTITION_EOF:  # noqa: SLF001
            logger.debug("partition_eof", topic=msg.topic(), partition=msg.partition())
        else:
            logger.error("consumer_error", error=str(err))

    def _register_signals(self) -> None:
        def _handle(signum: int, _frame: FrameType | None) -> None:
            logger.info("shutdown_signal_received", signal=signum)
            self._running = False

        signal.signal(signal.SIGINT, _handle)
        signal.signal(signal.SIGTERM, _handle)


def main() -> None:
    settings = Settings()  # type: ignore[call-arg]
    telemetry = TelemetryProvider(settings)
    kafka = KafkaProvider(settings)
    processor = EventProcessor()
    consumer = KafkaConsumer(settings=settings, kafka=kafka, processor=processor, telemetry=telemetry)
    consumer.run()


if __name__ == "__main__":
    main()
