"""Tests for the event processor (service layer)."""

from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from src.service.processor import EventProcessor
from src.types.models import EventType, RawEvent


def make_event(event_type: EventType = EventType.STREAM_STARTED) -> RawEvent:
    return RawEvent(
        event_id=uuid4(),
        event_type=event_type,
        node_id="node-1",
        stream_id="stream-abc",
        timestamp=datetime.now(timezone.utc),
        payload={},
    )


class TestEventProcessor:
    def test_process_returns_none_for_stub(self) -> None:
        """Processor stub returns None until windowing logic is implemented."""
        processor = EventProcessor()
        result = processor.process(make_event())
        assert result is None

    def test_process_all_event_types_without_error(self) -> None:
        processor = EventProcessor()
        for event_type in EventType:
            result = processor.process(make_event(event_type))
            assert result is None  # stub behaviour
