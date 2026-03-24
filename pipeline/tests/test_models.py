"""Tests for domain models (types layer).

Validates that schema contracts are enforced at the boundary.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from uuid import uuid4

import pytest
from pydantic import ValidationError

from src.types.models import AggregatedWindow, Alert, EventType, RawEvent


def make_raw_event(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = {
        "event_id": str(uuid4()),
        "event_type": "stream_started",
        "node_id": "node-1",
        "stream_id": "stream-abc",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "payload": {},
    }
    base.update(overrides)
    return base


class TestRawEvent:
    def test_valid_event_parses(self) -> None:
        data = make_raw_event()
        event = RawEvent.model_validate(data)
        assert event.event_type == EventType.STREAM_STARTED
        assert event.node_id == "node-1"

    def test_roundtrip_json(self) -> None:
        data = make_raw_event()
        event = RawEvent.model_validate(data)
        recovered = RawEvent.model_validate(json.loads(event.model_dump_json()))
        assert event == recovered

    def test_invalid_event_type_raises(self) -> None:
        data = make_raw_event(event_type="not_a_real_type")
        with pytest.raises(ValidationError):
            RawEvent.model_validate(data)

    def test_missing_required_field_raises(self) -> None:
        data = make_raw_event()
        del data["node_id"]
        with pytest.raises(ValidationError):
            RawEvent.model_validate(data)

    def test_model_is_immutable(self) -> None:
        event = RawEvent.model_validate(make_raw_event())
        with pytest.raises(Exception):
            event.node_id = "other"  # type: ignore[misc]


class TestAggregatedWindow:
    def test_success_rate_out_of_range_raises(self) -> None:
        now = datetime.now(timezone.utc)
        with pytest.raises(ValidationError):
            AggregatedWindow(
                window_start=now,
                window_end=now,
                node_id="n",
                stream_count=1,
                transcode_success_rate=1.5,  # invalid
                total_duration_seconds=0.0,
            )

    def test_negative_duration_raises(self) -> None:
        now = datetime.now(timezone.utc)
        with pytest.raises(ValidationError):
            AggregatedWindow(
                window_start=now,
                window_end=now,
                node_id="n",
                stream_count=1,
                transcode_success_rate=1.0,
                total_duration_seconds=-1.0,  # invalid
            )
