"""Domain types for the NAAP Analytics pipeline.

Layer 1 — no imports from other internal modules.
All Kafka message schemas are defined here as Pydantic models.
Messages are validated against these models at the boundary (on consume).
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field


class EventType(str, Enum):
    STREAM_STARTED = "stream_started"
    STREAM_ENDED = "stream_ended"
    TRANSCODE_COMPLETED = "transcode_completed"
    TRANSCODE_FAILED = "transcode_failed"


class RawEvent(BaseModel):
    """A raw network event as received from the naap.events.raw Kafka topic."""

    event_id: UUID
    event_type: EventType
    node_id: str
    stream_id: str
    timestamp: datetime
    payload: dict[str, Any] = Field(default_factory=dict)

    model_config = {"frozen": True}


class AggregatedWindow(BaseModel):
    """A time-windowed analytics aggregate emitted to naap.analytics.aggregated."""

    window_start: datetime
    window_end: datetime
    node_id: str
    stream_count: int
    transcode_success_rate: float = Field(ge=0.0, le=1.0)
    total_duration_seconds: float = Field(ge=0.0)

    model_config = {"frozen": True}


class Alert(BaseModel):
    """A threshold breach alert emitted to naap.alerts."""

    alert_id: UUID
    alert_type: str
    node_id: str
    triggered_at: datetime
    threshold: float
    observed_value: float

    model_config = {"frozen": True}
