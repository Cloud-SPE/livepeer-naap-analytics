"""Event processing service — business logic layer.

Layer 4 — depends on types, config, repo interfaces.
No Kafka, no HTTP, no framework concerns here.
"""

from __future__ import annotations

import structlog

from src.types.models import AggregatedWindow, RawEvent

logger = structlog.get_logger(__name__)


class EventProcessor:
    """Processes raw Livepeer network events into analytics aggregates.

    Aggregation strategy and thresholds are intentional design decisions.
    See docs/product-specs/ for the full specification.
    """

    def process(self, event: RawEvent) -> AggregatedWindow | None:
        """Process a single raw event.

        Returns an AggregatedWindow if the event completes a window, else None.
        This method must be pure — no I/O, no side effects.
        """
        log = logger.bind(event_id=str(event.event_id), event_type=event.event_type)
        log.debug("processing_event")

        # TODO: implement windowing and aggregation logic.
        # This stub is intentionally minimal — replace with real logic via an exec-plan.
        # See docs/exec-plans/active/ for planned work.
        return None
