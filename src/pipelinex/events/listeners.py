"""Concrete event listeners.

These are intentionally tiny — the point is to demonstrate that a new
listener can be added without modifying the detector that produces the
event. Production-grade integrations (Slack, PagerDuty, S3 audit log) would
follow the same shape.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

from pipelinex.events.events import AnomalyEventPublished, BlockTraceClosed
from pipelinex.observability.metrics import MetricsRegistry, default_registry

logger = logging.getLogger("pipelinex.alerts")


class ConsoleAlertListener:
    """Log every anomaly to stdout."""

    @property
    def name(self) -> str:
        return "console_alert"

    async def on_anomaly(self, event: AnomalyEventPublished) -> None:
        logger.warning(
            "ANOMALY detector=%s score=%.3f log=%s metadata=%s",
            event.anomaly.detector_name,
            event.anomaly.severity_score,
            event.anomaly.log_record_id,
            event.anomaly.metadata,
        )


class CollectingListener:
    """Test/debug listener: records every event in memory."""

    def __init__(self) -> None:
        self.events: list[Any] = []

    @property
    def name(self) -> str:
        return "collecting"

    async def on_event(self, event: Any) -> None:
        self.events.append(event)


class MetricsListener:
    """Increments a counter when an event fires."""

    def __init__(self, registry: MetricsRegistry | None = None) -> None:
        self._registry = registry or default_registry

    @property
    def name(self) -> str:
        return "metrics"

    async def on_anomaly(self, event: AnomalyEventPublished) -> None:
        await self._registry.inc(f"anomalies.{event.anomaly.detector_name}")

    async def on_trace_closed(self, event: BlockTraceClosed) -> None:
        await self._registry.inc(f"traces.closed.{event.trace.closed_reason}")


def make_collecting_handler(
    listener: CollectingListener,
) -> Callable[[Any], Any]:
    return listener.on_event
