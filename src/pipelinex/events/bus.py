"""EventBus: in-process pub/sub for events between pipeline components.

Implements the Observer pattern. Listeners subscribe by event *type* (the
class itself), and the bus dispatches each published event to every matching
listener concurrently. One slow listener cannot block another.

Subscription is by exact type — no inheritance walking — so adding a new
event type is fully decoupled. The plan's "new listeners require zero
changes to detectors" guarantee depends on this.
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

logger = logging.getLogger(__name__)

E = TypeVar("E")
Handler = Callable[[Any], Awaitable[None]]


class EventBus:
    """Concurrent in-process event bus."""

    def __init__(self) -> None:
        self._handlers: dict[type, list[Handler]] = defaultdict(list)

    def subscribe(self, event_type: type[E], handler: Callable[[E], Awaitable[None]]) -> None:
        """Subscribe a handler to events of ``event_type`` (exact match)."""
        self._handlers[event_type].append(handler)

    def unsubscribe(
        self, event_type: type[E], handler: Callable[[E], Awaitable[None]]
    ) -> None:
        if handler in self._handlers.get(event_type, []):
            self._handlers[event_type].remove(handler)

    async def publish(self, event: object) -> None:
        """Dispatch ``event`` to every subscriber of its exact type, concurrently.

        Listener exceptions are logged and swallowed — one bad listener
        cannot poison the bus.
        """
        handlers = list(self._handlers.get(type(event), ()))
        if not handlers:
            return

        results = await asyncio.gather(
            *(self._safe_call(h, event) for h in handlers),
            return_exceptions=True,
        )
        for result in results:
            if isinstance(result, BaseException):
                logger.warning("listener raised: %s", result)

    @staticmethod
    async def _safe_call(handler: Handler, event: object) -> None:
        try:
            await handler(event)
        except Exception as e:
            logger.exception("event handler failed: %s", e)
            raise

    def listener_count(self, event_type: type) -> int:
        return len(self._handlers.get(event_type, []))
