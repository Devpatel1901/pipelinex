"""Repository circuit breaker — fail fast when persistence is unhealthy.

When the connection pool saturates repeatedly inside a short window, the
breaker opens and rejects subsequent writes immediately with
``LoadSheddingError``. This keeps the pipeline responsive (the executor
counts sheds and continues) instead of blocking workers on a sick database.

State machine
-------------
- **CLOSED** (healthy): all calls pass through. ``PoolSaturatedError``s
  are counted in a rolling window.
- **OPEN** (unhealthy): every call raises ``LoadSheddingError`` without
  touching the DB. After ``cooldown_s`` seconds the breaker transitions
  to HALF_OPEN.
- **HALF_OPEN** (probing): the next call is admitted. On success the
  breaker returns to CLOSED; on failure it returns to OPEN with a fresh
  cooldown.

The breaker wraps any ``ILogRepository`` — same interface, same Liskov
substitutability. The wrapper is opt-in: set
``RepositoryConfig.circuit_breaker.enabled = false`` to bypass it.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Final

from pipelinex.core.exceptions import LoadSheddingError, PoolSaturatedError
from pipelinex.core.interfaces import (
    AggregateResult,
    Aggregation,
    ILogRepository,
    QueryFilters,
)
from pipelinex.core.models import AnomalyEvent, BlockTrace, LogRecord

logger = logging.getLogger(__name__)

DEFAULT_FAILURE_THRESHOLD: Final = 5
DEFAULT_WINDOW_SECONDS: Final = 30.0
DEFAULT_COOLDOWN_S: Final = 10.0


class BreakerState(StrEnum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


@dataclass
class _BreakerCounters:
    failure_timestamps: deque[float]
    opened_at: float | None = None


class RepositoryCircuitBreaker(ILogRepository):
    """Wrap an ``ILogRepository`` with pool-saturation circuit-breaker logic."""

    def __init__(
        self,
        inner: ILogRepository,
        failure_threshold: int = DEFAULT_FAILURE_THRESHOLD,
        window_seconds: float = DEFAULT_WINDOW_SECONDS,
        cooldown_s: float = DEFAULT_COOLDOWN_S,
    ) -> None:
        if failure_threshold < 1:
            raise ValueError("failure_threshold must be >= 1")
        if window_seconds <= 0:
            raise ValueError("window_seconds must be > 0")
        if cooldown_s < 0:
            raise ValueError("cooldown_s must be >= 0")

        self._inner = inner
        self._threshold = failure_threshold
        self._window_s = window_seconds
        self._cooldown_s = cooldown_s
        self._state = BreakerState.CLOSED
        self._counters = _BreakerCounters(failure_timestamps=deque())
        self._lock = asyncio.Lock()

    @property
    def state(self) -> BreakerState:
        return self._state

    @property
    def inner(self) -> ILogRepository:
        return self._inner

    # ---------- writes ----------

    async def save(self, record: LogRecord) -> None:
        await self._gated_call(self._inner.save, record)

    async def save_batch(self, records: list[LogRecord]) -> None:
        await self._gated_call(self._inner.save_batch, records)

    async def save_trace(self, trace: BlockTrace) -> None:
        await self._gated_call(self._inner.save_trace, trace)

    async def save_trace_batch(self, traces: list[BlockTrace]) -> None:
        await self._gated_call(self._inner.save_trace_batch, traces)

    async def save_anomaly(self, anomaly: AnomalyEvent) -> None:
        await self._gated_call(self._inner.save_anomaly, anomaly)

    # ---------- reads (not gated — reads can't make the situation worse) ----------

    async def query(self, filters: QueryFilters) -> list[LogRecord]:
        return await self._inner.query(filters)

    async def aggregate(self, agg: Aggregation) -> AggregateResult:
        return await self._inner.aggregate(agg)

    # ---------- breaker core ----------

    async def _gated_call(
        self, fn: Callable[..., Awaitable[Any]], *args: Any
    ) -> None:
        async with self._lock:
            self._maybe_transition_from_open()
            if self._state is BreakerState.OPEN:
                raise LoadSheddingError(
                    "circuit breaker is OPEN - repository is shedding load"
                )
        try:
            await fn(*args)
        except PoolSaturatedError:
            await self._record_failure()
            raise
        else:
            await self._record_success()

    def _maybe_transition_from_open(self) -> None:
        if self._state is not BreakerState.OPEN:
            return
        if self._counters.opened_at is None:
            return
        if time.monotonic() - self._counters.opened_at >= self._cooldown_s:
            self._state = BreakerState.HALF_OPEN
            logger.info("circuit breaker transitioning OPEN → HALF_OPEN")

    async def _record_failure(self) -> None:
        async with self._lock:
            now = time.monotonic()
            self._counters.failure_timestamps.append(now)
            self._prune_old_failures(now)

            if self._state is BreakerState.HALF_OPEN:
                self._open(now, reason="half-open probe failed")
                return

            if len(self._counters.failure_timestamps) >= self._threshold:
                self._open(now, reason="failure threshold exceeded")

    async def _record_success(self) -> None:
        async with self._lock:
            if self._state is BreakerState.HALF_OPEN:
                logger.info("circuit breaker recovering: HALF_OPEN → CLOSED")
                self._state = BreakerState.CLOSED
                self._counters.failure_timestamps.clear()
                self._counters.opened_at = None

    def _prune_old_failures(self, now: float) -> None:
        cutoff = now - self._window_s
        while self._counters.failure_timestamps and self._counters.failure_timestamps[0] < cutoff:
            self._counters.failure_timestamps.popleft()

    def _open(self, now: float, *, reason: str) -> None:
        logger.warning(
            "circuit breaker tripping CLOSED → OPEN (%s, %d failures in %.1fs)",
            reason,
            len(self._counters.failure_timestamps),
            self._window_s,
        )
        self._state = BreakerState.OPEN
        self._counters.opened_at = now
