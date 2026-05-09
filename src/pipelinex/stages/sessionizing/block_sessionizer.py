"""BlockSessionizerStage — group HDFS log lines by block_id into traces.

Architecture
------------
The stage observes records as they flow through the pipeline. For each
record carrying ``enrichment["block_id"]`` it:

1. Looks up (or creates) an open trace for that block_id.
2. Appends the record's ``event_id`` to the trace's event sequence and the
   record id to the trace's record_ids tuple.
3. Updates first/last timestamps.
4. The original record continues downstream unchanged — this is a *tee*,
   not a transform. The plan calls this out as a critical decision: every
   downstream consumer (storage, point detectors) still sees per-line records.

When does a trace close?
~~~~~~~~~~~~~~~~~~~~~~~~
Four triggers, in priority order:

- **Hard cap**: ``record_count > max_records_per_trace`` → ``"evicted"``.
- **Terminal event**: an event_id in ``terminal_events`` was observed →
  the next janitor pass closes the trace as ``"terminal"`` (we don't close
  immediately so out-of-order arrivals within a small window can attach).
- **Timeout**: ``last_timestamp`` older than ``idle_window_s`` log-time →
  ``"timeout"``.
- **Shutdown**: pipeline is shutting down → all open traces are flushed as
  ``"shutdown"``.

LRU eviction
~~~~~~~~~~~~
If open-trace count exceeds ``max_open_traces``, the oldest trace (by
``last_timestamp``) is force-emitted with ``"evicted"``. This guards the
~575K-block HDFS corpus from OOM on a laptop.

Memory footprint
~~~~~~~~~~~~~~~~
Each open trace stores ``record_ids`` (UUIDs) and ``event_sequence`` (short
strings). Critically, **no full LogRecords are held**. The detector that
needs raw records queries the repository.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections import OrderedDict
from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Final
from uuid import UUID

from pipelinex.core.interfaces import IPipelineStage
from pipelinex.core.models import BlockTrace, ClosedReason, LogRecord
from pipelinex.events.bus import EventBus
from pipelinex.events.events import BlockTraceClosed

logger = logging.getLogger(__name__)

# HDFS event ids that signal "block lifecycle complete" — used as the
# terminal-event hint to the janitor. E9 = "Received block ... of size ...",
# E21 = "Deleting block ...".
DEFAULT_TERMINAL_EVENTS: Final = frozenset({"E9", "E21"})


class _OpenTrace:
    """Mutable scratch-pad while a block is in flight."""

    __slots__ = (
        "block_id",
        "events",
        "first_ts",
        "last_ts",
        "record_ids",
        "terminal_seen",
    )

    def __init__(
        self, block_id: str, first_ts: datetime, last_ts: datetime
    ) -> None:
        self.block_id = block_id
        self.events: list[str] = []
        self.record_ids: list[UUID] = []
        self.first_ts = first_ts
        self.last_ts = last_ts
        self.terminal_seen = False

    def add(self, event_id: str | None, rid: UUID, ts: datetime | None) -> None:
        if event_id:
            self.events.append(event_id)
        self.record_ids.append(rid)
        if ts is not None:
            if self.first_ts is None or ts < self.first_ts:
                self.first_ts = ts
            if self.last_ts is None or ts > self.last_ts:
                self.last_ts = ts


class BlockSessionizerStage(IPipelineStage):
    """Stateful pipeline stage that fans BlockTrace events out to the bus."""

    def __init__(
        self,
        bus: EventBus,
        pipeline_run_id: UUID,
        idle_window_s: float = 30.0,
        max_records_per_trace: int = 1000,
        max_open_traces: int = 50_000,
        terminal_events: Iterable[str] = DEFAULT_TERMINAL_EVENTS,
        flush_interval_s: float = 5.0,
        block_id_key: str = "block_id",
        event_id_key: str = "event_id",
    ) -> None:
        if idle_window_s <= 0:
            raise ValueError("idle_window_s must be > 0")
        if max_records_per_trace <= 0:
            raise ValueError("max_records_per_trace must be > 0")
        if max_open_traces <= 0:
            raise ValueError("max_open_traces must be > 0")

        self._bus = bus
        self._run_id = pipeline_run_id
        self._idle_window_s = idle_window_s
        self._max_per_trace = max_records_per_trace
        self._max_open = max_open_traces
        self._terminal = frozenset(terminal_events)
        self._flush_interval_s = flush_interval_s
        self._block_id_key = block_id_key
        self._event_id_key = event_id_key

        # OrderedDict gives us LRU semantics for eviction by simply popping
        # the front when over capacity.
        self._open: OrderedDict[str, _OpenTrace] = OrderedDict()
        self._lock = asyncio.Lock()
        self._latest_log_ts: datetime | None = None

        self._janitor_task: asyncio.Task[None] | None = None
        self._stopped = asyncio.Event()
        self._traces_emitted = 0

    @property
    def name(self) -> str:
        return "sessionize:block"

    @property
    def open_count(self) -> int:
        return len(self._open)

    @property
    def traces_emitted(self) -> int:
        return self._traces_emitted

    async def start(self) -> None:
        """Launch the janitor coroutine that handles timeout-based emission."""
        if self._janitor_task is None:
            self._stopped.clear()
            self._janitor_task = asyncio.create_task(
                self._janitor_loop(), name="sessionizer-janitor"
            )

    async def stop(self) -> None:
        """Stop the janitor and flush every still-open trace."""
        self._stopped.set()
        if self._janitor_task is not None:
            self._janitor_task.cancel()
            with contextlib.suppress(BaseException):
                await self._janitor_task
            self._janitor_task = None
        async with self._lock:
            keys = list(self._open.keys())
            for k in keys:
                trace = self._open.pop(k)
                await self._emit(trace, "shutdown")

    async def process(self, record: LogRecord) -> LogRecord:
        block_id = record.enrichment.get(self._block_id_key)
        if not isinstance(block_id, str) or not block_id:
            return record

        event_id = record.enrichment.get(self._event_id_key)
        ts = record.timestamp
        async with self._lock:
            if ts is not None and (
                self._latest_log_ts is None or ts > self._latest_log_ts
            ):
                self._latest_log_ts = ts

            trace = self._open.get(block_id)
            if trace is None:
                trace = _OpenTrace(
                    block_id=block_id,
                    first_ts=ts or _utcnow(),
                    last_ts=ts or _utcnow(),
                )
                self._open[block_id] = trace
            else:
                self._open.move_to_end(block_id)

            trace.add(
                event_id if isinstance(event_id, str) else None,
                record.id,
                ts,
            )
            if event_id in self._terminal:
                trace.terminal_seen = True

            # Hard cap.
            if len(trace.events) >= self._max_per_trace:
                self._open.pop(block_id, None)
                await self._emit(trace, "evicted")
            # LRU eviction when over capacity.
            elif len(self._open) > self._max_open:
                _, victim = self._open.popitem(last=False)
                await self._emit(victim, "evicted")

        return record

    # ---------- private ----------

    async def _emit(self, trace: _OpenTrace, reason: ClosedReason) -> None:
        bt = BlockTrace(
            block_id=trace.block_id,
            event_sequence=tuple(trace.events),
            record_ids=tuple(trace.record_ids),
            first_timestamp=trace.first_ts,
            last_timestamp=trace.last_ts,
            record_count=len(trace.record_ids),
            pipeline_run_id=self._run_id,
            closed_reason=reason,
        )
        self._traces_emitted += 1
        try:
            await self._bus.publish(BlockTraceClosed(trace=bt))
        except Exception as e:  # pragma: no cover — bus has its own try/except
            logger.warning("failed to publish BlockTraceClosed: %s", e)

    async def _janitor_loop(self) -> None:
        try:
            while not self._stopped.is_set():
                await asyncio.sleep(self._flush_interval_s)
                await self._janitor_pass()
        except asyncio.CancelledError:
            return

    async def _janitor_pass(self) -> None:
        async with self._lock:
            if not self._open:
                return
            cutoff_ts = self._latest_log_ts
            if cutoff_ts is None:
                return
            cutoff = cutoff_ts.timestamp() - self._idle_window_s

            to_close: list[tuple[str, _OpenTrace, ClosedReason]] = []
            # Iterate in insertion order; first traces are oldest.
            for block_id, trace in list(self._open.items()):
                age_pass = trace.last_ts.timestamp() < cutoff
                if trace.terminal_seen:
                    to_close.append((block_id, trace, "terminal"))
                elif age_pass:
                    to_close.append((block_id, trace, "timeout"))

            for block_id, trace, reason in to_close:
                self._open.pop(block_id, None)
                await self._emit(trace, reason)


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _wall_now() -> float:
    return time.monotonic()
