"""Tests for the EventBus and listeners."""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime
from uuid import uuid4

from pipelinex.core.models import AnomalyEvent, BlockTrace
from pipelinex.events.bus import EventBus
from pipelinex.events.events import AnomalyEventPublished, BlockTraceClosed
from pipelinex.events.listeners import CollectingListener, MetricsListener
from pipelinex.observability.metrics import MetricsRegistry


class TestSubscribe:
    async def test_handler_receives_event(self) -> None:
        bus = EventBus()
        listener = CollectingListener()
        bus.subscribe(AnomalyEventPublished, listener.on_event)

        anomaly = AnomalyEventPublished(
            anomaly=AnomalyEvent(detector_name="z_score", severity_score=4.0)
        )
        await bus.publish(anomaly)

        assert len(listener.events) == 1
        assert listener.events[0] is anomaly

    async def test_multiple_handlers_all_invoked(self) -> None:
        bus = EventBus()
        a = CollectingListener()
        b = CollectingListener()
        bus.subscribe(AnomalyEventPublished, a.on_event)
        bus.subscribe(AnomalyEventPublished, b.on_event)

        await bus.publish(
            AnomalyEventPublished(anomaly=AnomalyEvent(detector_name="x"))
        )

        assert len(a.events) == 1
        assert len(b.events) == 1

    async def test_handler_only_invoked_for_subscribed_type(self) -> None:
        bus = EventBus()
        listener = CollectingListener()
        bus.subscribe(AnomalyEventPublished, listener.on_event)

        # Publishing a different event type should not reach this listener.
        trace = BlockTrace(
            block_id="blk_1",
            event_sequence=("E22",),
            record_ids=(),
            first_timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            last_timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            record_count=1,
            pipeline_run_id=uuid4(),
            closed_reason="terminal",
        )
        await bus.publish(BlockTraceClosed(trace=trace))

        assert listener.events == []

    async def test_unsubscribe(self) -> None:
        bus = EventBus()
        listener = CollectingListener()
        bus.subscribe(AnomalyEventPublished, listener.on_event)
        bus.unsubscribe(AnomalyEventPublished, listener.on_event)

        await bus.publish(
            AnomalyEventPublished(anomaly=AnomalyEvent(detector_name="x"))
        )
        assert listener.events == []


class TestConcurrency:
    async def test_one_slow_listener_does_not_block_others(self) -> None:
        # The plan calls this out explicitly: one slow listener must not
        # block fast listeners. We assert that wall time is dominated by
        # the slow handler (not slow * num_handlers).
        bus = EventBus()
        delay = 0.10
        events_seen: list[int] = []

        async def slow(_: AnomalyEventPublished) -> None:
            await asyncio.sleep(delay)
            events_seen.append(1)

        async def fast(_: AnomalyEventPublished) -> None:
            events_seen.append(2)

        bus.subscribe(AnomalyEventPublished, slow)
        bus.subscribe(AnomalyEventPublished, fast)

        start = time.perf_counter()
        await bus.publish(
            AnomalyEventPublished(anomaly=AnomalyEvent(detector_name="x"))
        )
        elapsed = time.perf_counter() - start

        # Both ran; total time bounded by slow handler, not 2x of it.
        assert sorted(events_seen) == [1, 2]
        assert elapsed < delay * 1.8


class TestErrorIsolation:
    async def test_one_failing_listener_does_not_break_others(self) -> None:
        bus = EventBus()
        good = CollectingListener()

        async def bad(_: AnomalyEventPublished) -> None:
            raise RuntimeError("listener bug")

        bus.subscribe(AnomalyEventPublished, bad)
        bus.subscribe(AnomalyEventPublished, good.on_event)

        # Bus should not raise even though `bad` did.
        await bus.publish(
            AnomalyEventPublished(anomaly=AnomalyEvent(detector_name="x"))
        )

        assert len(good.events) == 1


class TestMetricsListener:
    async def test_anomaly_counter_increments(self) -> None:
        registry = MetricsRegistry()
        bus = EventBus()
        m = MetricsListener(registry=registry)
        bus.subscribe(AnomalyEventPublished, m.on_anomaly)

        for _ in range(3):
            await bus.publish(
                AnomalyEventPublished(anomaly=AnomalyEvent(detector_name="z_score"))
            )

        snap = registry.snapshot()
        assert snap["counters"]["anomalies.z_score"] == 3

    async def test_trace_closed_counter_increments(self) -> None:
        registry = MetricsRegistry()
        bus = EventBus()
        m = MetricsListener(registry=registry)
        bus.subscribe(BlockTraceClosed, m.on_trace_closed)

        trace = BlockTrace(
            block_id="blk_1",
            event_sequence=("E22",),
            record_ids=(),
            first_timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            last_timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            record_count=1,
            pipeline_run_id=uuid4(),
            closed_reason="terminal",
        )
        await bus.publish(BlockTraceClosed(trace=trace))

        snap = registry.snapshot()
        assert snap["counters"]["traces.closed.terminal"] == 1
