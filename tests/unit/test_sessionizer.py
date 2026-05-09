"""Tests for BlockSessionizerStage."""

from __future__ import annotations

import asyncio
import contextlib
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from pipelinex.core.models import LogRecord
from pipelinex.events.bus import EventBus
from pipelinex.events.events import BlockTraceClosed
from pipelinex.events.listeners import CollectingListener
from pipelinex.stages.sessionizing.block_sessionizer import BlockSessionizerStage


def _record(block_id: str, event_id: str, ts: datetime) -> LogRecord:
    rec = LogRecord(timestamp=ts)
    rec.add_enrichment("block_id", block_id)
    rec.add_enrichment("event_id", event_id)
    return rec


def _setup_stage(
    bus: EventBus,
    *,
    idle: float = 30.0,
    max_per_trace: int = 1000,
    max_open: int = 50_000,
    flush_interval: float = 5.0,
) -> tuple[BlockSessionizerStage, CollectingListener]:
    listener = CollectingListener()
    bus.subscribe(BlockTraceClosed, listener.on_event)
    stage = BlockSessionizerStage(
        bus=bus,
        pipeline_run_id=uuid4(),
        idle_window_s=idle,
        max_records_per_trace=max_per_trace,
        max_open_traces=max_open,
        flush_interval_s=flush_interval,
    )
    return stage, listener


class TestRecordPassThrough:
    async def test_returns_record_unchanged(self) -> None:
        bus = EventBus()
        stage, _ = _setup_stage(bus)
        rec = _record("blk_1", "E22", datetime(2024, 1, 1, tzinfo=UTC))
        out = await stage.process(rec)
        assert out is rec

    async def test_records_without_block_id_are_ignored(self) -> None:
        bus = EventBus()
        stage, _ = _setup_stage(bus)
        rec = LogRecord()
        await stage.process(rec)
        assert stage.open_count == 0


class TestAccumulation:
    async def test_groups_records_by_block_id(self) -> None:
        bus = EventBus()
        stage, _ = _setup_stage(bus)
        ts = datetime(2024, 1, 1, tzinfo=UTC)
        await stage.process(_record("blk_1", "E22", ts))
        await stage.process(_record("blk_1", "E5", ts))
        await stage.process(_record("blk_2", "E22", ts))
        assert stage.open_count == 2

    async def test_hard_cap_force_emits(self) -> None:
        bus = EventBus()
        stage, listener = _setup_stage(bus, max_per_trace=3)
        ts = datetime(2024, 1, 1, tzinfo=UTC)
        for _ in range(3):
            await stage.process(_record("blk_1", "Ex", ts))
        # On the 3rd record we hit the cap and emit.
        assert len(listener.events) == 1
        assert isinstance(listener.events[0], BlockTraceClosed)
        assert listener.events[0].trace.closed_reason == "evicted"


class TestEmissionOnTimeout:
    async def test_idle_block_closed_as_timeout(self) -> None:
        bus = EventBus()
        stage, listener = _setup_stage(bus, idle=1.0, flush_interval=0.05)
        await stage.start()
        try:
            t0 = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
            await stage.process(_record("blk_1", "E22", t0))
            # Advance log time by 10s by ingesting another block's record;
            # this updates the sessionizer's "latest log timestamp".
            t1 = t0 + timedelta(seconds=10)
            await stage.process(_record("blk_2", "E22", t1))
            # Janitor will close blk_1 (last_ts = t0, cutoff = t1 - 1s).
            for _ in range(20):
                await asyncio.sleep(0.05)
                if listener.events:
                    break
            reasons = {e.trace.closed_reason for e in listener.events}
            block_ids = {e.trace.block_id for e in listener.events}
            assert "timeout" in reasons
            assert "blk_1" in block_ids
        finally:
            await stage.stop()


class TestEmissionOnTerminal:
    async def test_terminal_event_closes_on_next_pass(self) -> None:
        bus = EventBus()
        stage, listener = _setup_stage(bus, idle=999.0, flush_interval=0.05)
        await stage.start()
        try:
            ts = datetime(2024, 1, 1, tzinfo=UTC)
            await stage.process(_record("blk_1", "E22", ts))
            await stage.process(_record("blk_1", "E5", ts))
            await stage.process(_record("blk_1", "E9", ts))  # terminal hint
            # Wait for the janitor to close it.
            for _ in range(20):
                await asyncio.sleep(0.05)
                if listener.events:
                    break
            assert listener.events
            assert listener.events[0].trace.closed_reason == "terminal"
        finally:
            await stage.stop()


class TestEmissionOnShutdown:
    async def test_open_traces_flushed_on_stop(self) -> None:
        bus = EventBus()
        stage, listener = _setup_stage(bus, idle=999.0, flush_interval=999.0)
        await stage.start()
        ts = datetime(2024, 1, 1, tzinfo=UTC)
        await stage.process(_record("blk_a", "E22", ts))
        await stage.process(_record("blk_b", "E22", ts))
        await stage.stop()
        assert len(listener.events) == 2
        for e in listener.events:
            assert e.trace.closed_reason == "shutdown"


class TestLRUEviction:
    async def test_oldest_block_evicted_when_over_capacity(self) -> None:
        bus = EventBus()
        stage, listener = _setup_stage(bus, max_open=2, flush_interval=999.0)
        ts = datetime(2024, 1, 1, tzinfo=UTC)
        await stage.process(_record("blk_a", "E22", ts))
        await stage.process(_record("blk_b", "E22", ts))
        # This third unique block forces eviction of blk_a (oldest).
        await stage.process(_record("blk_c", "E22", ts))

        evicted_ids = [
            e.trace.block_id
            for e in listener.events
            if e.trace.closed_reason == "evicted"
        ]
        assert "blk_a" in evicted_ids


class TestTraceContents:
    async def test_event_sequence_matches_input(self) -> None:
        bus = EventBus()
        stage, listener = _setup_stage(bus, idle=999.0, flush_interval=999.0)
        ts = datetime(2024, 1, 1, tzinfo=UTC)
        await stage.process(_record("blk_1", "E22", ts))
        await stage.process(_record("blk_1", "E5", ts))
        await stage.process(_record("blk_1", "E11", ts))
        await stage.start()
        with contextlib.suppress(Exception):
            await stage.stop()

        assert listener.events
        trace = listener.events[0].trace
        assert trace.event_sequence == ("E22", "E5", "E11")
        assert trace.record_count == 3
        # Plan-mandated decision: record_ids only, no full LogRecords.
        for rid in trace.record_ids:
            assert hasattr(rid, "hex")  # UUID-shaped
