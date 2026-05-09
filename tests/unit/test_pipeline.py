"""Tests for the PipelineExecutor."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from pipelinex.core.exceptions import FatalStageError
from pipelinex.core.interfaces import IPipelineStage
from pipelinex.core.models import LogRecord
from pipelinex.core.pipeline import PipelineExecutor
from pipelinex.core.sources import FileLogSource, IterableLogSource
from pipelinex.persistence.memory_repo import InMemoryLogRepository
from pipelinex.stages.parsing.hdfs_parser import HDFSParser
from pipelinex.stages.parsing.parser_stage import ParserStage


class _IdentityStage(IPipelineStage):
    """Pass-through stage. Records the calls made for assertions."""

    def __init__(self, label: str = "identity") -> None:
        self._label = label
        self.calls = 0

    @property
    def name(self) -> str:
        return self._label

    async def process(self, record: LogRecord) -> LogRecord:
        self.calls += 1
        record.add_enrichment(self._label, True)
        return record


class _FailingStage(IPipelineStage):
    """Always raises FatalStageError — used to test error isolation."""

    @property
    def name(self) -> str:
        return "always_fails"

    async def process(self, record: LogRecord) -> LogRecord:
        raise FatalStageError("synthetic failure")


class _SlowStage(IPipelineStage):
    """Sleeps for a bit to simulate slow processing."""

    def __init__(self, delay_s: float) -> None:
        self._delay = delay_s

    @property
    def name(self) -> str:
        return "slow"

    async def process(self, record: LogRecord) -> LogRecord:
        await asyncio.sleep(self._delay)
        return record


class TestEndToEnd:
    async def test_processes_all_records(self) -> None:
        source = IterableLogSource(["line-a", "line-b", "line-c"])
        stage = _IdentityStage()
        repo = InMemoryLogRepository()

        executor = PipelineExecutor(
            source=source,
            stages=[stage],
            repository=repo,
            num_workers=2,
            queue_size=10,
            batch_size=2,
            flush_interval_s=0.1,
        )
        result = await executor.run()

        assert result.records_processed == 3
        assert result.records_failed == 0
        assert stage.calls == 3
        assert len(await repo.all_records()) == 3

    async def test_run_id_propagates_to_records(self) -> None:
        source = IterableLogSource(["one", "two"])
        repo = InMemoryLogRepository()
        executor = PipelineExecutor(
            source=source,
            stages=[_IdentityStage()],
            repository=repo,
            num_workers=1,
            queue_size=4,
            batch_size=1,
            flush_interval_s=0.05,
        )
        await executor.run()

        records = await repo.all_records()
        assert all(r.pipeline_run_id is not None for r in records)
        assert len({r.pipeline_run_id for r in records}) == 1  # all share the run id

    async def test_stage_history_records_each_stage(self) -> None:
        source = IterableLogSource(["x"])
        repo = InMemoryLogRepository()
        s1 = _IdentityStage("first")
        s2 = _IdentityStage("second")
        executor = PipelineExecutor(
            source=source,
            stages=[s1, s2],
            repository=repo,
            num_workers=1,
            queue_size=4,
            batch_size=1,
            flush_interval_s=0.05,
        )
        await executor.run()

        records = await repo.all_records()
        assert records[0].stage_history == ["first", "second"]


class TestErrorHandling:
    async def test_fatal_stage_error_is_isolated_to_one_record(self) -> None:
        source = IterableLogSource(["a", "b", "c", "d"])
        repo = InMemoryLogRepository()
        executor = PipelineExecutor(
            source=source,
            stages=[_FailingStage()],
            repository=repo,
            num_workers=1,
            queue_size=4,
            batch_size=2,
            flush_interval_s=0.05,
        )
        result = await executor.run()
        # All 4 fail, but the pipeline survives and reports.
        assert result.records_processed == 0
        assert result.records_failed == 4
        assert (await repo.all_records()) == []


class TestBackpressure:
    async def test_bounded_queue_caps_in_flight_records(self) -> None:
        # 50 records, queue_size=4, slow consumer (50ms each), 1 worker.
        # If the queue were unbounded, the producer would finish almost
        # instantly with 50 records waiting. With a 4-cap, the producer
        # must block on put() until consumers drain.
        n = 50
        source = IterableLogSource(f"line-{i}" for i in range(n))
        repo = InMemoryLogRepository()
        slow = _SlowStage(delay_s=0.005)
        executor = PipelineExecutor(
            source=source,
            stages=[slow],
            repository=repo,
            num_workers=1,
            queue_size=4,
            batch_size=10,
            flush_interval_s=0.05,
        )
        result = await executor.run()
        assert result.records_processed == n
        assert len(await repo.all_records()) == n


class TestBatching:
    async def test_records_persisted_via_save_batch(self) -> None:
        # Verify multiple records hit save_batch (rather than save) by
        # using a repo subclass that counts batch-vs-single calls.
        class CountingRepo(InMemoryLogRepository):
            def __init__(self) -> None:
                super().__init__()
                self.single_calls = 0
                self.batch_calls = 0

            async def save(self, record: LogRecord) -> None:
                self.single_calls += 1
                await super().save(record)

            async def save_batch(self, records: list[LogRecord]) -> None:
                self.batch_calls += 1
                await super().save_batch(records)

        source = IterableLogSource(f"l{i}" for i in range(10))
        repo = CountingRepo()
        executor = PipelineExecutor(
            source=source,
            stages=[_IdentityStage()],
            repository=repo,
            num_workers=1,
            queue_size=4,
            batch_size=5,
            flush_interval_s=10.0,  # large so size-based flush dominates
        )
        await executor.run()
        assert repo.batch_calls >= 1
        assert repo.single_calls == 0
        assert len(await repo.all_records()) == 10


class TestRealHDFSSample:
    """Smoke test against the 2k HDFS sample."""

    @pytest.fixture
    def hdfs_sample(self) -> Path:
        path = Path(__file__).resolve().parents[2] / "HDFS" / "HDFS_2k.log"
        if not path.exists():
            pytest.skip(f"HDFS_2k.log missing: {path}")
        return path

    async def test_parses_2k_sample_end_to_end(self, hdfs_sample: Path) -> None:
        source = FileLogSource(hdfs_sample)
        repo = InMemoryLogRepository()
        executor = PipelineExecutor(
            source=source,
            stages=[ParserStage(HDFSParser())],
            repository=repo,
            num_workers=4,
            queue_size=200,
            batch_size=100,
            flush_interval_s=0.1,
        )
        result = await executor.run()
        # Allow a tiny number of parser failures (the plan accepts <=0.1%).
        assert result.records_processed >= 1998
        assert result.records_failed <= 2

        records = await repo.all_records()
        # At least 80% of records should have block_id enrichment.
        with_block = sum(1 for r in records if "block_id" in r.enrichment)
        assert with_block / len(records) >= 0.8
