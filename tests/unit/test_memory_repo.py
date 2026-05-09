"""Unit tests for InMemoryLogRepository."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from pipelinex.core.interfaces import Aggregation, QueryFilters
from pipelinex.core.models import AnomalyEvent, BlockTrace, LogRecord, Severity
from pipelinex.persistence.memory_repo import InMemoryLogRepository


@pytest.fixture
def repo() -> InMemoryLogRepository:
    return InMemoryLogRepository()


def _record(severity: Severity = Severity.INFO, source: str = "app", **kw: object) -> LogRecord:
    return LogRecord(severity=severity, source=source, **kw)  # type: ignore[arg-type]


class TestSaveAndQuery:
    async def test_save_then_all_records_returns_it(self, repo: InMemoryLogRepository) -> None:
        rec = _record(message="hello")
        await repo.save(rec)
        all_recs = await repo.all_records()
        assert len(all_recs) == 1
        assert all_recs[0].message == "hello"

    async def test_save_batch(self, repo: InMemoryLogRepository) -> None:
        recs = [_record(message=f"msg-{i}") for i in range(5)]
        await repo.save_batch(recs)
        all_recs = await repo.all_records()
        assert len(all_recs) == 5

    async def test_query_filters_by_severity(self, repo: InMemoryLogRepository) -> None:
        await repo.save(_record(severity=Severity.INFO))
        await repo.save(_record(severity=Severity.ERROR))
        await repo.save(_record(severity=Severity.ERROR))

        results = await repo.query(QueryFilters(severity="ERROR"))
        assert len(results) == 2
        for r in results:
            assert r.severity is Severity.ERROR

    async def test_query_filters_by_source(self, repo: InMemoryLogRepository) -> None:
        await repo.save(_record(source="api"))
        await repo.save(_record(source="db"))
        results = await repo.query(QueryFilters(source="api"))
        assert len(results) == 1
        assert results[0].source == "api"

    async def test_query_filters_by_time_range(self, repo: InMemoryLogRepository) -> None:
        base = datetime(2024, 1, 1, tzinfo=UTC)
        await repo.save(_record(timestamp=base))
        await repo.save(_record(timestamp=base + timedelta(hours=1)))
        await repo.save(_record(timestamp=base + timedelta(hours=2)))

        results = await repo.query(
            QueryFilters(
                after=base + timedelta(minutes=30),
                before=base + timedelta(hours=1, minutes=30),
            )
        )
        assert len(results) == 1

    async def test_query_respects_limit(self, repo: InMemoryLogRepository) -> None:
        await repo.save_batch([_record() for _ in range(50)])
        results = await repo.query(QueryFilters(limit=10))
        assert len(results) == 10


class TestAggregate:
    async def test_count_by_severity(self, repo: InMemoryLogRepository) -> None:
        await repo.save_batch(
            [
                _record(severity=Severity.INFO),
                _record(severity=Severity.INFO),
                _record(severity=Severity.ERROR),
            ]
        )
        result = await repo.aggregate(Aggregation(group_by="severity"))
        rows_by_sev = {row["severity"]: row["count"] for row in result.rows}
        assert rows_by_sev == {"INFO": 2, "ERROR": 1}

    async def test_count_by_source(self, repo: InMemoryLogRepository) -> None:
        await repo.save_batch(
            [_record(source="api"), _record(source="api"), _record(source="db")]
        )
        result = await repo.aggregate(Aggregation(group_by="source"))
        rows_by_src = {row["source"]: row["count"] for row in result.rows}
        assert rows_by_src == {"api": 2, "db": 1}

    async def test_unknown_group_by_raises(self, repo: InMemoryLogRepository) -> None:
        with pytest.raises(NotImplementedError):
            await repo.aggregate(Aggregation(group_by="unknown"))


class TestTraces:
    def _trace(self, **overrides: object) -> BlockTrace:
        defaults = {
            "block_id": "blk_1",
            "event_sequence": ("E22", "E5", "E11", "E9"),
            "record_ids": (uuid4(),),
            "first_timestamp": datetime(2024, 1, 1, tzinfo=UTC),
            "last_timestamp": datetime(2024, 1, 1, 0, 5, tzinfo=UTC),
            "record_count": 4,
            "pipeline_run_id": uuid4(),
            "closed_reason": "terminal",
        }
        defaults.update(overrides)
        return BlockTrace(**defaults)  # type: ignore[arg-type]

    async def test_save_trace(self, repo: InMemoryLogRepository) -> None:
        trace = self._trace()
        await repo.save_trace(trace)
        traces = await repo.all_traces()
        assert len(traces) == 1
        assert traces[0].block_id == "blk_1"

    async def test_save_trace_batch(self, repo: InMemoryLogRepository) -> None:
        traces = [self._trace(block_id=f"blk_{i}") for i in range(3)]
        await repo.save_trace_batch(traces)
        all_traces = await repo.all_traces()
        assert len(all_traces) == 3


class TestAnomalies:
    async def test_save_anomaly(self, repo: InMemoryLogRepository) -> None:
        anomaly = AnomalyEvent(detector_name="z_score", severity_score=4.2)
        await repo.save_anomaly(anomaly)
        anomalies = await repo.all_anomalies()
        assert len(anomalies) == 1
        assert anomalies[0].detector_name == "z_score"


class TestClear:
    async def test_clear_resets_everything(self, repo: InMemoryLogRepository) -> None:
        await repo.save(_record())
        await repo.save_anomaly(AnomalyEvent(detector_name="x"))
        await repo.clear()
        assert (await repo.all_records()) == []
        assert (await repo.all_anomalies()) == []
