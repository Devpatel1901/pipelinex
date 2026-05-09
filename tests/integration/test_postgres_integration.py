"""Integration tests against a real PostgreSQL.

This is the **Liskov substitution test** for the repository abstraction:
the same scenarios that pass for ``InMemoryLogRepository`` must pass against
``PostgresLogRepository``. If they don't, the abstraction is leaky.

The tests are skipped unless ``POSTGRES_DSN`` is set in the environment, so
local unit-test runs and CI without docker stay fast.

To run::

    docker compose up -d
    POSTGRES_DSN=postgresql+asyncpg://pipelinex:pipelinex@localhost:5432/pipelinex \\
        pytest tests/integration/test_postgres_integration.py -v
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from pipelinex.core.interfaces import Aggregation, QueryFilters
from pipelinex.core.models import AnomalyEvent, BlockTrace, LogRecord, Severity
from pipelinex.persistence.postgres_repo import PostgresLogRepository

DSN = os.environ.get("POSTGRES_DSN")
pytestmark = pytest.mark.skipif(
    not DSN,
    reason="POSTGRES_DSN not set; skipping PostgreSQL integration tests",
)


@pytest.fixture
async def repo() -> AsyncIterator[PostgresLogRepository]:
    assert DSN is not None
    repo = PostgresLogRepository.from_dsn(DSN)
    await repo.truncate_all()
    try:
        yield repo
    finally:
        await repo.close()


def _record(**kw: object) -> LogRecord:
    rec = LogRecord(timestamp=datetime.now(UTC))
    for k, v in kw.items():
        setattr(rec, k, v)
    return rec


class TestSaveAndQuery:
    async def test_save_then_query_returns_it(
        self, repo: PostgresLogRepository
    ) -> None:
        rec = _record(message="hello")
        await repo.save(rec)
        results = await repo.query(QueryFilters(limit=10))
        assert len(results) == 1
        assert results[0].message == "hello"

    async def test_save_batch_with_payload(
        self, repo: PostgresLogRepository
    ) -> None:
        recs = [_record(severity=Severity.ERROR) for _ in range(20)]
        for r in recs:
            r.raw_payload = {"k": "v"}
            r.enrichment = {"block_id": "blk_1"}
        await repo.save_batch(recs)

        results = await repo.query(QueryFilters(severity="ERROR", limit=100))
        assert len(results) == 20
        assert all(r.raw_payload.get("k") == "v" for r in results)
        assert all(r.enrichment.get("block_id") == "blk_1" for r in results)

    async def test_query_filters_by_time(
        self, repo: PostgresLogRepository
    ) -> None:
        base = datetime(2024, 1, 1, tzinfo=UTC)
        await repo.save(_record(timestamp=base))
        await repo.save(_record(timestamp=base + timedelta(hours=1)))
        await repo.save(_record(timestamp=base + timedelta(hours=2)))

        results = await repo.query(
            QueryFilters(
                after=base + timedelta(minutes=30),
                before=base + timedelta(hours=1, minutes=30),
                limit=100,
            )
        )
        assert len(results) == 1


class TestAggregate:
    async def test_count_by_severity(self, repo: PostgresLogRepository) -> None:
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


class TestTraces:
    async def test_save_and_retrieve_trace(
        self, repo: PostgresLogRepository
    ) -> None:
        trace = BlockTrace(
            block_id="blk_1",
            event_sequence=("E22", "E5", "E11", "E9"),
            record_ids=(uuid4(), uuid4()),
            first_timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            last_timestamp=datetime(2024, 1, 1, 0, 5, tzinfo=UTC),
            record_count=4,
            pipeline_run_id=uuid4(),
            closed_reason="terminal",
        )
        await repo.save_trace(trace)
        traces = await repo.all_traces()
        assert len(traces) == 1
        assert traces[0].block_id == "blk_1"
        assert traces[0].event_sequence == ("E22", "E5", "E11", "E9")


class TestAnomalies:
    async def test_save_and_retrieve_anomaly(
        self, repo: PostgresLogRepository
    ) -> None:
        anomaly = AnomalyEvent(
            detector_name="z_score",
            severity_score=4.2,
            metadata={"value": 1000.0},
        )
        await repo.save_anomaly(anomaly)
        anomalies = await repo.all_anomalies()
        assert len(anomalies) == 1
        assert anomalies[0].detector_name == "z_score"
        assert anomalies[0].metadata.get("value") == 1000.0
