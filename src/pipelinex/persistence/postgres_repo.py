"""PostgreSQL implementation of ``ILogRepository``.

Uses SQLAlchemy 2.0's async API with the asyncpg driver. The schema lives in
``schema.sql`` and is loaded into the docker-compose container at startup.

Liskov substitution
~~~~~~~~~~~~~~~~~~~
This repository must pass the same test suite as ``InMemoryLogRepository``.
The shared suite lives in ``tests/integration/test_postgres_integration.py``
and only runs when ``POSTGRES_DSN`` is set in the environment, so unit-test
runs (and CI without docker) skip it cleanly.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import (
    ARRAY,
    JSON,
    Column,
    DateTime,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    and_,
    func,
    select,
)
from sqlalchemy import (
    UUID as SAUUID,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from pipelinex.core.exceptions import RepositoryError
from pipelinex.core.interfaces import (
    AggregateResult,
    Aggregation,
    ILogRepository,
    QueryFilters,
)
from pipelinex.core.models import AnomalyEvent, BlockTrace, LogRecord, Severity

_metadata = MetaData()

log_records_table = Table(
    "log_records",
    _metadata,
    Column("id", SAUUID(as_uuid=True), primary_key=True),
    Column("pipeline_run_id", SAUUID(as_uuid=True), nullable=True),
    Column("timestamp", DateTime(timezone=True), nullable=True),
    Column("source", String(255), nullable=False, default=""),
    Column("severity", String(20), nullable=False, default="INFO"),
    Column("message", Text, nullable=False, default=""),
    Column("raw_payload", JSONB, nullable=False, default=dict),
    Column("enrichment", JSONB, nullable=False, default=dict),
    Column("stage_history", ARRAY(Text), nullable=False, default=list),
)

block_traces_table = Table(
    "block_traces",
    _metadata,
    Column("block_id", String(64), primary_key=True),
    Column("pipeline_run_id", SAUUID(as_uuid=True), nullable=False),
    Column("first_timestamp", DateTime(timezone=True), nullable=True),
    Column("last_timestamp", DateTime(timezone=True), nullable=True),
    Column("record_count", Integer, nullable=False),
    Column("event_sequence", ARRAY(Text), nullable=False),
    Column("closed_reason", String(20), nullable=False),
)

anomaly_events_table = Table(
    "anomaly_events",
    _metadata,
    Column("id", SAUUID(as_uuid=True), primary_key=True),
    Column("log_record_id", SAUUID(as_uuid=True), nullable=False),
    Column("detector_name", String(100), nullable=False),
    Column("severity_score", Float, nullable=False),
    Column("detected_at", DateTime(timezone=True), nullable=False),
    Column("metadata", JSON, nullable=False, default=dict),
)


class PostgresLogRepository(ILogRepository):
    """Async PostgreSQL implementation of ``ILogRepository``.

    Construct via ``PostgresLogRepository.from_dsn(dsn)`` for the common case;
    pass an existing ``AsyncEngine`` for tests that share an engine.
    """

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine
        self._sessionmaker = async_sessionmaker(engine, expire_on_commit=False)

    @classmethod
    def from_dsn(cls, dsn: str) -> PostgresLogRepository:
        engine = create_async_engine(dsn, future=True)
        return cls(engine)

    async def close(self) -> None:
        await self._engine.dispose()

    # ---------- writes ----------

    async def save(self, record: LogRecord) -> None:
        await self.save_batch([record])

    async def save_batch(self, records: list[LogRecord]) -> None:
        if not records:
            return
        rows = [self._record_to_row(r) for r in records]
        try:
            async with self._engine.begin() as conn:
                await conn.execute(log_records_table.insert(), rows)
        except SQLAlchemyError as e:
            raise RepositoryError(f"save_batch failed: {e}") from e

    async def save_trace(self, trace: BlockTrace) -> None:
        await self.save_trace_batch([trace])

    async def save_trace_batch(self, traces: list[BlockTrace]) -> None:
        if not traces:
            return
        rows = [self._trace_to_row(t) for t in traces]
        try:
            async with self._engine.begin() as conn:
                await conn.execute(block_traces_table.insert(), rows)
        except SQLAlchemyError as e:
            raise RepositoryError(f"save_trace_batch failed: {e}") from e

    async def save_anomaly(self, anomaly: AnomalyEvent) -> None:
        try:
            async with self._engine.begin() as conn:
                await conn.execute(
                    anomaly_events_table.insert(),
                    [
                        {
                            "id": anomaly.id,
                            "log_record_id": anomaly.log_record_id,
                            "detector_name": anomaly.detector_name,
                            "severity_score": anomaly.severity_score,
                            "detected_at": anomaly.detected_at,
                            "metadata": dict(anomaly.metadata),
                        }
                    ],
                )
        except SQLAlchemyError as e:
            raise RepositoryError(f"save_anomaly failed: {e}") from e

    # ---------- reads ----------

    async def query(self, filters: QueryFilters) -> list[LogRecord]:
        stmt = select(log_records_table)
        clauses = []
        if filters.severity is not None:
            clauses.append(log_records_table.c.severity == filters.severity)
        if filters.source is not None:
            clauses.append(log_records_table.c.source == filters.source)
        if filters.pipeline_run_id is not None:
            clauses.append(
                log_records_table.c.pipeline_run_id == UUID(filters.pipeline_run_id)
            )
        if filters.after is not None:
            clauses.append(log_records_table.c.timestamp >= filters.after)
        if filters.before is not None:
            clauses.append(log_records_table.c.timestamp <= filters.before)
        if clauses:
            stmt = stmt.where(and_(*clauses))
        stmt = stmt.limit(filters.limit)

        try:
            async with self._engine.connect() as conn:
                rows = (await conn.execute(stmt)).mappings().all()
        except SQLAlchemyError as e:
            raise RepositoryError(f"query failed: {e}") from e
        return [self._row_to_record(r) for r in rows]

    async def aggregate(self, agg: Aggregation) -> AggregateResult:
        if agg.metric != "count":
            raise NotImplementedError(f"unsupported metric: {agg.metric}")
        if agg.group_by == "severity":
            col = log_records_table.c.severity
        elif agg.group_by == "source":
            col = log_records_table.c.source
        else:
            raise NotImplementedError(f"unsupported group_by: {agg.group_by}")

        stmt = (
            select(col.label(agg.group_by), func.count().label("count"))
            .group_by(col)
            .order_by(func.count().desc())
        )
        try:
            async with self._engine.connect() as conn:
                result = (await conn.execute(stmt)).mappings().all()
        except SQLAlchemyError as e:
            raise RepositoryError(f"aggregate failed: {e}") from e
        return AggregateResult(rows=[dict(r) for r in result])

    # ---------- helpers used by the integration test suite ----------

    async def truncate_all(self) -> None:
        """Test-only: clear every table managed by this repository."""
        async with self._engine.begin() as conn:
            for table in (log_records_table, block_traces_table, anomaly_events_table):
                await conn.execute(table.delete())

    async def all_records(self) -> list[LogRecord]:
        return await self.query(QueryFilters(limit=10**6))

    async def all_traces(self) -> list[BlockTrace]:
        try:
            async with self._engine.connect() as conn:
                rows = (await conn.execute(select(block_traces_table))).mappings().all()
        except SQLAlchemyError as e:
            raise RepositoryError(f"all_traces failed: {e}") from e
        return [self._row_to_trace(r) for r in rows]

    async def all_anomalies(self) -> list[AnomalyEvent]:
        try:
            async with self._engine.connect() as conn:
                rows = (
                    await conn.execute(select(anomaly_events_table))
                ).mappings().all()
        except SQLAlchemyError as e:
            raise RepositoryError(f"all_anomalies failed: {e}") from e
        return [self._row_to_anomaly(r) for r in rows]

    async def clear(self) -> None:
        await self.truncate_all()

    # ---------- mappers ----------

    @staticmethod
    def _record_to_row(r: LogRecord) -> dict[str, Any]:
        return {
            "id": r.id,
            "pipeline_run_id": r.pipeline_run_id,
            "timestamp": r.timestamp,
            "source": r.source,
            "severity": str(r.severity),
            "message": r.message,
            "raw_payload": dict(r.raw_payload),
            "enrichment": dict(r.enrichment),
            "stage_history": list(r.stage_history),
        }

    @staticmethod
    def _row_to_record(row: Any) -> LogRecord:
        return LogRecord(
            id=row["id"],
            pipeline_run_id=row["pipeline_run_id"],
            timestamp=_as_utc(row["timestamp"]),
            source=row["source"] or "",
            severity=Severity(row["severity"]) if row["severity"] else Severity.INFO,
            message=row["message"] or "",
            raw_payload=dict(row["raw_payload"] or {}),
            enrichment=dict(row["enrichment"] or {}),
            stage_history=list(row["stage_history"] or []),
        )

    @staticmethod
    def _trace_to_row(t: BlockTrace) -> dict[str, Any]:
        return {
            "block_id": t.block_id,
            "pipeline_run_id": t.pipeline_run_id,
            "first_timestamp": t.first_timestamp,
            "last_timestamp": t.last_timestamp,
            "record_count": t.record_count,
            "event_sequence": list(t.event_sequence),
            "closed_reason": t.closed_reason,
        }

    @staticmethod
    def _row_to_trace(row: Any) -> BlockTrace:
        return BlockTrace(
            block_id=row["block_id"],
            event_sequence=tuple(row["event_sequence"] or ()),
            record_ids=(),  # not persisted; regenerable from log_records if needed
            first_timestamp=_as_utc(row["first_timestamp"]),
            last_timestamp=_as_utc(row["last_timestamp"]),
            record_count=int(row["record_count"]),
            pipeline_run_id=row["pipeline_run_id"],
            closed_reason=row["closed_reason"],
        )

    @staticmethod
    def _row_to_anomaly(row: Any) -> AnomalyEvent:
        return AnomalyEvent(
            id=row["id"],
            log_record_id=row["log_record_id"],
            detector_name=row["detector_name"],
            severity_score=float(row["severity_score"]),
            detected_at=_as_utc(row["detected_at"]),
            metadata=dict(row["metadata"] or {}),
        )


def _as_utc(value: datetime | None) -> datetime:
    """Coerce a possibly-naive datetime returned by Postgres into UTC.

    The schema columns are TIMESTAMPTZ, so values are tz-aware on read; this
    is defensive against drivers that return naive datetimes in some paths.
    """
    if value is None:
        from datetime import UTC

        return datetime.now(UTC)
    if value.tzinfo is None:
        from datetime import UTC

        return value.replace(tzinfo=UTC)
    return value


def _ensure_sequence(value: Sequence[str] | None) -> list[str]:
    return list(value or [])
