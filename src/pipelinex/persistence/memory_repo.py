"""In-memory implementation of ``ILogRepository``.

This is the test-friendly implementation. The PostgreSQL implementation
(arriving Day 8) must pass the same test suite.
"""

from __future__ import annotations

import asyncio
from collections import Counter

from pipelinex.core.interfaces import (
    AggregateResult,
    Aggregation,
    ILogRepository,
    QueryFilters,
)
from pipelinex.core.models import AnomalyEvent, BlockTrace, LogRecord


class InMemoryLogRepository(ILogRepository):
    """Stores records, traces, and anomalies in process memory."""

    def __init__(self) -> None:
        self._records: list[LogRecord] = []
        self._traces: list[BlockTrace] = []
        self._anomalies: list[AnomalyEvent] = []
        self._lock = asyncio.Lock()

    async def save(self, record: LogRecord) -> None:
        async with self._lock:
            self._records.append(record)

    async def save_batch(self, records: list[LogRecord]) -> None:
        async with self._lock:
            self._records.extend(records)

    async def save_trace(self, trace: BlockTrace) -> None:
        async with self._lock:
            self._traces.append(trace)

    async def save_trace_batch(self, traces: list[BlockTrace]) -> None:
        async with self._lock:
            self._traces.extend(traces)

    async def save_anomaly(self, anomaly: AnomalyEvent) -> None:
        async with self._lock:
            self._anomalies.append(anomaly)

    async def query(self, filters: QueryFilters) -> list[LogRecord]:
        async with self._lock:
            results: list[LogRecord] = []
            for r in self._records:
                if filters.severity is not None and r.severity != filters.severity:
                    continue
                if filters.source is not None and r.source != filters.source:
                    continue
                if (
                    filters.pipeline_run_id is not None
                    and str(r.pipeline_run_id) != filters.pipeline_run_id
                ):
                    continue
                if filters.after is not None and (
                    r.timestamp is None or r.timestamp < filters.after
                ):
                    continue
                if filters.before is not None and (
                    r.timestamp is None or r.timestamp > filters.before
                ):
                    continue
                results.append(r)
                if len(results) >= filters.limit:
                    break
            return results

    async def aggregate(self, agg: Aggregation) -> AggregateResult:
        if agg.metric != "count":
            raise NotImplementedError(f"unsupported metric: {agg.metric}")
        if agg.group_by not in {"severity", "source"}:
            raise NotImplementedError(f"unsupported group_by: {agg.group_by}")

        async with self._lock:
            buckets: Counter[str] = Counter()
            for r in self._records:
                key = str(r.severity) if agg.group_by == "severity" else r.source
                buckets[key] += 1

        rows = [{agg.group_by: key, "count": count} for key, count in buckets.most_common()]
        return AggregateResult(rows=rows)

    # Test/debug helpers ---------------------------------------------------

    async def all_records(self) -> list[LogRecord]:
        async with self._lock:
            return list(self._records)

    async def all_traces(self) -> list[BlockTrace]:
        async with self._lock:
            return list(self._traces)

    async def all_anomalies(self) -> list[AnomalyEvent]:
        async with self._lock:
            return list(self._anomalies)

    async def clear(self) -> None:
        async with self._lock:
            self._records.clear()
            self._traces.clear()
            self._anomalies.clear()
