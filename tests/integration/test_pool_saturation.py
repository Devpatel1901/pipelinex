"""Tests for Postgres pool sizing + circuit breaker.

The pool-sizing helper is pure: tested as a unit.

The circuit breaker logic is tested against a fake repository that lets us
inject ``PoolSaturatedError`` deterministically. This keeps the test suite
runnable without Docker; the real Postgres path is exercised by the
broader integration suite when ``POSTGRES_DSN`` is set.
"""

from __future__ import annotations

import asyncio

import pytest

from pipelinex.core.exceptions import LoadSheddingError, PoolSaturatedError
from pipelinex.core.interfaces import (
    AggregateResult,
    Aggregation,
    ILogRepository,
    QueryFilters,
)
from pipelinex.core.models import AnomalyEvent, BlockTrace, LogRecord
from pipelinex.persistence.circuit_breaker import (
    BreakerState,
    RepositoryCircuitBreaker,
)
from pipelinex.persistence.pool import PoolConfig, build_pool_kwargs


class _FakeRepo(ILogRepository):
    """Deterministic fake for breaker testing.

    Each call consumes one item from ``script``. ``"ok"`` succeeds;
    ``"saturate"`` raises ``PoolSaturatedError``.
    """

    def __init__(self, script: list[str]) -> None:
        self.script = list(script)
        self.calls = 0

    async def save(self, record: LogRecord) -> None:
        return await self.save_batch([record])

    async def save_batch(self, records: list[LogRecord]) -> None:
        self.calls += 1
        action = self.script.pop(0) if self.script else "ok"
        if action == "saturate":
            raise PoolSaturatedError("pool checkout timed out")

    async def save_trace(self, trace: BlockTrace) -> None:
        return await self.save_batch([])

    async def save_trace_batch(self, traces: list[BlockTrace]) -> None:
        return await self.save_batch([])

    async def save_anomaly(self, anomaly: AnomalyEvent) -> None:
        return await self.save_batch([])

    async def query(self, filters: QueryFilters) -> list[LogRecord]:
        return []

    async def aggregate(self, agg: Aggregation) -> AggregateResult:
        return AggregateResult(rows=[])


# ---------- Pool sizing ----------


class TestPoolSizing:
    def test_pool_size_derived_from_workers(self) -> None:
        kwargs = build_pool_kwargs(num_workers=4)
        assert kwargs["pool_size"] == 8
        assert kwargs["max_overflow"] == 4
        assert kwargs["pool_timeout"] == 5.0
        assert kwargs["pool_pre_ping"] is True

    def test_overrides_take_precedence(self) -> None:
        cfg = PoolConfig(pool_size=20, max_overflow=10, pool_timeout_s=2.5, pool_pre_ping=False)
        kwargs = build_pool_kwargs(num_workers=4, overrides=cfg)
        assert kwargs["pool_size"] == 20
        assert kwargs["max_overflow"] == 10
        assert kwargs["pool_timeout"] == 2.5
        assert kwargs["pool_pre_ping"] is False

    def test_partial_overrides(self) -> None:
        cfg = PoolConfig(pool_size=12)
        kwargs = build_pool_kwargs(num_workers=8, overrides=cfg)
        assert kwargs["pool_size"] == 12  # override
        assert kwargs["max_overflow"] == 8  # default
        assert kwargs["pool_timeout"] == 5.0
        assert kwargs["pool_pre_ping"] is True

    def test_rejects_zero_workers(self) -> None:
        with pytest.raises(ValueError, match="num_workers"):
            build_pool_kwargs(num_workers=0)


# ---------- Circuit breaker ----------


class TestCircuitBreaker:
    async def test_passthrough_when_closed(self) -> None:
        inner = _FakeRepo(["ok", "ok", "ok"])
        breaker = RepositoryCircuitBreaker(inner)
        for _ in range(3):
            await breaker.save_batch([])
        assert breaker.state is BreakerState.CLOSED
        assert inner.calls == 3

    async def test_opens_on_repeated_saturation(self) -> None:
        inner = _FakeRepo(["saturate"] * 5)
        breaker = RepositoryCircuitBreaker(
            inner, failure_threshold=5, window_seconds=30.0, cooldown_s=10.0
        )
        for _ in range(5):
            with pytest.raises(PoolSaturatedError):
                await breaker.save_batch([])
        assert breaker.state is BreakerState.OPEN

    async def test_open_sheds_load(self) -> None:
        inner = _FakeRepo(["saturate"] * 3)
        breaker = RepositoryCircuitBreaker(
            inner, failure_threshold=3, window_seconds=30.0, cooldown_s=10.0
        )
        # Trip the breaker.
        for _ in range(3):
            with pytest.raises(PoolSaturatedError):
                await breaker.save_batch([])
        assert breaker.state is BreakerState.OPEN

        # Next call should be shed without touching the inner repo.
        with pytest.raises(LoadSheddingError):
            await breaker.save_batch([])
        # inner.calls is the count *before* this shed call — unchanged.
        assert inner.calls == 3

    async def test_half_open_recovery_on_success(self) -> None:
        inner = _FakeRepo(["saturate"] * 3 + ["ok"])
        breaker = RepositoryCircuitBreaker(
            inner, failure_threshold=3, window_seconds=30.0, cooldown_s=0.05
        )
        for _ in range(3):
            with pytest.raises(PoolSaturatedError):
                await breaker.save_batch([])
        assert breaker.state is BreakerState.OPEN

        # Wait past the cooldown so the breaker transitions HALF_OPEN.
        await asyncio.sleep(0.1)

        # The probe call succeeds → breaker closes.
        await breaker.save_batch([])
        assert breaker.state is BreakerState.CLOSED

    async def test_half_open_failure_returns_to_open(self) -> None:
        inner = _FakeRepo(["saturate"] * 3 + ["saturate"])
        breaker = RepositoryCircuitBreaker(
            inner, failure_threshold=3, window_seconds=30.0, cooldown_s=0.05
        )
        for _ in range(3):
            with pytest.raises(PoolSaturatedError):
                await breaker.save_batch([])
        await asyncio.sleep(0.1)

        # Probe fails → breaker re-opens.
        with pytest.raises(PoolSaturatedError):
            await breaker.save_batch([])
        assert breaker.state is BreakerState.OPEN

    async def test_old_failures_age_out_of_window(self) -> None:
        inner = _FakeRepo(["saturate", "saturate", "ok", "saturate", "saturate"])
        breaker = RepositoryCircuitBreaker(
            inner, failure_threshold=3, window_seconds=0.05, cooldown_s=10.0
        )
        # Two failures inside the window.
        for _ in range(2):
            with pytest.raises(PoolSaturatedError):
                await breaker.save_batch([])
        # Let them age out.
        await asyncio.sleep(0.1)
        # A success keeps the breaker closed.
        await breaker.save_batch([])
        # Two new failures — old two are pruned, total inside-window = 2 < 3.
        for _ in range(2):
            with pytest.raises(PoolSaturatedError):
                await breaker.save_batch([])
        assert breaker.state is BreakerState.CLOSED

    async def test_reads_bypass_breaker(self) -> None:
        # Even when the breaker is OPEN, reads should pass through (queries
        # don't make the situation worse).
        inner = _FakeRepo(["saturate"] * 3)
        breaker = RepositoryCircuitBreaker(
            inner, failure_threshold=3, window_seconds=30.0, cooldown_s=10.0
        )
        for _ in range(3):
            with pytest.raises(PoolSaturatedError):
                await breaker.save_batch([])
        # Reads are not gated, no exception raised.
        result = await breaker.query(QueryFilters())
        assert result == []


class TestCircuitBreakerConstruction:
    def test_rejects_zero_threshold(self) -> None:
        with pytest.raises(ValueError, match="failure_threshold"):
            RepositoryCircuitBreaker(_FakeRepo([]), failure_threshold=0)

    def test_rejects_nonpositive_window(self) -> None:
        with pytest.raises(ValueError, match="window_seconds"):
            RepositoryCircuitBreaker(_FakeRepo([]), window_seconds=0.0)

    def test_rejects_negative_cooldown(self) -> None:
        with pytest.raises(ValueError, match="cooldown_s"):
            RepositoryCircuitBreaker(_FakeRepo([]), cooldown_s=-1.0)
