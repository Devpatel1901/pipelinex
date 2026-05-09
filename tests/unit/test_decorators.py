"""Unit tests for the stage decorators (Decorator pattern)."""

from __future__ import annotations

import logging
from typing import cast

import pytest

from pipelinex.core.exceptions import FatalStageError, TransientStageError
from pipelinex.core.interfaces import IPipelineStage
from pipelinex.core.models import LogRecord
from pipelinex.observability.metrics import MetricsRegistry
from pipelinex.stages.decorators.logging import LoggingDecorator
from pipelinex.stages.decorators.retry import RetryDecorator
from pipelinex.stages.decorators.timing import TimingDecorator


class _SuccessStage(IPipelineStage):
    def __init__(self, label: str = "success") -> None:
        self._label = label
        self.calls = 0

    @property
    def name(self) -> str:
        return self._label

    async def process(self, record: LogRecord) -> LogRecord:
        self.calls += 1
        return record


class _FlakyStage(IPipelineStage):
    """Fails N times with TransientStageError, then succeeds."""

    def __init__(self, fail_n: int) -> None:
        self._remaining = fail_n
        self.attempts = 0

    @property
    def name(self) -> str:
        return "flaky"

    async def process(self, record: LogRecord) -> LogRecord:
        self.attempts += 1
        if self._remaining > 0:
            self._remaining -= 1
            raise TransientStageError(f"will retry, remaining={self._remaining}")
        return record


class _AlwaysFatalStage(IPipelineStage):
    @property
    def name(self) -> str:
        return "fatal"

    async def process(self, record: LogRecord) -> LogRecord:
        raise FatalStageError("hard failure")


class TestTimingDecorator:
    async def test_records_timing_per_call(self) -> None:
        registry = MetricsRegistry()
        inner = _SuccessStage("parser")
        decorated = TimingDecorator(inner, registry=registry)

        for _ in range(5):
            await decorated.process(LogRecord())

        snap = registry.snapshot()
        assert snap["counters"]["stage.parser.invocations"] == 5
        assert snap["timers"]["stage.parser.duration_s"]["count"] == 5

    async def test_records_timing_even_on_failure(self) -> None:
        registry = MetricsRegistry()
        inner = _AlwaysFatalStage()
        decorated = TimingDecorator(inner, registry=registry)

        with pytest.raises(FatalStageError):
            await decorated.process(LogRecord())

        snap = registry.snapshot()
        # Both invocation count and timing should be recorded.
        assert snap["counters"]["stage.fatal.invocations"] == 1
        assert snap["timers"]["stage.fatal.duration_s"]["count"] == 1

    async def test_inner_name_preserved(self) -> None:
        decorated = TimingDecorator(_SuccessStage("parser"))
        assert decorated.name == "parser"


class TestRetryDecorator:
    async def test_retries_transient_until_success(self) -> None:
        flaky = _FlakyStage(fail_n=2)
        decorated = RetryDecorator(flaky, max_retries=3, base_delay_s=0.001)

        await decorated.process(LogRecord())
        # 2 failures + 1 success = 3 attempts total.
        assert flaky.attempts == 3

    async def test_gives_up_after_max_retries(self) -> None:
        flaky = _FlakyStage(fail_n=10)
        decorated = RetryDecorator(flaky, max_retries=2, base_delay_s=0.001)

        with pytest.raises(TransientStageError):
            await decorated.process(LogRecord())
        # 1 initial + 2 retries = 3 attempts.
        assert flaky.attempts == 3

    async def test_does_not_retry_fatal_errors(self) -> None:
        # Fatal errors bypass retry — they're non-recoverable by design.
        fatal = _AlwaysFatalStage()
        decorated = RetryDecorator(fatal, max_retries=5, base_delay_s=0.001)

        with pytest.raises(FatalStageError):
            await decorated.process(LogRecord())

    def test_rejects_negative_max_retries(self) -> None:
        with pytest.raises(ValueError, match="max_retries"):
            RetryDecorator(_SuccessStage(), max_retries=-1)


class TestLoggingDecorator:
    async def test_logs_enter_and_exit(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        decorated = LoggingDecorator(_SuccessStage("parser"), log_level=logging.INFO)

        with caplog.at_level(logging.INFO, logger="pipelinex.stages"):
            await decorated.process(LogRecord())

        messages = [r.getMessage() for r in caplog.records]
        assert any("enter stage=parser" in m for m in messages)
        assert any("exit stage=parser" in m for m in messages)

    async def test_logs_exceptions(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        decorated = LoggingDecorator(_AlwaysFatalStage(), log_level=logging.INFO)

        with (
            caplog.at_level(logging.INFO, logger="pipelinex.stages"),
            pytest.raises(FatalStageError),
        ):
            await decorated.process(LogRecord())

        messages = [r.getMessage() for r in caplog.records]
        assert any("FatalStageError" in m for m in messages)


class TestStacking:
    async def test_decorators_stack_arbitrarily(self) -> None:
        # The whole point of the Decorator pattern: stacking.
        registry = MetricsRegistry()
        flaky = _FlakyStage(fail_n=1)
        stacked = TimingDecorator(
            RetryDecorator(
                LoggingDecorator(flaky, log_level=logging.DEBUG),
                max_retries=2,
                base_delay_s=0.001,
            ),
            registry=registry,
        )

        await stacked.process(LogRecord())
        # Inner stage was retried once → 2 total attempts.
        assert flaky.attempts == 2
        # Outer timing decorator saw exactly 1 invocation (it sees the whole
        # retry loop as a single call).
        snap = registry.snapshot()
        assert snap["counters"]["stage.flaky.invocations"] == 1

    async def test_inner_property_traverses_stack(self) -> None:
        inner = _SuccessStage("base")
        stacked = TimingDecorator(RetryDecorator(LoggingDecorator(inner)))
        assert stacked.name == "base"
        # We can drill in via .inner to reach the underlying stage if needed.
        layer1 = cast(TimingDecorator, stacked).inner
        layer2 = cast(RetryDecorator, layer1).inner
        layer3 = cast(LoggingDecorator, layer2).inner
        assert layer3 is inner
