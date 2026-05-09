"""Base decorator for pipeline stages.

The Decorator pattern lets us compose orthogonal cross-cutting concerns
(timing, retry, logging) around any stage without modifying the stage. Each
decorator wraps an inner ``IPipelineStage`` and itself implements
``IPipelineStage`` — so decorators stack.

Example::

    stage = TimingDecorator(
        RetryDecorator(
            LoggingDecorator(ParserStage(HDFSParser())),
            max_retries=3,
        )
    )
"""

from __future__ import annotations

from pipelinex.core.interfaces import IPipelineStage
from pipelinex.core.models import LogRecord


class BaseStageDecorator(IPipelineStage):
    """Common scaffolding for pipeline stage decorators."""

    def __init__(self, inner: IPipelineStage) -> None:
        self._inner = inner

    @property
    def name(self) -> str:
        return self._inner.name

    @property
    def inner(self) -> IPipelineStage:
        return self._inner

    async def process(self, record: LogRecord) -> LogRecord:
        return await self._inner.process(record)
