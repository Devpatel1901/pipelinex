"""TimingDecorator: records per-stage processing time into a MetricsRegistry."""

from __future__ import annotations

import time

from pipelinex.core.interfaces import IPipelineStage
from pipelinex.core.models import LogRecord
from pipelinex.observability.metrics import MetricsRegistry, default_registry
from pipelinex.stages.decorators.base import BaseStageDecorator


class TimingDecorator(BaseStageDecorator):
    """Wrap a stage to record its per-record processing time."""

    def __init__(
        self,
        inner: IPipelineStage,
        registry: MetricsRegistry | None = None,
    ) -> None:
        super().__init__(inner)
        self._registry = registry or default_registry

    async def process(self, record: LogRecord) -> LogRecord:
        start = time.perf_counter()
        try:
            return await self._inner.process(record)
        finally:
            elapsed = time.perf_counter() - start
            await self._registry.timing(f"stage.{self._inner.name}.duration_s", elapsed)
            await self._registry.inc(f"stage.{self._inner.name}.invocations")
