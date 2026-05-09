"""LoggingDecorator: structured per-record logs around a stage.

Logs at DEBUG so production runs aren't drowned in volume. Tests can capture
the log output to assert behaviour.
"""

from __future__ import annotations

import logging

from pipelinex.core.interfaces import IPipelineStage
from pipelinex.core.models import LogRecord
from pipelinex.stages.decorators.base import BaseStageDecorator

logger = logging.getLogger("pipelinex.stages")


class LoggingDecorator(BaseStageDecorator):
    """Emit DEBUG logs before and after the inner stage."""

    def __init__(
        self,
        inner: IPipelineStage,
        log_level: int = logging.DEBUG,
    ) -> None:
        super().__init__(inner)
        self._level = log_level

    async def process(self, record: LogRecord) -> LogRecord:
        logger.log(
            self._level,
            "enter stage=%s record_id=%s severity=%s source=%s",
            self._inner.name,
            record.id,
            record.severity,
            record.source,
        )
        try:
            result = await self._inner.process(record)
        except Exception as e:
            logger.log(
                self._level,
                "stage=%s record_id=%s raised %s: %s",
                self._inner.name,
                record.id,
                type(e).__name__,
                e,
            )
            raise
        logger.log(
            self._level,
            "exit stage=%s record_id=%s",
            self._inner.name,
            result.id,
        )
        return result
