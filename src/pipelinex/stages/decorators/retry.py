"""RetryDecorator: retries TransientStageError with exponential backoff.

Only ``TransientStageError`` is retried — ``FatalStageError`` and any other
exception propagates immediately. This is why the exception hierarchy in
``core/exceptions.py`` matters: it's what lets the retry policy be precise
without coupling to the inner stage.
"""

from __future__ import annotations

import asyncio
import logging

from pipelinex.core.exceptions import TransientStageError
from pipelinex.core.interfaces import IPipelineStage
from pipelinex.core.models import LogRecord
from pipelinex.stages.decorators.base import BaseStageDecorator

logger = logging.getLogger(__name__)


class RetryDecorator(BaseStageDecorator):
    """Wrap a stage to retry on TransientStageError."""

    def __init__(
        self,
        inner: IPipelineStage,
        max_retries: int = 3,
        base_delay_s: float = 0.05,
        max_delay_s: float = 1.0,
        backoff_factor: float = 2.0,
    ) -> None:
        if max_retries < 0:
            raise ValueError("max_retries must be >= 0")
        if base_delay_s < 0:
            raise ValueError("base_delay_s must be >= 0")
        super().__init__(inner)
        self._max_retries = max_retries
        self._base = base_delay_s
        self._cap = max_delay_s
        self._factor = backoff_factor

    async def process(self, record: LogRecord) -> LogRecord:
        attempt = 0
        while True:
            try:
                return await self._inner.process(record)
            except TransientStageError as e:
                attempt += 1
                if attempt > self._max_retries:
                    logger.warning(
                        "stage %s gave up after %s attempts: %s",
                        self._inner.name,
                        attempt,
                        e,
                    )
                    raise
                delay = min(self._cap, self._base * (self._factor ** (attempt - 1)))
                logger.debug(
                    "stage %s transient failure (attempt %s/%s), retrying in %.2fs",
                    self._inner.name,
                    attempt,
                    self._max_retries,
                    delay,
                )
                await asyncio.sleep(delay)
