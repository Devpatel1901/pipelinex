"""Concurrent pipeline executor — the heart of the system.

Architecture
------------
- One **producer** task pulls raw lines from an ``ILogSource`` and pushes
  ``LogRecord`` shells onto a bounded ``asyncio.Queue``.
- N **consumer** workers pull records off the queue and run them through the
  full stage list sequentially.
- Records that survive the stages are buffered and flushed in batches to the
  repository — this amortizes per-record persistence cost roughly 100x.
- A **flusher** task periodically drains partial batches so they don't sit
  in memory indefinitely.
- A **shutdown event** lets the run wind down cleanly (no record loss).

Architectural decisions
~~~~~~~~~~~~~~~~~~~~~~~
- Bounded queue (default ``maxsize=1000``) → backpressure: the producer
  blocks when consumers fall behind, instead of leaking memory.
- Shared worker pool (each record traverses all stages on one worker) →
  simpler than per-stage queues; trade-off documented in ARCHITECTURE.md.
- Errors classified by exception type:
  - ``FatalStageError``: drop the record, log, continue.
  - ``TransientStageError``: caller (e.g. RetryDecorator) is expected to
    have already retried; if it bubbles here we still drop and continue.
  - Anything else: log unexpected, drop the record.
- ``queue.task_done()`` in ``finally`` so ``queue.join()`` is correct even
  when a stage crashes.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from uuid import uuid4

from pipelinex.core.exceptions import (
    FatalStageError,
    LoadSheddingError,
    RepositoryError,
    TransientStageError,
)
from pipelinex.core.interfaces import ILogRepository, ILogSource, IPipelineStage
from pipelinex.core.models import LogRecord, PipelineRunResult

logger = logging.getLogger(__name__)


class PipelineExecutor:
    """Run a pipeline end-to-end with bounded queues and graceful shutdown."""

    def __init__(
        self,
        source: ILogSource,
        stages: list[IPipelineStage],
        repository: ILogRepository,
        num_workers: int = 4,
        queue_size: int = 1000,
        batch_size: int = 100,
        flush_interval_s: float = 1.0,
    ) -> None:
        self._source = source
        self._stages = stages
        self._repository = repository
        self._num_workers = num_workers
        self._queue: asyncio.Queue[LogRecord] = asyncio.Queue(maxsize=queue_size)
        self._batch_buffer: list[LogRecord] = []
        self._batch_size = batch_size
        self._flush_interval_s = flush_interval_s
        self._batch_lock = asyncio.Lock()
        self._run_id = uuid4()
        self._shutdown_event = asyncio.Event()

        self._records_processed = 0
        self._records_failed = 0
        self._records_shed = 0
        self._anomalies_detected = 0

    @property
    def run_id(self) -> str:
        return str(self._run_id)

    async def run(self) -> PipelineRunResult:
        started = datetime.now(UTC)
        logger.info(
            "pipeline run %s starting (workers=%s, queue_size=%s, batch_size=%s)",
            self._run_id,
            self._num_workers,
            self._queue.maxsize,
            self._batch_size,
        )

        producer = asyncio.create_task(self._produce(), name="producer")
        consumers = [
            asyncio.create_task(self._consume(worker_id=i), name=f"consumer-{i}")
            for i in range(self._num_workers)
        ]
        flusher = asyncio.create_task(self._batch_flusher(), name="flusher")

        try:
            await producer  # source exhausted (or raised)
            await self._queue.join()  # wait for queued records to be consumed
        finally:
            self._shutdown_event.set()
            for c in consumers:
                c.cancel()
            await asyncio.gather(*consumers, flusher, return_exceptions=True)
            await self._flush_batch()  # final drain

        completed = datetime.now(UTC)
        result = PipelineRunResult(
            run_id=self._run_id,
            records_processed=self._records_processed,
            records_failed=self._records_failed,
            records_shed=self._records_shed,
            started_at=started,
            completed_at=completed,
            anomalies_detected=self._anomalies_detected,
        )
        logger.info(
            "pipeline run %s done: processed=%s failed=%s shed=%s elapsed=%.2fs",
            self._run_id,
            result.records_processed,
            result.records_failed,
            result.records_shed,
            (completed - started).total_seconds(),
        )
        return result

    # ---------- private ----------

    async def _produce(self) -> None:
        """Pull from source, wrap each line as a LogRecord, push to queue."""
        async for raw_line in self._source.stream():
            record = LogRecord(
                pipeline_run_id=self._run_id,
                raw_payload={"raw": raw_line},
            )
            await self._queue.put(record)  # blocks if queue full → backpressure

    async def _consume(self, worker_id: int) -> None:
        """Worker loop: pull a record, run all stages, buffer for batch save."""
        while not self._shutdown_event.is_set():
            try:
                record = await asyncio.wait_for(self._queue.get(), timeout=0.5)
            except TimeoutError:
                continue

            try:
                for stage in self._stages:
                    record = await stage.process(record)
                    record.mark_stage(stage.name)
                await self._buffer_for_batch(record)
                self._records_processed += 1
            except LoadSheddingError as e:
                # Circuit breaker is OPEN — count separately so the
                # conservation invariant remains processed + failed + shed.
                logger.warning(
                    "worker %s: load-shed on %s: %s", worker_id, record.id, e
                )
                self._records_shed += 1
            except FatalStageError as e:
                logger.warning("worker %s: fatal error on %s: %s", worker_id, record.id, e)
                self._records_failed += 1
            except TransientStageError as e:
                # If we got here a Retry decorator already gave up.
                logger.warning("worker %s: transient error on %s: %s", worker_id, record.id, e)
                self._records_failed += 1
            except Exception as e:
                # Keep the worker loop alive even on unexpected exceptions.
                logger.exception("worker %s: unexpected error: %s", worker_id, e)
                self._records_failed += 1
            finally:
                self._queue.task_done()

    async def _buffer_for_batch(self, record: LogRecord) -> None:
        async with self._batch_lock:
            self._batch_buffer.append(record)
            should_flush = len(self._batch_buffer) >= self._batch_size
        if should_flush:
            await self._flush_batch()

    async def _flush_batch(self) -> None:
        async with self._batch_lock:
            if not self._batch_buffer:
                return
            to_save = self._batch_buffer
            self._batch_buffer = []

        try:
            await self._repository.save_batch(to_save)
        except LoadSheddingError as e:
            # Circuit breaker open during batch flush — these records were
            # successfully processed but cannot be persisted right now.
            logger.warning("batch shed (n=%s): %s", len(to_save), e)
            self._records_shed += len(to_save)
            self._records_processed -= len(to_save)
        except RepositoryError as e:
            logger.error("batch save failed (n=%s): %s", len(to_save), e)
            self._records_failed += len(to_save)
            self._records_processed -= len(to_save)

    async def _batch_flusher(self) -> None:
        """Periodic flush so partial batches don't sit too long."""
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(self._flush_interval_s)
            except asyncio.CancelledError:
                return
            await self._flush_batch()
