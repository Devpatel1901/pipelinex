# Concurrency Model

PipelineX is a single-process async system. This page documents the
runtime task layout, the queues that connect them, and the failure modes
each piece is designed to survive.

## Task layout

A running pipeline has these coroutines alive at once:

| Coroutine | Count | Job |
|---|---|---|
| `_produce` | 1 | Pull lines from `ILogSource`, wrap as `LogRecord`, push to queue |
| `_consume` | N | Pull a record, run all stages, buffer for batch save |
| `_batch_flusher` | 1 | Sleep `flush_interval_s`, drain partial batches |
| `BlockSessionizer.janitor` | 0 or 1 | Timeout-based emission (only if sessionizer enabled) |
| EventBus dispatch | spawned per `publish` | One short-lived task per listener |

```
                          ┌──────────────────────┐
                          │   ILogSource.stream() │
                          └──────────┬───────────┘
                                     │ async-iterates raw lines
                                     ▼
                          ┌──────────────────────┐
                          │     _produce  (1)    │  ← blocks on queue.put when full
                          └──────────┬───────────┘
                                     │ LogRecord
                                     ▼
                ┌──────────────────────────────────────┐
                │ asyncio.Queue(maxsize=queue_size)    │  ← bounded → backpressure
                └──┬─────────────┬──────────────┬──────┘
                   │             │              │
                   ▼             ▼              ▼
              ┌─────────┐   ┌─────────┐    ┌─────────┐
              │_consume │   │_consume │ …  │_consume │  N workers
              └────┬────┘   └────┬────┘    └────┬────┘
                   └───────┬─────┴──────────────┘
                           ▼  through stages 1..K, then ↓
                  ┌────────────────────┐
                  │   batch buffer     │  protected by asyncio.Lock
                  └─────────┬──────────┘
                            ▼ size cap OR flusher tick
                  ┌────────────────────┐
                  │  Repository.save_  │
                  │       batch        │
                  └────────────────────┘
```

## Queue sizing rule of thumb

- `queue_size = N * 4` is a reasonable starting point — enough to
  absorb short producer/consumer rate mismatches, small enough to OOM-
  protect.
- `batch_size = 100` amortizes round-trips well for typical Postgres
  workloads. Going higher trades memory for ~2-5% additional throughput.

## Failure modes and isolation

| Where | What we do |
|---|---|
| Stage raises `FatalStageError` | drop the record, log warning, continue |
| Stage raises `TransientStageError` (already through `RetryDecorator`) | drop, log, continue |
| Stage raises any other Exception | drop, log exception (not warning), continue |
| `Repository.save_batch` raises `RepositoryError` | the **whole batch** counts as failed; flusher loop continues |
| Producer raises a `SourceError` | producer dies; consumers drain the queue then exit |
| Consumer crashes hard (e.g. `MemoryError`) | other consumers keep going; the failed task's `task_done` is in `finally` so `queue.join()` is correct |
| `SIGTERM` | `_shutdown_event` is set; consumers and flusher unwind cleanly; final `_flush_batch` ensures buffered records are persisted |

The `task_done()` call lives in a `finally` block specifically so that
`queue.join()` is correct *even when a stage crashes*. Without that, the
join would hang and the run would never report.

## Backpressure semantics

The bounded `asyncio.Queue` is the only place producer/consumer are
coupled. `_produce` calls `await self._queue.put(record)`. When the
queue is full, the put suspends the producer until a consumer's
`task_done()` triggers the queue's internal semaphore.

The plan's "zero data loss under backpressure" promise is tested two
ways:

1. **Property test** at `tests/property/test_pipeline_properties.py`:
   for arbitrary input lists × arbitrary worker counts × arbitrary
   queue sizes × arbitrary batch sizes, `processed + failed == lines_in`.
2. **Backpressure benchmark** at `scripts/benchmark_throughput.py
   --backpressure`: a `SlowRepository` is injected; the queue
   saturates at capacity (verified by a sidecar sampler), the
   producer's wall time dilates, and the final record count equals the
   input count.

## Why no per-stage queues (yet)

A per-stage-queue architecture would give stage-level parallelism: a
fast parser can pile up records while a slow detector chews through
them. We skipped that in v1 because:

- It doubles the number of bounded queues to size and tune.
- The ~40K rec/s observed with one queue + 8 workers exceeds the
  charter goal by 2x — the bottleneck is currently the asyncio loop
  itself, not stage serialization.
- It's complexity that has to be justified by a profile, not a hunch.

When the profile says otherwise, the migration is a one-day rewrite of
`PipelineExecutor` and is documented in `ARCHITECTURE.md` as v2 work.

## Sessionizer + EventBus interplay

When the sessionizer is enabled, every record passing through it is
mutated *in place* with no transformation, then the record continues
downstream. Asynchronously, the sessionizer's janitor coroutine fires
`BlockTraceClosed` events on the `EventBus`. The `SequenceAnomalyDetector`
is subscribed; it scores each closed trace.

This means the sequence detector runs **outside the consumer loop**:

```
   consumer worker:  ParserStage → ValidationStage → ... → SessionizerStage(tee) → ...
                                                                  │
                                                                  ▼
                                                  bus.publish(BlockTraceClosed)
                                                                  │
                                              spawned task: SequenceDetector.detect_trace
```

A slow sequence detector cannot stall the consumer — the bus dispatch
is `gather(...)` of independent listener tasks, and slow listeners are
isolated by the bus's per-handler exception handling.
