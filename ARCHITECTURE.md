# PipelineX Architecture

This document captures the load-bearing architectural decisions and the
reasoning behind them. The companion `docs/design-patterns.md` covers the
six patterns in detail; this file focuses on the macro-level shape.

## 1. The mental model

PipelineX is a **single-process async pipeline** that ingests log lines,
processes them through a configurable list of stages, and persists the
results. The unit of work flowing through the system is a `LogRecord`.

```
Source ── raw line ──▶ Pipeline ──▶ Repository
                          │
                  bounded asyncio.Queue
                          │
                  N consumer workers
                  (parse → validate → enrich → sessionize → detect)
                          │
                       EventBus ──▶ Listeners (alerts, sequence detector, …)
```

The **pipeline** is the heart of the system; everything else (parsers,
validators, detectors, repositories, listeners) is a plug-in connected
through small interfaces.

## 2. Why async, not threads or processes

- Workload is **I/O-bound** — file reads, repository writes, future
  network sources. Async gives us thousands of concurrent in-flight
  operations at the cost of a few worker tasks. The GIL never bites
  because we don't hold it for compute-heavy work.
- The plan's headline target (≥20K records/sec) is sustained on **8
  asyncio workers** in `benchmark_throughput.py`. Threads would have
  added contention without speedup; processes would have added IPC
  serialization overhead.
- CPU-bound work (e.g. heavy regex template matching on multi-MB lines)
  could be offloaded to a thread pool via `asyncio.run_in_executor`.
  Documented as future work; not needed at current target rates.

## 3. Concurrency engine: bounded queue + N workers + batch persistence

The `PipelineExecutor` runs three coroutine families:

1. **Producer** (1 task): pulls raw lines from `ILogSource`, wraps each
   in a `LogRecord`, puts it on the queue. Blocks on `put` when the
   queue is full — this is the backpressure mechanism.
2. **Consumers** (N tasks): pull records, run them through the stage
   list in order, buffer for batch persistence.
3. **Flusher** (1 task): wakes every `flush_interval_s` to drain
   partial batches so they don't sit in memory indefinitely.

### Why a bounded queue (default 1000)
Unbounded queues are a classic memory leak. When ingestion outruns
processing, an unbounded queue grows until OOM. A bounded queue
backpressures the producer, which is the correct, observable behaviour
under load. Verified by `tests/property/test_pipeline_properties.py`
(records-conservation under random configurations) and the
`benchmark_throughput.py --backpressure` sub-experiment (queue
saturates at capacity, producer dilates its wall time, no records lost).

### Why batch persistence (default 100 records / batch)
Per-record inserts cost a network round-trip each. Batching of 100
amortizes the round-trip across many records — verified by the test
that asserts `save_batch` calls dominate over `save` calls.

### Why a shared worker pool, not per-stage workers
Each record traverses every stage on the same worker (no inter-stage
queues). This is **simpler** but trades stage-level parallelism: a
slow stage blocks the worker until that record is done. The plan
explicitly accepts this trade-off and documents per-stage queues as a
v2 evolution.

### Error classification
- `FatalStageError` → drop the record, increment `records_failed`.
- `TransientStageError` → caller (e.g. `RetryDecorator`) is expected to
  retry; if it bubbles to the executor we still drop and continue.
- Anything else → log unexpected, drop the record.

## 4. The four detectors and the two anomaly models

PipelineX ships **four detectors with deliberately different
mathematical foundations**:

| Detector | Operates on | Detects | Dataset |
|---|---|---|---|
| Z-Score | numeric stream | point outliers (Gaussian-ish) | BGL |
| IQR | numeric stream | point outliers (heavy-tailed/skewed) | BGL |
| CUSUM | numeric stream | gradual drift | BGL |
| Sequence (n-gram) | closed `BlockTrace` | sequence-shape anomalies | HDFS_v1 |

The **two anomaly models**:

- **Per-line / point**: BGL labels each line directly. Detectors run
  inline on a binned rate stream (`event_rate_per_minute`).
- **Per-trace / sequence**: HDFS labels each *block id*, where a block's
  full lifecycle is a sequence of events across many lines. The
  pipeline must group lines by `block_id` before scoring.

The **`BlockSessionizerStage` + `SequenceAnomalyDetector` + `EventBus`**
trio is the architectural answer to that second model. The sessionizer
tees records (per-line records still flow downstream) while emitting
closed `BlockTrace` events to the bus; the sequence detector subscribes
as a listener. New detectors that operate on traces require zero
changes to the sessionizer or the executor.

## 5. The sessionizer: timeout-dominant emission

A naive sessionizer would close a trace only when a "terminal" event
arrives. That breaks on real HDFS data because anomalous traces
*by definition* often miss the terminal event — that's the anomaly. So
emission is driven by **multiple complementary triggers**, in priority
order:

1. **Hard cap** (`record_count > 1000`): force-emit, `closed_reason = "evicted"`.
2. **Terminal-event hint**: when an `event_id ∈ {E9, E21}` is observed,
   the trace is *marked* as likely complete. The next janitor pass
   closes it with `closed_reason = "terminal"`. The delay lets
   slightly-out-of-order arrivals attach.
3. **Idle timeout** (`last_timestamp` older than `idle_window_s` of
   *log time*): close as `"timeout"`.
4. **Shutdown**: on pipeline stop, close all open traces as
   `"shutdown"`.
5. **LRU eviction**: when open-trace count exceeds `max_open_traces`
   (default 50,000), force-emit the oldest as `"evicted"`. This caps
   memory on the ~575K-block HDFS_v1 corpus.

### Why `record_ids` not `raw_records`
A `BlockTrace` holds **UUIDs**, not full `LogRecord`s. With ~575K
blocks × ~19 lines/block average, holding full records would pin 11M
records in RAM. Detectors that need raw records query the repository
by id. This is the single highest-leverage memory decision in the
project, and it's pinned by `tests/unit/test_models.py::test_holds_record_ids_not_records`.

## 6. The repository pattern and the Liskov guarantee

`ILogRepository` defines the persistence contract. Two implementations:

- `InMemoryLogRepository` — used everywhere by default; gives unit
  tests the data-store they need without docker.
- `PostgresLogRepository` — SQLAlchemy 2.0 async + asyncpg; mounted in
  docker-compose with `schema.sql` auto-loaded.

The integration test suite at
`tests/integration/test_postgres_integration.py` is **shared** with
the in-memory tests in spirit: any scenario that passes against
`InMemoryLogRepository` must pass against `PostgresLogRepository`.
That's the Liskov guarantee made operational.

## 7. Observability

- `MetricsRegistry` (counters / gauges / timers with p50/p95/p99) used
  by `TimingDecorator` and `MetricsListener`.
- `/metrics` HTTP endpoint exposes a JSON snapshot.
- `PipelineRunResult` carries record_id-level stats per run.
- `stage_history` field on every record traces which stages it visited
  — invaluable for debugging.

## 8. What's explicitly out of scope (for v1)

- **Distributed processing.** Single-node only. Per-stage queues +
  Kafka source documented as v2.
- **Auth on the API.** This is a portfolio project; auth would distract
  from the engineering points being made.
- **Sub-millisecond latency.** Designed for throughput. Latency
  measurements are reported (p50/p95/p99) but not optimised.
- **PCA / DeepLog sequence detectors.** N-gram chosen for v1 — published
  HDFS results show comparable F1 at a fraction of the complexity.

## 9. Where to read next

- `docs/design-patterns.md` — six patterns with rationale.
- `docs/concurrency-model.md` — diagrams of the executor's task layout.
- `BENCHMARKS.md` — measured throughput, latency, F1 numbers.
- `README.md` — top-level summary + quickstart.
