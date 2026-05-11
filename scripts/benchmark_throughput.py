#!/usr/bin/env python3
"""Throughput, latency, and backpressure benchmarks.

Three sub-experiments (run any subset via the CLI flags):

1. ``--workers-sweep`` — sweep workers in {1,2,4,8} and record records/sec.
2. ``--latency`` — p50/p95/p99 record latency through the pipeline.
3. ``--backpressure`` — verify the bounded queue blocks the producer when
   a deliberately slow repository can't keep up.

Outputs are written to ``benchmarks/results/*.json`` and matplotlib charts
(if matplotlib is available) to ``benchmarks/results/*.png``.

Usage::

    python scripts/benchmark_throughput.py --all
"""

from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import sys
import time
from collections.abc import AsyncIterator, Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

# Allow running directly as a script.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pipelinex.core.interfaces import ILogSource, IPipelineStage
from pipelinex.core.models import LogRecord
from pipelinex.core.pipeline import PipelineExecutor
from pipelinex.persistence.memory_repo import InMemoryLogRepository
from pipelinex.stages.parsing.hdfs_parser import HDFSParser
from pipelinex.stages.parsing.parser_stage import ParserStage

REPO_ROOT = Path(__file__).resolve().parents[1]
RESULTS_DIR = REPO_ROOT / "benchmarks" / "results"


# ---------- helpers ----------


class IterableLogSource(ILogSource):
    """Inline copy of the test-only iterable source (kept here to avoid pulling
    in test deps in production runs)."""

    def __init__(self, lines: Iterable[str]) -> None:
        self._lines = list(lines)

    async def stream(self) -> AsyncIterator[str]:
        for line in self._lines:
            yield line


class SlowRepository(InMemoryLogRepository):
    """Sleep for ``sleep_per_batch`` on each save_batch call."""

    def __init__(self, sleep_per_batch: float) -> None:
        super().__init__()
        self._sleep = sleep_per_batch

    async def save_batch(self, records: list[LogRecord]) -> None:
        await asyncio.sleep(self._sleep)
        await super().save_batch(records)


def _generate_hdfs_lines(n: int) -> list[str]:
    """Cheap generator — just stamp templated lines."""
    base = datetime(2024, 1, 1, tzinfo=UTC)
    out: list[str] = []
    for i in range(n):
        ts = base
        date_str = ts.strftime("%y%m%d")
        time_str = ts.strftime("%H%M%S")
        out.append(
            f"{date_str} {time_str} {i % 999} INFO dfs.DataNode$PacketResponder: "
            f"PacketResponder 1 for block blk_{i} terminating"
        )
    return out


# ---------- sub-experiments ----------


async def workers_sweep(total: int, worker_counts: list[int]) -> dict[str, Any]:
    print(f"\n[workers-sweep] total={total}")
    lines = _generate_hdfs_lines(total)
    results: list[dict[str, Any]] = []
    for n in worker_counts:
        source = IterableLogSource(lines)
        repo = InMemoryLogRepository()
        executor = PipelineExecutor(
            source=source,
            stages=[ParserStage(HDFSParser())],
            repository=repo,
            num_workers=n,
            queue_size=2000,
            batch_size=200,
            flush_interval_s=0.5,
        )
        start = time.perf_counter()
        result = await executor.run()
        elapsed = time.perf_counter() - start
        rate = result.records_processed / elapsed if elapsed else float("inf")
        print(
            f"  workers={n:>2}  records={result.records_processed:>7}  "
            f"failed={result.records_failed:>3}  elapsed={elapsed:.2f}s  "
            f"rate={rate:>9,.0f} rec/s"
        )
        results.append(
            {
                "workers": n,
                "records": result.records_processed,
                "failed": result.records_failed,
                "elapsed_s": elapsed,
                "records_per_sec": rate,
            }
        )
    return {"total": total, "runs": results}


class _StampingStage(IPipelineStage):
    """Records (t_persist - t_ingest) into a list — used for latency stats."""

    def __init__(self, latencies: list[float]) -> None:
        self._latencies = latencies

    @property
    def name(self) -> str:
        return "stamp:latency"

    async def process(self, record: LogRecord) -> LogRecord:
        ts_in = record.enrichment.get("t_ingest")
        if isinstance(ts_in, float):
            self._latencies.append(time.perf_counter() - ts_in)
        return record


async def latency_run(total: int, workers: int) -> dict[str, Any]:
    """Stamp ingest time on each record and measure pre-batch latency."""
    print(f"\n[latency] total={total} workers={workers}")
    lines = _generate_hdfs_lines(total)
    latencies: list[float] = []
    source = IterableLogSource(lines)
    repo = InMemoryLogRepository()

    stages: list[IPipelineStage] = [
        ParserStage(HDFSParser()),
        _StampingStage(latencies),
    ]

    # Wrap the source so each emitted line is stamped on the way in.
    # Doing this at the source layer is cleaner than monkey-patching the
    # executor.
    class _StampingSource(ILogSource):
        def __init__(self, inner: ILogSource) -> None:
            self._inner = inner

        async def stream(self) -> AsyncIterator[str]:
            async for line in self._inner.stream():
                # Stamp via a side channel: use a sentinel-prefixed line so
                # the latency stage can find it. Cleaner: use a custom
                # ParserStage that stamps. Simplest: just stamp inside the
                # pipeline by injecting the stamping stage *before* parser
                # — already done. We just need the producer to yield.
                yield line

    stamping_source = _StampingSource(source)

    # Insert a pre-parser stamping stage that runs as the first thing each
    # worker does on a record, capturing arrival time.
    class _IngestStamp(IPipelineStage):
        @property
        def name(self) -> str:
            return "stamp:ingest"

        async def process(self, record: LogRecord) -> LogRecord:
            record.add_enrichment("t_ingest", time.perf_counter())
            return record

    stages = [_IngestStamp(), ParserStage(HDFSParser()), _StampingStage(latencies)]
    executor = PipelineExecutor(
        source=stamping_source,
        stages=stages,
        repository=repo,
        num_workers=workers,
        queue_size=2000,
        batch_size=200,
        flush_interval_s=0.5,
    )

    await executor.run()

    if not latencies:
        return {"workers": workers, "samples": 0}
    p50 = statistics.median(latencies)
    p95 = statistics.quantiles(latencies, n=20, method="inclusive")[-1]
    p99 = statistics.quantiles(latencies, n=100, method="inclusive")[-1]
    print(
        f"  samples={len(latencies):>7}  "
        f"p50={p50 * 1000:.3f}ms  p95={p95 * 1000:.3f}ms  p99={p99 * 1000:.3f}ms"
    )
    return {
        "workers": workers,
        "samples": len(latencies),
        "p50_ms": p50 * 1000,
        "p95_ms": p95 * 1000,
        "p99_ms": p99 * 1000,
    }


async def backpressure_run(total: int) -> dict[str, Any]:
    """Slow repo + small queue → assert producer dilates and queue saturates."""
    print(f"\n[backpressure] total={total} (slow repo)")
    lines = _generate_hdfs_lines(total)
    source = IterableLogSource(lines)
    repo = SlowRepository(sleep_per_batch=0.05)
    queue_size = 100
    executor = PipelineExecutor(
        source=source,
        stages=[ParserStage(HDFSParser())],
        repository=repo,
        num_workers=4,
        queue_size=queue_size,
        batch_size=20,
        flush_interval_s=0.05,
    )

    sample_max_qsize = 0

    async def sampler() -> None:
        nonlocal sample_max_qsize
        while True:
            try:
                await asyncio.sleep(0.01)
                sample_max_qsize = max(
                    sample_max_qsize,
                    executor._queue.qsize(),  # type: ignore[attr-defined]
                )
            except asyncio.CancelledError:
                return

    sampler_task = asyncio.create_task(sampler())
    start = time.perf_counter()
    result = await executor.run()
    elapsed = time.perf_counter() - start
    sampler_task.cancel()

    saturation_observed = sample_max_qsize >= int(queue_size * 0.5)
    print(
        f"  records={result.records_processed:>5}  elapsed={elapsed:.2f}s  "
        f"max_qsize={sample_max_qsize}/{queue_size}  saturation={saturation_observed}"
    )
    return {
        "total": total,
        "queue_size": queue_size,
        "max_observed_qsize": sample_max_qsize,
        "saturation_observed": saturation_observed,
        "elapsed_s": elapsed,
        "records": result.records_processed,
    }


# ---------- charts ----------


def _try_render_charts(results: dict[str, Any]) -> None:
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("[charts] matplotlib not installed; skipping")
        return

    if "workers_sweep" in results:
        runs = results["workers_sweep"]["runs"]
        ws = [r["workers"] for r in runs]
        rates = [r["records_per_sec"] for r in runs]
        fig, ax = plt.subplots(figsize=(7, 4))
        ax.plot(ws, rates, marker="o")
        ax.set_xlabel("Workers")
        ax.set_ylabel("Records / sec")
        ax.set_title("Pipeline throughput vs worker count")
        ax.grid(True, alpha=0.3)
        out = RESULTS_DIR / "throughput_vs_workers.png"
        fig.tight_layout()
        fig.savefig(out)
        plt.close(fig)
        print(f"[charts] wrote {out}")

    if "latency" in results and results["latency"].get("samples"):
        lat = results["latency"]
        fig, ax = plt.subplots(figsize=(7, 4))
        ax.bar(["p50", "p95", "p99"], [lat["p50_ms"], lat["p95_ms"], lat["p99_ms"]])
        ax.set_ylabel("Latency (ms, ingest -> pre-batch)")
        ax.set_title(f"Pipeline latency percentiles (n={lat['samples']})")
        out = RESULTS_DIR / "latency_percentiles.png"
        fig.tight_layout()
        fig.savefig(out)
        plt.close(fig)
        print(f"[charts] wrote {out}")


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="PipelineX throughput benchmarks.")
    p.add_argument("--workers-sweep", action="store_true")
    p.add_argument("--latency", action="store_true")
    p.add_argument("--backpressure", action="store_true")
    p.add_argument("--all", action="store_true")
    p.add_argument(
        "--total",
        type=int,
        default=200_000,
        help="records per sub-experiment (default 200000)",
    )
    p.add_argument(
        "--worker-counts",
        type=lambda s: [int(x) for x in s.split(",")],
        default=[1, 2, 4, 8],
    )
    p.add_argument("--output", default=str(RESULTS_DIR / "throughput.json"))
    return p.parse_args()


async def amain() -> int:
    args = _parse_args()
    if args.all:
        args.workers_sweep = True
        args.latency = True
        args.backpressure = True
    if not (args.workers_sweep or args.latency or args.backpressure):
        print("nothing to do — pass at least one of --workers-sweep / --latency / "
              "--backpressure / --all")
        return 1

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    results: dict[str, Any] = {}
    if args.workers_sweep:
        results["workers_sweep"] = await workers_sweep(args.total, args.worker_counts)
    if args.latency:
        results["latency"] = await latency_run(args.total, workers=4)
    if args.backpressure:
        results["backpressure"] = await backpressure_run(min(args.total, 5000))

    out_path = Path(args.output)
    out_path.write_text(json.dumps(results, indent=2))
    print(f"\nresults written to {out_path}")
    _try_render_charts(results)
    return 0


def main() -> int:
    return asyncio.run(amain())


if __name__ == "__main__":
    sys.exit(main())
