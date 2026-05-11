#!/usr/bin/env python3
"""Synthetic log generator — emits HDFS- or BGL-format lines at controlled rate.

Used by ``benchmark_throughput.py`` to drive the pipeline at a known rate
with a known anomaly density. Decouples performance benchmarks from the
real-data files.

Examples
--------
::

    # Generate 1M HDFS lines, no rate cap, write to stdout.
    python scripts/generate_synthetic_logs.py --total 1000000 --format hdfs

    # 50K BGL lines at 5K/s with 10% anomaly rate.
    python scripts/generate_synthetic_logs.py --format bgl --total 50000 \\
        --rate 5000 --anomaly-rate 0.1 --output bgl_synth.log
"""

from __future__ import annotations

import argparse
import random
import sys
import time
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TextIO

# ---------- HDFS templates (subset of HDFS_v1) ----------
# Normal block lifecycle: allocate -> replicate -> received-block ... -> deleted.
_HDFS_NORMAL_LIFECYCLE = ("E22", "E5", "E26", "E11", "E9", "E21")
_HDFS_EXCEPTION_EVENTS = ("E4", "E10", "E14")  # exception-flavoured templates

_HDFS_TEMPLATES = {
    "E5": "BLOCK* ask 10.250.{node}.{port}:50010 to replicate blk_{blk} to datanode(s) 10.250.{node2}.{port2}:50010",
    "E11": "Received block blk_{blk} of size 67108864 from /10.251.{node}.{port}",
    "E22": "BLOCK* NameSystem.allocateBlock: /user/root/data/part-{seq}. blk_{blk}",
    "E26": "BLOCK* NameSystem.addStoredBlock: blockMap updated: 10.251.{node}.{port}:50010 is added to blk_{blk} size 67108864",
    "E9": "Deleting block blk_{blk} file /tmp/blk_{blk}",
    "E21": "Deleting block blk_{blk} file /var/cache/hadoop/blk_{blk}",
    "E4": "BLOCK* ask 10.250.{node}.{port}:50010 to delete blk_{blk}",
    "E10": "PacketResponder {idx} for block blk_{blk} terminating",
    "E14": "Exception while reading from block blk_{blk}: connection reset",
}

# ---------- BGL canonical alert codes ----------
_BGL_ALERT_CODES = ("KERNDTLB", "KERNSTOR", "KERNRTSP", "APPSEV", "APPCHILD")


def main() -> int:
    args = _parse_args()
    rng = random.Random(args.seed)
    delay = 1.0 / args.rate if args.rate else 0.0

    if args.format == "hdfs":
        stream: Iterator[str] = _hdfs_stream(
            rng=rng,
            num_blocks=args.blocks,
            anomaly_rate=args.anomaly_rate,
        )
    else:
        stream = _bgl_stream(rng=rng, anomaly_rate=args.anomaly_rate)

    if args.output == "-":
        _write_stream(sys.stdout, stream, args, delay)
    else:
        with Path(args.output).open("w", encoding="utf-8") as out_handle:
            _write_stream(out_handle, stream, args, delay)
    return 0


def _write_stream(
    out_handle: TextIO,
    stream: Iterator[str],
    args: argparse.Namespace,
    delay: float,
) -> None:
    deadline = time.perf_counter() + args.duration if args.duration else None
    for i, line in enumerate(stream):
        if args.total is not None and i >= args.total:
            break
        if deadline is not None and time.perf_counter() >= deadline:
            break
        out_handle.write(line)
        out_handle.write("\n")
        if delay:
            time.sleep(delay)


# ---------- HDFS generation ----------


def _hdfs_stream(
    rng: random.Random, num_blocks: int, anomaly_rate: float
) -> Iterator[str]:
    """Yield interleaved HDFS-format lines for ``num_blocks`` distinct blocks.

    Lifecycle: build a sequence per block (with anomaly injection by
    probability) then round-robin-interleave so the same block_id reappears
    over time — exactly what the sessionizer expects.
    """
    base_ts = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
    sequences: list[list[tuple[str, str]]] = []  # list[ list[(event, blk_id)] ]
    for _ in range(num_blocks):
        blk = str(_random_block_id(rng))
        events = list(_HDFS_NORMAL_LIFECYCLE)
        if rng.random() < anomaly_rate:
            events = _inject_hdfs_anomaly(events, rng)
        sequences.append([(e, blk) for e in events])

    # Round-robin interleave with jitter so blocks don't appear in lockstep.
    cursors = [0] * num_blocks
    pid_counter = 100
    while True:
        active = [i for i, c in enumerate(cursors) if c < len(sequences[i])]
        if not active:
            return
        idx = rng.choice(active)
        event, blk = sequences[idx][cursors[idx]]
        cursors[idx] += 1
        ts = base_ts + timedelta(seconds=rng.randint(0, 30 * 24 * 3600))
        pid = pid_counter
        pid_counter += 1
        yield _format_hdfs_line(ts, pid, event, blk, rng)


def _inject_hdfs_anomaly(
    events: list[str], rng: random.Random
) -> list[str]:
    """Apply one of three anomaly strategies."""
    strategy = rng.choice(("truncate", "reorder", "inject"))
    if strategy == "truncate" and len(events) >= 3:
        # Drop a random terminal event.
        return events[: rng.randint(2, len(events) - 1)]
    if strategy == "reorder" and len(events) >= 3:
        events = list(events)
        i = rng.randint(0, len(events) - 2)
        events[i], events[i + 1] = events[i + 1], events[i]
        return events
    # Inject (default).
    insert_at = rng.randint(1, max(1, len(events) - 1))
    events = list(events)
    events.insert(insert_at, rng.choice(_HDFS_EXCEPTION_EVENTS))
    return events


def _format_hdfs_line(
    ts: datetime, pid: int, event: str, blk: str, rng: random.Random
) -> str:
    template = _HDFS_TEMPLATES[event]
    content = template.format(
        node=rng.randint(0, 255),
        node2=rng.randint(0, 255),
        port=rng.randint(40000, 60000),
        port2=rng.randint(40000, 60000),
        blk=blk[4:],  # strip leading "blk_"
        seq=rng.randint(1, 100000),
        idx=rng.randint(0, 3),
    )
    component = (
        "dfs.FSNamesystem"
        if event in {"E22", "E26", "E5", "E4"}
        else "dfs.DataNode$PacketResponder"
    )
    date_str = ts.strftime("%y%m%d")
    time_str = ts.strftime("%H%M%S")
    return f"{date_str} {time_str} {pid} INFO {component}: {content}"


def _random_block_id(rng: random.Random) -> str:
    n = rng.randint(-(2**62), 2**62)
    return f"blk_{n}"


# ---------- BGL generation ----------


def _bgl_stream(rng: random.Random, anomaly_rate: float) -> Iterator[str]:
    base_ts = datetime(2005, 6, 3, 0, 0, 0, tzinfo=UTC)
    while True:
        ts = base_ts + timedelta(seconds=rng.randint(0, 30 * 24 * 3600))
        node = (
            f"R{rng.randint(0, 31):02d}-M{rng.randint(0, 1)}-N"
            f"{rng.randint(0, 9)}-C:J{rng.randint(0, 18):02d}-U{rng.randint(0, 11):02d}"
        )
        if rng.random() < anomaly_rate:
            label = rng.choice(_BGL_ALERT_CODES)
            level = "FATAL"
            message = "data TLB error interrupt"
        else:
            label = "-"
            level = "INFO"
            message = "instruction cache parity error corrected"
        epoch = int(ts.timestamp())
        date = ts.strftime("%Y.%m.%d")
        time_full = ts.strftime("%Y-%m-%d-%H.%M.%S.") + f"{rng.randint(0, 999999):06d}"
        yield f"{label} {epoch} {date} {node} {time_full} {node} RAS KERNEL {level} {message}"


# ---------- argparse ----------


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate synthetic log lines.")
    p.add_argument("--format", choices=("hdfs", "bgl"), default="hdfs")
    p.add_argument(
        "--total", type=int, default=None, help="emit at most N lines"
    )
    p.add_argument(
        "--duration",
        type=float,
        default=None,
        help="emit for N seconds (mutually exclusive with --total)",
    )
    p.add_argument(
        "--rate",
        type=float,
        default=None,
        help="cap emission to N records/sec (default: unthrottled)",
    )
    p.add_argument(
        "--anomaly-rate",
        type=float,
        default=0.029,
        help="fraction of blocks/lines that should be anomalous (HDFS=0.029 default)",
    )
    p.add_argument(
        "--blocks",
        type=int,
        default=10000,
        help="HDFS only: number of distinct block_ids to interleave",
    )
    p.add_argument("--seed", type=int, default=42)
    p.add_argument(
        "--output",
        default="-",
        help="output path or '-' for stdout (default: -)",
    )
    args = p.parse_args()
    if args.total is None and args.duration is None:
        args.total = 100_000
    if args.total is not None and args.duration is not None:
        p.error("--total and --duration are mutually exclusive")
    if args.anomaly_rate < 0 or args.anomaly_rate > 1:
        p.error("--anomaly-rate must be in [0,1]")
    return args


if __name__ == "__main__":
    sys.exit(main())
