#!/usr/bin/env python3
"""Evaluate point-anomaly detectors on BGL.

The BGL labels are per-line (first field, ``-`` vs alert code). Z-Score /
IQR / CUSUM operate on numeric streams, so we reduce BGL to one rate per
1-minute window:

- ``event_rate``: number of records emitted in that window.
- ``window_label``: anomaly if the window contains at least one alert line.

We then feed each detector the per-window rates in time order and compare
its firings to the window labels.

Outputs
-------
- ``benchmarks/results/bgl_eval.json``
- ``BENCHMARKS.md`` — appended/updated table
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[0]))

from _eval_common import confusion, format_table

from pipelinex.core.models import LogRecord
from pipelinex.detectors.cusum import CUSUMDetector
from pipelinex.detectors.iqr import IQRDetector
from pipelinex.detectors.zscore import ZScoreDetector
from pipelinex.stages.parsing.bgl_parser import BGLParser

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LOG = REPO_ROOT / "data" / "BGL" / "BGL.log"
RESULTS_DIR = REPO_ROOT / "benchmarks" / "results"
BENCHMARKS_MD = REPO_ROOT / "BENCHMARKS.md"


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Evaluate BGL point detectors.")
    p.add_argument("--log", type=Path, default=DEFAULT_LOG)
    p.add_argument("--max-lines", type=int, default=None)
    p.add_argument(
        "--window-seconds",
        type=int,
        default=60,
        help="bin width for rate aggregation (default 60s)",
    )
    return p.parse_args()


async def amain() -> int:
    args = _parse_args()
    if not args.log.exists():
        print(f"ERROR: BGL log not found at {args.log}", file=sys.stderr)
        return 2

    print(f"[1/3] parsing BGL log: {args.log}")
    bins, labels = _aggregate(args.log, args.window_seconds, args.max_lines)
    print(
        f"      {sum(bins.values()):,} parsed lines spread across "
        f"{len(bins):,} {args.window_seconds}-second windows"
    )
    n_anom_windows = sum(1 for v in labels.values() if v)
    print(f"      {n_anom_windows:,}/{len(labels):,} windows contain at least one alert")

    print("[2/3] running detectors over windowed rate stream")
    sorted_keys = sorted(bins.keys())
    rate_series: list[tuple[datetime, int]] = [(k, bins[k]) for k in sorted_keys]

    rows = []
    detector_results = {}
    for name, factory in (
        ("z_score", lambda: ZScoreDetector(threshold=3.0, window_size=200, min_samples=50)),
        ("iqr", lambda: IQRDetector(k=3.0, window_size=200, min_samples=50)),
        ("cusum", lambda: CUSUMDetector(threshold=5.0, slack=0.5, warmup_samples=50)),
    ):
        det = factory()
        predicted_windows: set[datetime] = set()
        for ts, rate in rate_series:
            rec = LogRecord(timestamp=ts)
            rec.add_enrichment("metric", float(rate))
            anomaly = await det.detect(rec)
            if anomaly is not None:
                predicted_windows.add(ts)

        actual_windows = {ts for ts in sorted_keys if labels.get(ts, False)}
        universe = set(sorted_keys)
        cm = confusion(
            predicted={str(t.timestamp()) for t in predicted_windows},
            actual={str(t.timestamp()) for t in actual_windows},
            universe={str(t.timestamp()) for t in universe},
        )
        detector_results[name] = cm.as_dict()
        rows.append(
            {
                "detector": name,
                "windows": len(universe),
                "TP": cm.tp,
                "FP": cm.fp,
                "FN": cm.fn,
                "precision": f"{cm.precision:.4f}",
                "recall": f"{cm.recall:.4f}",
                "F1": f"{cm.f1:.4f}",
            }
        )

    print("\n=== BGL — point-anomaly evaluation ===")
    for r in rows:
        print(
            f"  {r['detector']:>8s}  TP={r['TP']:>5}  FP={r['FP']:>5}  "
            f"FN={r['FN']:>5}  precision={r['precision']}  "
            f"recall={r['recall']}  F1={r['F1']}"
        )

    print("[3/3] writing results")
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    out = {
        "dataset": "BGL",
        "window_seconds": args.window_seconds,
        "windows": len(sorted_keys),
        "results": detector_results,
    }
    (RESULTS_DIR / "bgl_eval.json").write_text(json.dumps(out, indent=2))
    _update_benchmarks_md(rows, args.window_seconds)
    return 0


def _aggregate(
    log_path: Path, window_seconds: int, max_lines: int | None
) -> tuple[dict[datetime, int], dict[datetime, bool]]:
    parser = BGLParser()
    bins: dict[datetime, int] = defaultdict(int)
    labels: dict[datetime, bool] = defaultdict(bool)
    n_skipped = 0
    start = time.perf_counter()
    with log_path.open(encoding="utf-8", errors="replace") as fh:
        for i, raw in enumerate(fh):
            if max_lines is not None and i >= max_lines:
                break
            raw = raw.rstrip("\n")
            if not raw:
                continue
            try:
                rec = parser.parse(raw)
            except Exception:
                n_skipped += 1
                continue
            ts = rec.timestamp
            if ts is None:
                continue
            window = ts.replace(
                second=(ts.second // window_seconds) * window_seconds,
                microsecond=0,
            )
            if window_seconds >= 60:
                window = window.replace(second=0)
                window = window.replace(
                    minute=(ts.minute // (window_seconds // 60))
                    * (window_seconds // 60)
                )
            bins[window] += 1
            label = rec.enrichment.get("bgl_label")
            if isinstance(label, str) and label != "-":
                labels[window] = True
            if (i + 1) % 1_000_000 == 0:
                rate = (i + 1) / (time.perf_counter() - start)
                print(f"      {i + 1:>10,} lines  ({rate:>10,.0f} lines/s)")
    print(f"      skipped {n_skipped:,} unparseable lines")
    return dict(bins), dict(labels)


def _update_benchmarks_md(rows: list[dict[str, object]], window_seconds: int) -> None:
    table = format_table(
        rows=rows,
        headers=[
            "detector",
            "windows",
            "TP",
            "FP",
            "FN",
            "precision",
            "recall",
            "F1",
        ],
    )
    section_marker = "<!-- BEGIN: bgl-point -->"
    end_marker = "<!-- END: bgl-point -->"
    block = (
        f"{section_marker}\n"
        "## BGL — Point-anomaly detection (Z-Score, IQR, CUSUM)\n\n"
        f"{table}\n"
        f"\n_window = {window_seconds} seconds; metric = events per window_\n"
        f"{end_marker}\n"
    )
    if BENCHMARKS_MD.exists():
        text = BENCHMARKS_MD.read_text(encoding="utf-8")
        if section_marker in text and end_marker in text:
            before = text.split(section_marker, 1)[0]
            after = text.split(end_marker, 1)[1]
            BENCHMARKS_MD.write_text(before + block + after, encoding="utf-8")
        else:
            BENCHMARKS_MD.write_text(text.rstrip() + "\n\n" + block, encoding="utf-8")
    else:
        BENCHMARKS_MD.write_text("# PipelineX Benchmarks\n\n" + block, encoding="utf-8")


def main() -> int:
    return asyncio.run(amain())


if __name__ == "__main__":
    sys.exit(main())
