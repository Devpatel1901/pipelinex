#!/usr/bin/env python3
"""Evaluate BGL anomaly detectors in two complementary modes.

BGL ground truth is *per-line*: the first field of every log line is ``-``
(normal) or an alert category code (``KERNDTLB``, ``APPREAD``, …). The
v1 evaluation collapsed this to per-minute event rates, which discarded
most of the signal and capped F1 around 0.20.

This v2 evaluation reports two complementary numbers:

**per-line** — every record is a sample. ``LabelAnomalyDetector`` reads the
classification BGL itself emits and fires accordingly. This is a
trust-the-source baseline and demonstrates the architecture can honor
upstream classifications cleanly.

**per-window** — records are bucketed into time windows. Each window is a
sample. Ground truth = any alert line in the window. Two layers of
detectors run:

- ``window_features``: an unsupervised four-feature detector (alert
  density, severity entropy, node diversity, template novelty) that
  proves the architecture isn't *only* honoring labels.
- ``z_score``/``iqr``/``cusum``: the original rate-only detectors stay
  in the ensemble so their unchanged ~0.20 numbers anchor the "wrong
  feature, not wrong detector" finding.

Outputs
-------
- ``benchmarks/results/bgl_eval.json`` — both modes, all detectors.
- ``BENCHMARKS.md`` — refreshed table.
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
from pipelinex.detectors.label import LabelAnomalyDetector
from pipelinex.detectors.window_features import WindowFeatureDetector
from pipelinex.detectors.zscore import ZScoreDetector
from pipelinex.stages.parsing.bgl_parser import BGLParser

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LOG = REPO_ROOT / "data" / "BGL" / "BGL.log"
RESULTS_DIR = REPO_ROOT / "benchmarks" / "results"
BENCHMARKS_MD = REPO_ROOT / "BENCHMARKS.md"


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Evaluate BGL anomaly detectors.")
    p.add_argument("--log", type=Path, default=DEFAULT_LOG)
    p.add_argument("--max-lines", type=int, default=None)
    p.add_argument(
        "--window-seconds",
        type=int,
        default=60,
        help="bin width for windowed detectors (default 60s)",
    )
    return p.parse_args()


async def amain() -> int:
    args = _parse_args()
    if not args.log.exists():
        print(f"ERROR: BGL log not found at {args.log}", file=sys.stderr)
        return 2

    print(f"[1/3] parsing BGL log: {args.log}")
    records = _parse_records(args.log, args.max_lines)
    print(f"      parsed {len(records):,} records")

    print("[2/3] per-line scoring (LabelAnomalyDetector)")
    per_line = await _per_line_eval(records)
    print(
        f"      label: TP={per_line['label']['tp']:,}  "
        f"FP={per_line['label']['fp']:,}  FN={per_line['label']['fn']:,}  "
        f"P={per_line['label']['precision']:.4f}  "
        f"R={per_line['label']['recall']:.4f}  "
        f"F1={per_line['label']['f1']:.4f}"
    )

    print("[3/3] per-window scoring (window_features + z_score + iqr + cusum)")
    per_window = await _per_window_eval(records, args.window_seconds)
    for det_name in ("window_features", "z_score", "iqr", "cusum"):
        r = per_window[det_name]
        print(
            f"      {det_name:>16s}: TP={r['tp']:>5}  FP={r['fp']:>5}  "
            f"FN={r['fn']:>5}  P={r['precision']:.4f}  "
            f"R={r['recall']:.4f}  F1={r['f1']:.4f}"
        )

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    out = {
        "dataset": "BGL",
        "lines_evaluated": len(records),
        "window_seconds": args.window_seconds,
        "per_line": per_line,
        "per_window": per_window,
    }
    (RESULTS_DIR / "bgl_eval.json").write_text(json.dumps(out, indent=2))
    _update_benchmarks_md(per_line, per_window, args.window_seconds)
    return 0


def _parse_records(log_path: Path, max_lines: int | None) -> list[LogRecord]:
    parser = BGLParser()
    out: list[LogRecord] = []
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
            out.append(rec)
            if (i + 1) % 1_000_000 == 0:
                rate = (i + 1) / (time.perf_counter() - start)
                print(f"      {i + 1:>10,} lines  ({rate:>10,.0f} lines/s)")
    print(f"      skipped {n_skipped:,} unparseable lines")
    return out


async def _per_line_eval(records: list[LogRecord]) -> dict[str, object]:
    detector = LabelAnomalyDetector()
    tp = fp = fn = tn = 0
    for rec in records:
        actual_alert = isinstance(rec.enrichment.get("bgl_label"), str) and rec.enrichment[
            "bgl_label"
        ] != "-"
        anomaly = await detector.detect(rec)
        predicted_alert = anomaly is not None
        if predicted_alert and actual_alert:
            tp += 1
        elif predicted_alert and not actual_alert:
            fp += 1
        elif not predicted_alert and actual_alert:
            fn += 1
        else:
            tn += 1

    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0
    return {
        "label": {
            "tp": tp,
            "fp": fp,
            "fn": fn,
            "tn": tn,
            "precision": round(precision, 4),
            "recall": round(recall, 4),
            "f1": round(f1, 4),
        }
    }


async def _per_window_eval(
    records: list[LogRecord], window_seconds: int
) -> dict[str, object]:
    # Determine the universe of windows and ground-truth labels.
    bins: dict[datetime, int] = defaultdict(int)
    labels: dict[datetime, bool] = defaultdict(bool)
    for rec in records:
        if rec.timestamp is None:
            continue
        ts = rec.timestamp
        window = ts.replace(
            second=(ts.second // window_seconds) * window_seconds, microsecond=0
        )
        if window_seconds >= 60:
            window = window.replace(second=0)
            window = window.replace(
                minute=(ts.minute // (window_seconds // 60)) * (window_seconds // 60)
            )
        bins[window] += 1
        label = rec.enrichment.get("bgl_label")
        if isinstance(label, str) and label != "-":
            labels[window] = True

    sorted_windows = sorted(bins.keys())
    universe = {str(w.timestamp()) for w in sorted_windows}
    actual = {str(w.timestamp()) for w in sorted_windows if labels.get(w, False)}

    out: dict[str, object] = {
        "windows_total": len(universe),
        "anomalous_windows": len(actual),
    }

    # window_features: per-record streaming detector. Replay records in
    # order. Its events anchor to the *first record of the closed window*,
    # but for evaluation we map that anchor back to a window key.
    wf_predicted = await _run_window_features(records, window_seconds)
    out["window_features"] = confusion(
        predicted=wf_predicted, actual=actual, universe=universe
    ).as_dict()

    # z_score / iqr / cusum: aggregate to per-window rate stream, replay.
    rate_series = [(w, bins[w]) for w in sorted_windows]
    for name, factory in (
        ("z_score", lambda: ZScoreDetector(threshold=3.0, window_size=200, min_samples=50)),
        ("iqr", lambda: IQRDetector(k=3.0, window_size=200, min_samples=50)),
        ("cusum", lambda: CUSUMDetector(threshold=5.0, slack=0.5, warmup_samples=50)),
    ):
        det = factory()
        predicted: set[str] = set()
        for ts, rate in rate_series:
            rec = LogRecord(timestamp=ts)
            rec.add_enrichment("metric", float(rate))
            anomaly = await det.detect(rec)
            if anomaly is not None:
                predicted.add(str(ts.timestamp()))
        out[name] = confusion(
            predicted=predicted, actual=actual, universe=universe
        ).as_dict()
    return out


async def _run_window_features(
    records: list[LogRecord], window_seconds: int
) -> set[str]:
    """Run WindowFeatureDetector and return the set of predicted-anomaly
    window keys (string-encoded timestamps to match the universe set)."""
    detector = WindowFeatureDetector(
        window_seconds=float(window_seconds),
        alert_density_threshold=0.05,
        entropy_threshold=1.5,
        novelty_threshold=0.30,
        weight_alert=0.5,
        weight_entropy=0.15,
        weight_node=0.15,
        weight_novelty=0.20,
        score_threshold=0.5,
    )
    predicted: set[str] = set()
    for rec in records:
        anomaly = await detector.detect(rec)
        if anomaly is not None:
            # The event metadata records the actual closed-window key.
            wk = anomaly.metadata.get("window_key")
            if isinstance(wk, int):
                ts_seconds = wk * window_seconds
                predicted.add(str(float(ts_seconds)))
    return predicted


def _update_benchmarks_md(
    per_line: dict[str, object],
    per_window: dict[str, object],
    window_seconds: int,
) -> None:
    per_line_label = per_line["label"]
    pl_rows = [
        {
            "detector": "label (per-line)",
            "TP": per_line_label["tp"],
            "FP": per_line_label["fp"],
            "FN": per_line_label["fn"],
            "precision": f"{per_line_label['precision']:.4f}",
            "recall": f"{per_line_label['recall']:.4f}",
            "F1": f"{per_line_label['f1']:.4f}",
        }
    ]
    pw_rows = []
    for det in ("window_features", "z_score", "iqr", "cusum"):
        r = per_window[det]
        pw_rows.append(
            {
                "detector": det,
                "TP": r["tp"],
                "FP": r["fp"],
                "FN": r["fn"],
                "precision": f"{r['precision']:.4f}",
                "recall": f"{r['recall']:.4f}",
                "F1": f"{r['f1']:.4f}",
            }
        )

    headers = ["detector", "TP", "FP", "FN", "precision", "recall", "F1"]
    pl_table = format_table(pl_rows, headers)
    pw_table = format_table(pw_rows, headers)

    section_marker = "<!-- BEGIN: bgl-point -->"
    end_marker = "<!-- END: bgl-point -->"
    block = (
        f"{section_marker}\n"
        "## BGL — Anomaly detection (per-line label + per-window ensemble)\n\n"
        "### Per-line label detector (trust-the-source)\n\n"
        f"{pl_table}\n\n"
        "### Per-window ensemble\n\n"
        f"{pw_table}\n"
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
