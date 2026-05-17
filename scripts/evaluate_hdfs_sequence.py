#!/usr/bin/env python3
"""Evaluate the KL-divergence sequence detector against HDFS_v1 ground truth.

Reuses the trainer's parsing + sessionization to assemble per-block event
sequences, then scores each block with ``KLDivergenceSequenceDetector`` and
joins predictions to ``anomaly_label.csv`` for precision/recall/F1.

Outputs
-------
- ``benchmarks/results/hdfs_eval.json`` — confusion matrix + summary stats.
- ``BENCHMARKS.md`` — appended/updated table row.

Usage::

    python scripts/evaluate_hdfs_sequence.py
    python scripts/evaluate_hdfs_sequence.py --threshold 1.0
    python scripts/evaluate_hdfs_sequence.py --sweep    # try several thresholds
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import sys
import time
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[0]))

from _eval_common import confusion, format_table

from pipelinex.core.models import BlockTrace
from pipelinex.detectors.sequence_kl import (
    KLDivergenceSequenceDetector,
    KLSequenceModel,
)
from pipelinex.stages.enrichment.template_matcher import TemplateMatcherStage
from pipelinex.stages.parsing.hdfs_parser import HDFSParser

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LOG = REPO_ROOT / "data" / "HDFS" / "HDFS.log"
DEFAULT_LABELS = REPO_ROOT / "data" / "HDFS" / "preprocessed" / "anomaly_label.csv"
DEFAULT_TEMPLATES = (
    REPO_ROOT / "data" / "HDFS" / "preprocessed" / "HDFS.log_templates.csv"
)
DEFAULT_MODEL = REPO_ROOT / "models" / "hdfs_kl_v1.json"
RESULTS_DIR = REPO_ROOT / "benchmarks" / "results"
BENCHMARKS_MD = REPO_ROOT / "BENCHMARKS.md"

SWEEP_THRESHOLDS = (0.1, 0.3, 0.5, 0.75, 1.0, 1.5, 2.0, 3.0, 5.0)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Evaluate HDFS KL sequence detector.")
    p.add_argument("--log", type=Path, default=DEFAULT_LOG)
    p.add_argument("--labels", type=Path, default=DEFAULT_LABELS)
    p.add_argument("--templates", type=Path, default=DEFAULT_TEMPLATES)
    p.add_argument("--model", type=Path, default=DEFAULT_MODEL)
    p.add_argument("--threshold", type=float, default=0.3)
    p.add_argument(
        "--score-mode",
        choices=("kl", "max_contrib"),
        default="max_contrib",
        help="Scoring mode: 'kl' (full divergence) or 'max_contrib' (worst bigram).",
    )
    p.add_argument(
        "--sweep",
        action="store_true",
        help=f"Score at multiple thresholds {SWEEP_THRESHOLDS} and report the best.",
    )
    p.add_argument("--max-lines", type=int, default=None)
    return p.parse_args()


async def amain() -> int:
    args = _parse_args()
    for p in (args.log, args.labels, args.templates, args.model):
        if not p.exists():
            print(f"ERROR: missing input: {p}", file=sys.stderr)
            return 2

    print(f"[1/4] loading model from {args.model}")
    model = KLSequenceModel.load(args.model)
    print(
        f"      n={model.n}, unique_bigrams={len(model.bigram_counts):,}, "
        f"vocab={len(model.vocabulary):,}, total={model.total_bigrams:,}, "
        f"alpha={model.alpha}"
    )

    print(f"[2/4] loading labels from {args.labels}")
    labels = _load_labels(args.labels)
    print(f"      {len(labels):,} labeled blocks")

    print(f"[3/4] parsing + classifying log: {args.log}")
    block_events = _build_block_events(args)

    print(f"[4/4] scoring blocks (score_mode={args.score_mode})")
    if args.sweep:
        sweep = await _sweep_thresholds(model, labels, block_events, args.score_mode)
        best = max(sweep, key=lambda r: r["f1"])
        print(f"\n=== HDFS_v1 KL threshold sweep (mode={args.score_mode}) ===")
        for r in sweep:
            print(
                f"  threshold={r['threshold']:>4}  "
                f"TP={r['tp']:>5}  FP={r['fp']:>5}  FN={r['fn']:>5}  "
                f"P={r['precision']:.4f}  R={r['recall']:.4f}  F1={r['f1']:.4f}"
            )
        print(f"\n  best threshold: {best['threshold']} (F1={best['f1']:.4f})")
        chosen = best
    else:
        chosen = await _score_at_threshold(
            model, labels, block_events, args.threshold, args.score_mode
        )
        print("\n=== HDFS_v1 KL sequence-detector evaluation ===")
        print(f"  blocks scored : {chosen['blocks_scored']:,}")
        print(f"  ground truth  : {chosen['anomalies_in_universe']:,} anomalies")
        print(f"  predicted     : {chosen['predicted']:,}")
        print(f"  TP={chosen['tp']}  FP={chosen['fp']}  TN={chosen['tn']}  FN={chosen['fn']}")
        print(f"  precision     : {chosen['precision']:.4f}")
        print(f"  recall        : {chosen['recall']:.4f}")
        print(f"  F1            : {chosen['f1']:.4f}")

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    out = {
        "dataset": "HDFS_v1",
        "detector": "sequence_kl",
        "n": model.n,
        "alpha": model.alpha,
        "score_mode": args.score_mode,
        **chosen,
    }
    (RESULTS_DIR / "hdfs_eval.json").write_text(json.dumps(out, indent=2))
    _update_benchmarks_md(out)
    return 0


async def _score_at_threshold(
    model: KLSequenceModel,
    labels: dict[str, str],
    block_events: dict[str, list[str]],
    threshold: float,
    score_mode: str = "max_contrib",
) -> dict[str, object]:
    detector = KLDivergenceSequenceDetector(
        model=model,
        threshold=threshold,
        alpha=model.alpha,
        score_mode=score_mode,
    )
    predicted: set[str] = set()
    universe: set[str] = set()
    for block_id, events in block_events.items():
        if block_id not in labels:
            continue
        universe.add(block_id)
        trace = BlockTrace(
            block_id=block_id,
            event_sequence=tuple(events),
            record_ids=(),
            first_timestamp=datetime.now(UTC),
            last_timestamp=datetime.now(UTC),
            record_count=len(events),
            pipeline_run_id=uuid4(),
            closed_reason="terminal",
        )
        anomaly = await detector.detect_trace(trace)
        if anomaly is not None:
            predicted.add(block_id)
    actual = {b for b, lab in labels.items() if lab == "Anomaly" and b in universe}
    cm = confusion(predicted=predicted, actual=actual, universe=universe)
    return {
        "threshold": threshold,
        "score_mode": score_mode,
        "blocks_scored": len(universe),
        "anomalies_in_universe": len(actual),
        "predicted": len(predicted),
        **cm.as_dict(),
    }


async def _sweep_thresholds(
    model: KLSequenceModel,
    labels: dict[str, str],
    block_events: dict[str, list[str]],
    score_mode: str = "max_contrib",
) -> list[dict[str, object]]:
    out = []
    for thr in SWEEP_THRESHOLDS:
        out.append(await _score_at_threshold(model, labels, block_events, thr, score_mode))
    return out


def _build_block_events(args: argparse.Namespace) -> dict[str, list[str]]:
    parser = HDFSParser()
    matcher = TemplateMatcherStage(args.templates)
    block_events: dict[str, list[str]] = defaultdict(list)
    start = time.perf_counter()
    with args.log.open(encoding="utf-8", errors="replace") as fh:
        for i, raw in enumerate(fh):
            if args.max_lines is not None and i >= args.max_lines:
                break
            raw = raw.rstrip("\n")
            if not raw:
                continue
            try:
                rec = parser.parse(raw)
            except Exception:
                continue
            block_id = rec.enrichment.get("block_id")
            if not isinstance(block_id, str):
                continue
            for eid, pat in matcher._patterns:  # type: ignore[attr-defined]
                if pat.match(rec.message):
                    block_events[block_id].append(eid)
                    break
    print(
        f"      parsed in {time.perf_counter() - start:.1f}s; "
        f"{len(block_events):,} distinct blocks observed"
    )
    return block_events


def _load_labels(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    with path.open(encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            block_id = row.get("BlockId")
            label = row.get("Label")
            if block_id and label:
                out[block_id] = label
    return out


def _update_benchmarks_md(record: dict[str, object]) -> None:
    line = format_table(
        rows=[
            {
                "detector": record["detector"],
                "blocks_scored": record["blocks_scored"],
                "TP": record["tp"],
                "FP": record["fp"],
                "FN": record["fn"],
                "precision": f"{record['precision']:.4f}",
                "recall": f"{record['recall']:.4f}",
                "F1": f"{record['f1']:.4f}",
            }
        ],
        headers=[
            "detector",
            "blocks_scored",
            "TP",
            "FP",
            "FN",
            "precision",
            "recall",
            "F1",
        ],
    )

    section_marker = "<!-- BEGIN: hdfs-sequence -->"
    end_marker = "<!-- END: hdfs-sequence -->"
    block = (
        f"{section_marker}\n"
        "## HDFS_v1 — Sequence-anomaly detection (KL-divergence)\n\n"
        f"{line}\n"
        f"\n_threshold = {record['threshold']} (nats), n = {record['n']}, "
        f"alpha = {record['alpha']}_\n"
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
