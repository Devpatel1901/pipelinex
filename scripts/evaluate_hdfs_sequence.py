#!/usr/bin/env python3
"""Evaluate the sequence detector against the HDFS_v1 ground truth.

Reuses the trainer's parsing + sessionization to assemble per-block event
sequences, then scores each block with ``SequenceAnomalyDetector`` and
joins predictions to ``anomaly_label.csv`` for precision/recall/F1.

Outputs
-------
- ``benchmarks/results/hdfs_eval.json`` — confusion matrix + summary stats.
- ``BENCHMARKS.md`` — appended/updated table row.
- ``benchmarks/results/hdfs_eval_confusion.png`` (optional, matplotlib).

Usage::

    python scripts/evaluate_hdfs_sequence.py
    python scripts/evaluate_hdfs_sequence.py --model models/hdfs_ngram_v1.json
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import sys
import time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[0]))

from _eval_common import confusion, format_table

from pipelinex.detectors.sequence import (
    SequenceAnomalyDetector,
    SequenceModel,
)
from pipelinex.stages.enrichment.template_matcher import TemplateMatcherStage
from pipelinex.stages.parsing.hdfs_parser import HDFSParser

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LOG = REPO_ROOT / "data" / "HDFS" / "HDFS.log"
DEFAULT_LABELS = REPO_ROOT / "data" / "HDFS" / "preprocessed" / "anomaly_label.csv"
DEFAULT_TEMPLATES = (
    REPO_ROOT / "data" / "HDFS" / "preprocessed" / "HDFS.log_templates.csv"
)
DEFAULT_MODEL = REPO_ROOT / "models" / "hdfs_ngram_v1.json"
RESULTS_DIR = REPO_ROOT / "benchmarks" / "results"
BENCHMARKS_MD = REPO_ROOT / "BENCHMARKS.md"


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Evaluate HDFS sequence detector.")
    p.add_argument("--log", type=Path, default=DEFAULT_LOG)
    p.add_argument("--labels", type=Path, default=DEFAULT_LABELS)
    p.add_argument("--templates", type=Path, default=DEFAULT_TEMPLATES)
    p.add_argument("--model", type=Path, default=DEFAULT_MODEL)
    p.add_argument("--threshold", type=float, default=0.0)
    p.add_argument("--max-lines", type=int, default=None)
    return p.parse_args()


async def amain() -> int:
    args = _parse_args()
    for p in (args.log, args.labels, args.templates, args.model):
        if not p.exists():
            print(f"ERROR: missing input: {p}", file=sys.stderr)
            return 2

    print(f"[1/4] loading model from {args.model}")
    model = SequenceModel.load(args.model)
    detector = SequenceAnomalyDetector(model=model, threshold=args.threshold)
    print(f"      n={model.n}, normal_ngrams={len(model.normal_ngrams):,}, "
          f"vocab={len(model.vocabulary):,}")

    print(f"[2/4] loading labels from {args.labels}")
    labels = _load_labels(args.labels)
    print(f"      {len(labels):,} labeled blocks")

    print(f"[3/4] parsing + classifying log: {args.log}")
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
    print(f"      parsed in {time.perf_counter() - start:.1f}s; "
          f"{len(block_events):,} distinct blocks observed")

    print("[4/4] scoring blocks")
    predicted: set[str] = set()
    universe: set[str] = set()
    for block_id, events in block_events.items():
        if block_id not in labels:
            continue
        universe.add(block_id)
        # Build a thin trace just for the detector — we don't need
        # timestamps or record_ids for scoring.
        from datetime import UTC, datetime
        from uuid import uuid4

        from pipelinex.core.models import BlockTrace

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

    print("\n=== HDFS_v1 sequence-detector evaluation ===")
    print(f"  blocks scored : {len(universe):,}")
    print(f"  ground truth  : {len(actual):,} anomalies")
    print(f"  predicted     : {len(predicted):,}")
    print(f"  TP={cm.tp}  FP={cm.fp}  TN={cm.tn}  FN={cm.fn}")
    print(f"  precision     : {cm.precision:.4f}")
    print(f"  recall        : {cm.recall:.4f}")
    print(f"  F1            : {cm.f1:.4f}")

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    out = {
        "dataset": "HDFS_v1",
        "detector": "sequence_ngram",
        "n": model.n,
        "threshold": args.threshold,
        "blocks_scored": len(universe),
        **cm.as_dict(),
    }
    (RESULTS_DIR / "hdfs_eval.json").write_text(json.dumps(out, indent=2))

    _update_benchmarks_md(out)
    return 0


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
    """Write or update the HDFS row in BENCHMARKS.md.

    For simplicity we just (re)render the whole 'HDFS_v1 sequence' table.
    Day 19 will fold this into a richer benchmarks doc.
    """
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
        "## HDFS_v1 — Sequence-anomaly detection (n-gram)\n\n"
        f"{line}\n"
        f"\n_threshold = {record['threshold']}, n = {record['n']}_\n"
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
