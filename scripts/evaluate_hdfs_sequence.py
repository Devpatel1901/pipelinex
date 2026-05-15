#!/usr/bin/env python3
"""Evaluate sequence anomaly detectors against the HDFS_v1 ground truth.

Supports two detectors selected via ``--detector``:

- ``sequence_ngram`` (default): set-membership novelty over labeled-Normal
  bigrams. Single operating point; reports P/R/F1.
- ``sequence_loglikelihood``: mean log-prob under a Laplace-smoothed bigram
  model. Sweeps ``--threshold-min..--threshold-max`` and writes one row
  per threshold to ``benchmarks/results/hdfs_eval_loglik.json`` plus a
  PR-curve PNG (if matplotlib is available).

Both flows share the parsing + grouping loop from ``_hdfs_common``.

Outputs
-------
- ``benchmarks/results/hdfs_eval.json`` (n-gram) or
  ``benchmarks/results/hdfs_eval_loglik.json`` (loglikelihood)
- ``BENCHMARKS.md`` — appended/updated table block for the active detector
- ``benchmarks/results/hdfs_eval_pr_curve.png`` (loglikelihood only,
  optional)
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[0]))

from _eval_common import ConfusionMatrix, confusion, format_table  # noqa: E402
from _hdfs_common import load_labels, parse_hdfs_blocks  # noqa: E402

from pipelinex.core.models import BlockTrace  # noqa: E402
from pipelinex.detectors.sequence import (  # noqa: E402
    SequenceAnomalyDetector,
    SequenceModel,
)
from pipelinex.detectors.sequence_loglikelihood import (  # noqa: E402
    SequenceStatsModel,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LOG = REPO_ROOT / "data" / "HDFS" / "HDFS.log"
DEFAULT_LABELS = REPO_ROOT / "data" / "HDFS" / "preprocessed" / "anomaly_label.csv"
DEFAULT_TEMPLATES = (
    REPO_ROOT / "data" / "HDFS" / "preprocessed" / "HDFS.log_templates.csv"
)
DEFAULT_NGRAM_MODEL = REPO_ROOT / "models" / "hdfs_ngram_v1.json"
DEFAULT_STATS_MODEL = REPO_ROOT / "models" / "hdfs_ngram_stats_v1.json"
RESULTS_DIR = REPO_ROOT / "benchmarks" / "results"
BENCHMARKS_MD = REPO_ROOT / "BENCHMARKS.md"

DETECTORS = ("sequence_ngram", "sequence_loglikelihood")


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Evaluate HDFS sequence detector.")
    p.add_argument("--log", type=Path, default=DEFAULT_LOG)
    p.add_argument("--labels", type=Path, default=DEFAULT_LABELS)
    p.add_argument("--templates", type=Path, default=DEFAULT_TEMPLATES)
    p.add_argument(
        "--detector",
        choices=DETECTORS,
        default="sequence_ngram",
        help="which sequence detector to evaluate",
    )
    p.add_argument(
        "--model",
        type=Path,
        default=None,
        help="model path (default depends on --detector)",
    )
    p.add_argument(
        "--threshold",
        type=float,
        default=0.0,
        help="for sequence_ngram only (novelty ratio cutoff)",
    )
    p.add_argument("--threshold-min", type=float, default=-10.0,
                   help="loglikelihood sweep: lower bound of mean_log_prob threshold")
    p.add_argument("--threshold-max", type=float, default=-1.0,
                   help="loglikelihood sweep: upper bound")
    p.add_argument("--threshold-step", type=float, default=0.5,
                   help="loglikelihood sweep step")
    p.add_argument("--alpha", type=float, default=0.1,
                   help="loglikelihood Laplace smoothing constant")
    p.add_argument("--max-lines", type=int, default=None)
    return p.parse_args()


async def amain() -> int:
    args = _parse_args()
    model_path = args.model or (
        DEFAULT_NGRAM_MODEL if args.detector == "sequence_ngram" else DEFAULT_STATS_MODEL
    )
    for p in (args.log, args.labels, args.templates, model_path):
        if not p.exists():
            print(f"ERROR: missing input: {p}", file=sys.stderr)
            return 2

    print(f"[1/4] loading {args.detector} model from {model_path}")
    print(f"[2/4] loading labels from {args.labels}")
    labels = load_labels(args.labels)
    print(f"      {len(labels):,} labeled blocks")

    print(f"[3/4] parsing + classifying log: {args.log}")
    block_events, stats = parse_hdfs_blocks(
        log_path=args.log,
        templates_path=args.templates,
        max_lines=args.max_lines,
    )
    print(
        f"      parsed in {stats['elapsed_s']}s; "
        f"{stats['n_blocks']:,} distinct blocks observed"
    )

    print("[4/4] scoring blocks")
    if args.detector == "sequence_ngram":
        return await _eval_ngram(
            block_events=block_events,
            labels=labels,
            model_path=model_path,
            threshold=args.threshold,
        )
    return await _eval_loglikelihood(
        block_events=block_events,
        labels=labels,
        model_path=model_path,
        threshold_min=args.threshold_min,
        threshold_max=args.threshold_max,
        threshold_step=args.threshold_step,
        alpha=args.alpha,
    )


def _make_trace(block_id: str, events: list[str]) -> BlockTrace:
    now = datetime.now(UTC)
    return BlockTrace(
        block_id=block_id,
        event_sequence=tuple(events),
        record_ids=(),
        first_timestamp=now,
        last_timestamp=now,
        record_count=len(events),
        pipeline_run_id=uuid4(),
        closed_reason="terminal",
    )


async def _eval_ngram(
    block_events: dict[str, list[str]],
    labels: dict[str, str],
    model_path: Path,
    threshold: float,
) -> int:
    model = SequenceModel.load(model_path)
    detector = SequenceAnomalyDetector(model=model, threshold=threshold)
    print(
        f"      n={model.n}, normal_ngrams={len(model.normal_ngrams):,}, "
        f"vocab={len(model.vocabulary):,}"
    )

    predicted: set[str] = set()
    universe: set[str] = set()
    for block_id, events in block_events.items():
        if block_id not in labels:
            continue
        universe.add(block_id)
        trace = _make_trace(block_id, events)
        anomaly = await detector.detect_trace(trace)
        if anomaly is not None:
            predicted.add(block_id)

    actual = {b for b, lab in labels.items() if lab == "Anomaly" and b in universe}
    cm = confusion(predicted=predicted, actual=actual, universe=universe)

    print("\n=== HDFS_v1 sequence_ngram evaluation ===")
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
        "threshold": threshold,
        "blocks_scored": len(universe),
        **cm.as_dict(),
    }
    (RESULTS_DIR / "hdfs_eval.json").write_text(json.dumps(out, indent=2))
    _update_ngram_section(out)
    return 0


async def _eval_loglikelihood(
    block_events: dict[str, list[str]],
    labels: dict[str, str],
    model_path: Path,
    threshold_min: float,
    threshold_max: float,
    threshold_step: float,
    alpha: float,
) -> int:
    model = SequenceStatsModel.load(model_path)
    print(
        f"      n={model.n}, transitions={sum(len(v) for v in model.transitions.values()):,}, "
        f"vocab={len(model.vocabulary):,}"
    )

    # 1) Compute mean_log_prob ONCE per block (read-only over the model);
    # then the threshold sweep is a pure comparison loop. We inline the
    # likelihood math (rather than calling detect_trace per threshold) to
    # avoid building AnomalyEvent objects N_thresholds times per block.
    import math

    vocab_size = max(1, len(model.vocabulary))
    block_score: dict[str, float] = {}
    t0 = time.perf_counter()
    for block_id, events in block_events.items():
        if block_id not in labels or len(events) < 2:
            continue
        log_probs: list[float] = []
        for i in range(len(events) - 1):
            a, b = events[i], events[i + 1]
            row = model.transitions.get(a, {})
            num = row.get(b, 0) + alpha
            denom = model.unigram_counts.get(a, 0) + alpha * vocab_size
            log_probs.append(math.log(num / denom))
        block_score[block_id] = sum(log_probs) / len(log_probs)
    print(
        f"      scored {len(block_score):,} blocks "
        f"({(time.perf_counter() - t0):.1f}s)"
    )

    # 2) Sweep thresholds.
    universe = {b for b in block_score.keys()}
    actual = {b for b, lab in labels.items() if lab == "Anomaly" and b in universe}

    thresholds: list[float] = []
    t = threshold_min
    while t <= threshold_max + 1e-9:
        thresholds.append(round(t, 4))
        t += threshold_step

    rows = []
    sweep_records: list[dict[str, object]] = []
    best: tuple[float, ConfusionMatrix] | None = None
    for thr in thresholds:
        predicted = {b for b, s in block_score.items() if s < thr}
        cm = confusion(predicted=predicted, actual=actual, universe=universe)
        rows.append(
            {
                "threshold": f"{thr:.2f}",
                "predicted": len(predicted),
                "TP": cm.tp,
                "FP": cm.fp,
                "FN": cm.fn,
                "precision": f"{cm.precision:.4f}",
                "recall": f"{cm.recall:.4f}",
                "F1": f"{cm.f1:.4f}",
            }
        )
        sweep_records.append(
            {
                "threshold": thr,
                "predicted": len(predicted),
                **cm.as_dict(),
            }
        )
        if best is None or cm.f1 > best[1].f1:
            best = (thr, cm)

    print("\n=== HDFS_v1 sequence_loglikelihood threshold sweep ===")
    for r in rows:
        print(
            f"  thr={r['threshold']:>6s}  TP={r['TP']:>5}  FP={r['FP']:>5}  "
            f"FN={r['FN']:>5}  P={r['precision']}  R={r['recall']}  F1={r['F1']}"
        )
    assert best is not None
    thr_star, cm_star = best
    print(
        f"\n  best F1 at threshold={thr_star:.2f}: "
        f"P={cm_star.precision:.4f}  R={cm_star.recall:.4f}  F1={cm_star.f1:.4f}"
    )

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    out = {
        "dataset": "HDFS_v1",
        "detector": "sequence_loglikelihood",
        "alpha": alpha,
        "n": model.n,
        "blocks_scored": len(universe),
        "best": {"threshold": thr_star, **cm_star.as_dict()},
        "sweep": sweep_records,
    }
    (RESULTS_DIR / "hdfs_eval_loglik.json").write_text(json.dumps(out, indent=2))
    _update_loglik_section(rows, alpha=alpha, n=model.n, best=(thr_star, cm_star))
    _maybe_save_pr_curve(sweep_records)
    return 0


def _update_ngram_section(record: dict[str, object]) -> None:
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
    _replace_section(section_marker, end_marker, block)


def _update_loglik_section(
    rows: list[dict[str, object]],
    alpha: float,
    n: int,
    best: tuple[float, ConfusionMatrix],
) -> None:
    table = format_table(
        rows=rows,
        headers=[
            "threshold",
            "predicted",
            "TP",
            "FP",
            "FN",
            "precision",
            "recall",
            "F1",
        ],
    )
    thr_star, cm_star = best
    section_marker = "<!-- BEGIN: hdfs-sequence-loglik -->"
    end_marker = "<!-- END: hdfs-sequence-loglik -->"
    block = (
        f"{section_marker}\n"
        "## HDFS_v1 — Sequence-anomaly detection (log-likelihood)\n\n"
        f"{table}\n"
        f"\n_n = {n}, alpha = {alpha}; "
        f"best F1 at threshold={thr_star:.2f}: "
        f"P={cm_star.precision:.4f}, R={cm_star.recall:.4f}, F1={cm_star.f1:.4f}_\n"
        f"{end_marker}\n"
    )
    _replace_section(section_marker, end_marker, block)


def _replace_section(section_marker: str, end_marker: str, block: str) -> None:
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


def _maybe_save_pr_curve(sweep_records: list[dict[str, object]]) -> None:
    """Render a precision/recall curve to PNG. No-op if matplotlib is absent."""
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        return
    if not sweep_records:
        return
    precisions = [float(r["precision"]) for r in sweep_records]
    recalls = [float(r["recall"]) for r in sweep_records]
    f1s = [float(r["f1"]) for r in sweep_records]
    thresholds = [float(r["threshold"]) for r in sweep_records]
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 4))
    ax1.plot(recalls, precisions, marker="o")
    ax1.set_xlabel("Recall")
    ax1.set_ylabel("Precision")
    ax1.set_title("HDFS_v1 — sequence_loglikelihood PR curve")
    ax1.grid(True, alpha=0.3)
    ax1.set_xlim(0, 1)
    ax1.set_ylim(0, 1)
    ax2.plot(thresholds, f1s, marker="o", color="tab:orange")
    ax2.set_xlabel("threshold (mean log-prob cutoff)")
    ax2.set_ylabel("F1")
    ax2.set_title("F1 vs threshold")
    ax2.grid(True, alpha=0.3)
    fig.tight_layout()
    out = RESULTS_DIR / "hdfs_eval_pr_curve.png"
    fig.savefig(out, dpi=140)
    plt.close(fig)
    print(f"      wrote {out}")


def main() -> int:
    return asyncio.run(amain())


if __name__ == "__main__":
    sys.exit(main())
