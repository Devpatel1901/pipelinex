#!/usr/bin/env python3
"""Train the bigram-statistics sequence model from labeled HDFS data.

Mirrors ``scripts/train_sequence_model.py`` but produces a counts model
(``SequenceStatsModel``) for the likelihood detector instead of a
set-membership model. Both trainers share the parsing/grouping loop via
``scripts/_hdfs_common.py``.

Pipeline
--------
1. Load labels from ``anomaly_label.csv``.
2. Stream-parse ``HDFS.log`` via ``_hdfs_common.parse_hdfs_blocks``.
3. From labeled-Normal blocks only, build ``transitions[a][b] = count`` and
   ``unigram_counts[a]``.
4. Persist as ``models/hdfs_ngram_stats_v1.json``.

Usage::

    python scripts/train_sequence_stats_model.py
    python scripts/train_sequence_stats_model.py --max-lines 200000  # quick run
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path

# Make src/ importable when running directly.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[0]))

from _hdfs_common import load_labels, parse_hdfs_blocks  # noqa: E402

from pipelinex.detectors.sequence_loglikelihood import SequenceStatsModel  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LOG = REPO_ROOT / "data" / "HDFS" / "HDFS.log"
DEFAULT_LABELS = REPO_ROOT / "data" / "HDFS" / "preprocessed" / "anomaly_label.csv"
DEFAULT_TEMPLATES = (
    REPO_ROOT / "data" / "HDFS" / "preprocessed" / "HDFS.log_templates.csv"
)
DEFAULT_MODEL_OUT = REPO_ROOT / "models" / "hdfs_ngram_stats_v1.json"


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Train HDFS bigram-statistics sequence model."
    )
    p.add_argument("--log", type=Path, default=DEFAULT_LOG)
    p.add_argument("--labels", type=Path, default=DEFAULT_LABELS)
    p.add_argument("--templates", type=Path, default=DEFAULT_TEMPLATES)
    p.add_argument("--out", type=Path, default=DEFAULT_MODEL_OUT)
    p.add_argument("--max-lines", type=int, default=None)
    return p.parse_args()


def main() -> int:
    args = _parse_args()
    if not args.log.exists():
        print(f"ERROR: HDFS log not found at {args.log}", file=sys.stderr)
        return 2
    if not args.labels.exists():
        print(f"ERROR: anomaly_label.csv not found at {args.labels}", file=sys.stderr)
        return 2

    # --- Step 1: load labels.
    print(f"[1/4] loading labels from {args.labels}")
    labels = load_labels(args.labels)
    n_anom = sum(1 for v in labels.values() if v == "Anomaly")
    n_norm = sum(1 for v in labels.values() if v == "Normal")
    print(f"      {len(labels):,} labeled blocks ({n_anom:,} anomaly, {n_norm:,} normal)")

    # --- Step 2: stream-parse HDFS.log and group by block_id.
    print(f"[2/4] parsing HDFS log: {args.log}")
    block_events, stats = parse_hdfs_blocks(
        log_path=args.log,
        templates_path=args.templates,
        max_lines=args.max_lines,
    )
    print(
        f"      parsed {stats['n_parsed']:,} lines "
        f"(matched {stats['n_matched']:,}, skipped {stats['n_skipped']:,}, "
        f"{stats['n_blocks']:,} distinct blocks) in {stats['elapsed_s']}s"
    )

    # --- Step 3: build bigram counts from labeled-Normal blocks only.
    print("[3/4] counting bigrams from Normal blocks")
    transitions: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    unigram_counts: dict[str, int] = defaultdict(int)
    vocabulary: set[str] = set()
    n_normal_used = 0
    n_unlabeled = 0
    for block_id, events in block_events.items():
        label = labels.get(block_id)
        if label is None:
            n_unlabeled += 1
            continue
        vocabulary.update(events)
        if label != "Normal":
            continue
        n_normal_used += 1
        for i in range(len(events) - 1):
            a, b = events[i], events[i + 1]
            transitions[a][b] += 1
            unigram_counts[a] += 1

    n_unique_pairs = sum(len(inner) for inner in transitions.values())
    print(
        f"      {n_normal_used:,} normal blocks contributed "
        f"{n_unique_pairs:,} unique bigrams "
        f"(vocab size {len(vocabulary)}, "
        f"{n_unlabeled:,} blocks were unlabeled and ignored)"
    )

    # --- Step 4: persist.
    print(f"[4/4] writing model to {args.out}")
    args.out.parent.mkdir(parents=True, exist_ok=True)
    model = SequenceStatsModel(
        n=2,
        vocabulary=frozenset(vocabulary),
        transitions={a: dict(inner) for a, inner in transitions.items()},
        unigram_counts=dict(unigram_counts),
    )
    args.out.write_text(json.dumps(model.to_dict(), indent=2))
    print("      done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
