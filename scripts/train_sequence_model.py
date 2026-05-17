#!/usr/bin/env python3
"""Train the KL-divergence sequence model from labeled HDFS data.

Pipeline
--------
1. Stream-parse ``data/HDFS/HDFS.log`` line-by-line via ``HDFSParser``.
2. Match each parsed line to one of the 29 templates in
   ``HDFS.log_templates.csv`` to assign an event_id.
3. Group events by ``block_id`` (extracted from the message).
4. Join blocks with ``data/HDFS/preprocessed/anomaly_label.csv``.
5. Count bigram occurrences across labelled-Normal blocks.
6. Persist the model as ``models/hdfs_kl_v1.json`` (Counter, not set).

Memory: streams the file; keeps one ``dict[block_id, list[str]]`` plus
the labels CSV (a flat ~18 MB dict). On a laptop with 8 GB RAM this fits
with room to spare; the heaviest object is the per-block event lists.

Usage::

    python scripts/train_sequence_model.py
    python scripts/train_sequence_model.py --max-lines 200000  # quick run
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

# Make src/ importable when running directly.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pipelinex.detectors.sequence_kl import KLSequenceModel, ngrams
from pipelinex.stages.enrichment.template_matcher import TemplateMatcherStage
from pipelinex.stages.parsing.hdfs_parser import HDFSParser

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LOG = REPO_ROOT / "data" / "HDFS" / "HDFS.log"
DEFAULT_LABELS = REPO_ROOT / "data" / "HDFS" / "preprocessed" / "anomaly_label.csv"
DEFAULT_TEMPLATES = (
    REPO_ROOT / "data" / "HDFS" / "preprocessed" / "HDFS.log_templates.csv"
)
DEFAULT_MODEL_OUT = REPO_ROOT / "models" / "hdfs_kl_v1.json"


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Train HDFS KL-divergence sequence model.")
    p.add_argument("--log", type=Path, default=DEFAULT_LOG)
    p.add_argument("--labels", type=Path, default=DEFAULT_LABELS)
    p.add_argument("--templates", type=Path, default=DEFAULT_TEMPLATES)
    p.add_argument("--out", type=Path, default=DEFAULT_MODEL_OUT)
    p.add_argument("--n", type=int, default=2)
    p.add_argument(
        "--alpha",
        type=float,
        default=0.5,
        help="Lidstone smoothing constant for unseen bigrams.",
    )
    p.add_argument(
        "--max-lines",
        type=int,
        default=None,
        help="cap input lines (useful for debugging on big logs)",
    )
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
    print(f"[1/5] loading labels from {args.labels}")
    labels = _load_labels(args.labels)
    print(f"      {len(labels):,} labeled blocks "
          f"({sum(1 for v in labels.values() if v == 'Anomaly'):,} anomaly, "
          f"{sum(1 for v in labels.values() if v == 'Normal'):,} normal)")

    # --- Step 2: build template matcher.
    print(f"[2/5] loading templates from {args.templates}")
    matcher = TemplateMatcherStage(args.templates)
    print(f"      {matcher.template_count} templates loaded")

    # --- Step 3: stream-parse HDFS.log, group by block_id.
    print(f"[3/5] parsing HDFS log: {args.log}")
    parser = HDFSParser()
    block_events: dict[str, list[str]] = defaultdict(list)
    n_parsed = 0
    n_matched = 0
    n_skipped = 0
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
                n_skipped += 1
                continue
            n_parsed += 1
            block_id = rec.enrichment.get("block_id")
            if not isinstance(block_id, str):
                continue
            event_id = _match_event_id(matcher, rec.message)
            if event_id is None:
                continue
            n_matched += 1
            block_events[block_id].append(event_id)

            if (i + 1) % 1_000_000 == 0:
                rate = (i + 1) / (time.perf_counter() - start)
                print(f"      {i + 1:>10,} lines  ({rate:>10,.0f} lines/s)")

    elapsed = time.perf_counter() - start
    print(
        f"      parsed {n_parsed:,} lines in {elapsed:.1f}s "
        f"(matched {n_matched:,}, skipped {n_skipped:,}, "
        f"{len(block_events):,} distinct blocks)"
    )

    # --- Step 4: count bigrams across labelled-Normal blocks.
    print(f"[4/5] counting {args.n}-grams from labelled-Normal blocks")
    bigram_counts: Counter[tuple[str, ...]] = Counter()
    vocabulary: set[str] = set()
    n_normal_used = 0
    n_unlabeled = 0
    for block_id, events in block_events.items():
        label = labels.get(block_id)
        if label is None:
            n_unlabeled += 1
            continue
        vocabulary.update(events)
        if label == "Normal":
            bigram_counts.update(ngrams(events, args.n))
            n_normal_used += 1
    print(
        f"      {n_normal_used:,} normal blocks contributed "
        f"{sum(bigram_counts.values()):,} total {args.n}-grams "
        f"({len(bigram_counts):,} unique, "
        f"vocab size {len(vocabulary)}, "
        f"{n_unlabeled:,} blocks were unlabeled and ignored)"
    )

    # --- Step 5: persist.
    print(f"[5/5] writing model to {args.out}")
    args.out.parent.mkdir(parents=True, exist_ok=True)
    model = KLSequenceModel(
        n=args.n,
        bigram_counts=dict(bigram_counts),
        vocabulary=frozenset(vocabulary),
        total_bigrams=sum(bigram_counts.values()),
        alpha=args.alpha,
    )
    args.out.write_text(json.dumps(model.to_dict(), indent=2))
    print("      done.")
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


def _match_event_id(matcher: TemplateMatcherStage, message: str) -> str | None:
    """Return the event_id for the first template that matches ``message``."""
    for eid, pat in matcher._patterns:  # type: ignore[attr-defined]
        if pat.match(message):
            return eid
    return None


if __name__ == "__main__":
    sys.exit(main())
