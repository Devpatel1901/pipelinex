"""Shared HDFS log preparation used by the trainers and the HDFS evaluator.

Production code under ``src/`` does NOT depend on this module — it lives
in ``scripts/`` so that pipeline correctness never relies on evaluation
machinery (same pattern as ``_eval_common.py``).

The single helper ``parse_hdfs_blocks`` streams ``HDFS.log`` line-by-line,
parses each line with ``HDFSParser``, assigns an ``event_id`` from the 30
preloaded templates, and groups events by ``block_id``.
"""

from __future__ import annotations

import csv
import sys
import time
from collections import defaultdict
from pathlib import Path

from pipelinex.stages.enrichment.template_matcher import TemplateMatcherStage
from pipelinex.stages.parsing.hdfs_parser import HDFSParser


def parse_hdfs_blocks(
    log_path: Path,
    templates_path: Path,
    max_lines: int | None = None,
    progress_every: int = 1_000_000,
) -> tuple[dict[str, list[str]], dict[str, int]]:
    """Parse ``log_path`` and return (block_events, stats).

    ``block_events[block_id]`` is the list of event_ids observed for that
    block, in arrival order. ``stats`` reports n_parsed, n_matched,
    n_skipped, elapsed_s and is intended for log lines, not assertions.
    """
    parser = HDFSParser()
    matcher = TemplateMatcherStage(templates_path)
    block_events: dict[str, list[str]] = defaultdict(list)
    n_parsed = 0
    n_matched = 0
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
            n_parsed += 1
            block_id = rec.enrichment.get("block_id")
            if not isinstance(block_id, str):
                continue
            event_id = _match_event_id(matcher, rec.message)
            if event_id is None:
                continue
            n_matched += 1
            block_events[block_id].append(event_id)

            if (i + 1) % progress_every == 0:
                rate = (i + 1) / (time.perf_counter() - start)
                print(
                    f"      {i + 1:>10,} lines  ({rate:>10,.0f} lines/s)",
                    file=sys.stderr,
                )

    elapsed = time.perf_counter() - start
    stats = {
        "n_parsed": n_parsed,
        "n_matched": n_matched,
        "n_skipped": n_skipped,
        "elapsed_s": int(elapsed),
        "n_blocks": len(block_events),
    }
    return dict(block_events), stats


def _match_event_id(matcher: TemplateMatcherStage, message: str) -> str | None:
    """Return the event_id for the first template that matches ``message``."""
    for eid, pat in matcher._patterns:  # type: ignore[attr-defined]
        if pat.match(message):
            return eid
    return None


def load_labels(path: Path) -> dict[str, str]:
    """Load ``anomaly_label.csv`` into ``{block_id: 'Normal'|'Anomaly'}``."""
    out: dict[str, str] = {}
    with path.open(encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            block_id = row.get("BlockId")
            label = row.get("Label")
            if block_id and label:
                out[block_id] = label
    return out
