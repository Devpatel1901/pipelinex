"""Shared helpers for evaluation scripts.

Production code under ``src/`` does NOT depend on this module — it lives
in ``scripts/`` so that pipeline correctness never relies on evaluation
machinery.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ConfusionMatrix:
    tp: int
    fp: int
    tn: int
    fn: int

    @property
    def precision(self) -> float:
        denom = self.tp + self.fp
        return self.tp / denom if denom else 0.0

    @property
    def recall(self) -> float:
        denom = self.tp + self.fn
        return self.tp / denom if denom else 0.0

    @property
    def f1(self) -> float:
        p, r = self.precision, self.recall
        return 2 * p * r / (p + r) if (p + r) else 0.0

    def as_dict(self) -> dict[str, float | int]:
        return {
            "tp": self.tp,
            "fp": self.fp,
            "tn": self.tn,
            "fn": self.fn,
            "precision": round(self.precision, 4),
            "recall": round(self.recall, 4),
            "f1": round(self.f1, 4),
        }


def confusion(predicted: set[str], actual: set[str], universe: set[str]) -> ConfusionMatrix:
    """Build a confusion matrix over a finite labeled universe.

    universe = the set of all entities being classified (e.g. all block ids).
    actual   = subset of universe known to be anomalous.
    predicted = subset of universe flagged by the detector.
    """
    tp = len(predicted & actual)
    fp = len(predicted - actual)
    fn = len(actual - predicted)
    tn = len(universe - predicted - actual)
    return ConfusionMatrix(tp=tp, fp=fp, tn=tn, fn=fn)


def format_table(rows: list[dict[str, object]], headers: list[str]) -> str:
    """Render a markdown table from rows of dicts."""
    out = ["| " + " | ".join(headers) + " |"]
    out.append("|" + "|".join(["---"] * len(headers)) + "|")
    for row in rows:
        cells = [str(row.get(h, "")) for h in headers]
        out.append("| " + " | ".join(cells) + " |")
    return "\n".join(out)
