"""SequenceAnomalyDetector — n-gram detection for HDFS block traces.

Algorithm
---------
A *normal* block trace produces a small set of expected event-id n-grams
(default n=2). For each closed ``BlockTrace`` we extract its observed
n-grams and compute::

    anomaly_ratio = |observed - normal| / |observed|

The trace is flagged when ``anomaly_ratio > threshold`` (default 0.0,
i.e. *any* novel bigram fires). The detector is trained offline by
``scripts/train_sequence_model.py`` against ``anomaly_label.csv``.

Why n-gram and not PCA?
-----------------------
- N-gram is a ``Counter[tuple]`` model — ~150 LOC, no scikit-learn.
- Published HDFS results show n-gram baselines reach F1 ~0.95 on this
  corpus, comparable to PCA.
- PCA would force scikit-learn + careful fit/transform separation +
  pickle persistence. v2 alternative documented in
  ``docs/design-patterns.md``.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Final

from pipelinex.core.interfaces import ISequenceAnomalyDetector
from pipelinex.core.models import AnomalyEvent, BlockTrace

logger = logging.getLogger(__name__)

DEFAULT_N: Final = 2
DEFAULT_THRESHOLD: Final = 0.0  # any novel n-gram fires


@dataclass(frozen=True)
class SequenceModel:
    """Persisted n-gram model loaded by SequenceAnomalyDetector."""

    n: int
    normal_ngrams: frozenset[tuple[str, ...]]
    vocabulary: frozenset[str]

    @classmethod
    def empty(cls, n: int = DEFAULT_N) -> SequenceModel:
        return cls(n=n, normal_ngrams=frozenset(), vocabulary=frozenset())

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> SequenceModel:
        raw_n = data.get("n", DEFAULT_N)
        if not isinstance(raw_n, int):
            raise ValueError("malformed sequence model: n must be an int")
        ngrams_field = data.get("normal_ngrams", [])
        vocab_field = data.get("vocabulary", [])
        if not isinstance(ngrams_field, list) or not isinstance(vocab_field, list):
            raise ValueError("malformed sequence model: ngrams/vocab must be lists")
        normal = frozenset(tuple(str(t) for t in g) for g in ngrams_field)
        return cls(n=raw_n, normal_ngrams=normal, vocabulary=frozenset(str(v) for v in vocab_field))

    @classmethod
    def load(cls, path: str | Path) -> SequenceModel:
        with Path(path).open(encoding="utf-8") as fh:
            data = json.load(fh)
        return cls.from_dict(data)

    def to_dict(self) -> dict[str, object]:
        return {
            "n": self.n,
            "normal_ngrams": [list(g) for g in self.normal_ngrams],
            "vocabulary": sorted(self.vocabulary),
        }


def ngrams(events: Iterable[str], n: int) -> Iterator[tuple[str, ...]]:
    """Yield n-grams from an event sequence. Sequences shorter than n yield nothing."""
    seq = list(events)
    if n <= 0 or len(seq) < n:
        return
    for i in range(len(seq) - n + 1):
        yield tuple(seq[i : i + n])


class SequenceAnomalyDetector(ISequenceAnomalyDetector):
    """Score a closed BlockTrace by the share of novel n-grams it contains."""

    def __init__(
        self,
        model: SequenceModel | None = None,
        threshold: float = DEFAULT_THRESHOLD,
    ) -> None:
        if threshold < 0:
            raise ValueError("threshold must be >= 0")
        self._model = model or SequenceModel.empty()
        self._threshold = threshold

    @property
    def name(self) -> str:
        return "sequence_ngram"

    def load_model(self, path: str | Path) -> None:
        self._model = SequenceModel.load(path)

    def set_model(self, model: SequenceModel) -> None:
        self._model = model

    @property
    def model(self) -> SequenceModel:
        return self._model

    async def detect_trace(self, trace: BlockTrace) -> AnomalyEvent | None:
        n = self._model.n
        observed = list(ngrams(trace.event_sequence, n))
        if not observed:
            return None

        observed_set = set(observed)
        novel = observed_set - self._model.normal_ngrams
        ratio = len(novel) / len(observed_set)

        if ratio <= self._threshold:
            return None

        # Use the first record id of the trace as the anchor — keeps existing
        # alert listeners (which expect a log_record_id) functional.
        anchor = trace.record_ids[0] if trace.record_ids else AnomalyEvent().log_record_id

        return AnomalyEvent(
            log_record_id=anchor,
            detector_name=self.name,
            severity_score=ratio,
            metadata={
                "block_id": trace.block_id,
                "n": n,
                "novel_ngrams": [list(g) for g in sorted(novel)][:20],
                "observed_count": len(observed_set),
                "novel_count": len(novel),
                "event_sequence_length": len(trace.event_sequence),
                "closed_reason": trace.closed_reason,
            },
        )
