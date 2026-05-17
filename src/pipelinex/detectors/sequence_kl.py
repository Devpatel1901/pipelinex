"""KL-divergence sequence anomaly detector for HDFS block traces.

Algorithm
---------
Each closed ``BlockTrace`` is scored against a reference distribution ``P``
of normal bigram frequencies learned offline from labelled-Normal blocks.
The trace produces its own bigram distribution ``Q`` and we compute the
Kullback-Leibler divergence::

    KL(Q || P) = sum_g  Q(g) * log( Q(g) / P(g) )

``P`` is Lidstone-smoothed over the full bigram space ``V x V`` so unseen
bigrams have nonzero probability and ``log(Q/P)`` stays finite::

    P(g) = (count(g) + alpha) / (total_bigrams + alpha * V**n)

The trace fires when ``KL(Q || P) > threshold``. The natural log puts the
result in nats; thresholds in BENCHMARKS.md are reported in the same unit.

Why this replaces the v1 set-membership detector
-----------------------------------------------
v1 fired only on bigrams that never appeared in any normal trace. 71% of
true HDFS anomalies use only "normal" bigrams in different proportions:
truncated traces, reordered events, missing terminal bigrams. Set
membership cannot capture *distributional* differences; KL-divergence
does. The interface (``ISequenceAnomalyDetector``) is unchanged.
"""

from __future__ import annotations

import json
import logging
import math
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Final

from pipelinex.core.interfaces import ISequenceAnomalyDetector
from pipelinex.core.models import AnomalyEvent, BlockTrace

logger = logging.getLogger(__name__)

DEFAULT_N: Final = 2
DEFAULT_THRESHOLD: Final = 0.3  # nats; tuned via sweep (see BENCHMARKS.md)
DEFAULT_ALPHA: Final = 0.5  # Lidstone smoothing
DEFAULT_MIN_TRACE_LEN: Final = 2  # minimum number of events for KL to be defined
DEFAULT_SCORE_MODE: Final = "max_contrib"  # "kl" or "max_contrib"


def ngrams(events: Iterable[str], n: int) -> Iterator[tuple[str, ...]]:
    """Yield consecutive n-grams. Sequences shorter than n yield nothing."""
    seq = list(events)
    if n <= 0 or len(seq) < n:
        return
    for i in range(len(seq) - n + 1):
        yield tuple(seq[i : i + n])


@dataclass(frozen=True)
class KLSequenceModel:
    """Reference bigram distribution for KL-divergence scoring."""

    n: int
    bigram_counts: Mapping[tuple[str, ...], int]
    vocabulary: frozenset[str]
    total_bigrams: int
    alpha: float = DEFAULT_ALPHA

    # Cached denominator: total + alpha * V^n. Computed in __post_init__.
    smoothed_denom: float = field(default=0.0, compare=False)

    def __post_init__(self) -> None:
        v = len(self.vocabulary)
        denom = float(self.total_bigrams) + self.alpha * (v**self.n)
        # frozen=True: bypass __setattr__ to cache the denominator.
        object.__setattr__(self, "smoothed_denom", denom)

    @classmethod
    def empty(cls, n: int = DEFAULT_N, alpha: float = DEFAULT_ALPHA) -> KLSequenceModel:
        return cls(
            n=n,
            bigram_counts={},
            vocabulary=frozenset(),
            total_bigrams=0,
            alpha=alpha,
        )

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> KLSequenceModel:
        raw_n = data.get("n", DEFAULT_N)
        if not isinstance(raw_n, int):
            raise ValueError("malformed KL model: n must be an int")
        counts_field = data.get("bigram_counts", [])
        vocab_field = data.get("vocabulary", [])
        if not isinstance(counts_field, list) or not isinstance(vocab_field, list):
            raise ValueError("malformed KL model: bigram_counts/vocabulary must be lists")
        counts: dict[tuple[str, ...], int] = {}
        for entry in counts_field:
            if not isinstance(entry, list) or len(entry) != 2:
                raise ValueError("malformed KL model: each bigram entry must be [tokens, count]")
            tokens, c = entry
            if not isinstance(tokens, list) or not isinstance(c, int):
                raise ValueError("malformed KL model: bigram entry types")
            counts[tuple(str(t) for t in tokens)] = c
        total = sum(counts.values())
        alpha_field = data.get("alpha", DEFAULT_ALPHA)
        alpha = float(alpha_field) if isinstance(alpha_field, (int, float)) else DEFAULT_ALPHA
        return cls(
            n=raw_n,
            bigram_counts=counts,
            vocabulary=frozenset(str(v) for v in vocab_field),
            total_bigrams=total,
            alpha=alpha,
        )

    @classmethod
    def load(cls, path: str | Path) -> KLSequenceModel:
        with Path(path).open(encoding="utf-8") as fh:
            data = json.load(fh)
        return cls.from_dict(data)

    def to_dict(self) -> dict[str, object]:
        return {
            "n": self.n,
            "alpha": self.alpha,
            "vocabulary": sorted(self.vocabulary),
            "bigram_counts": [
                [list(g), c] for g, c in sorted(self.bigram_counts.items())
            ],
            "total_bigrams": self.total_bigrams,
        }

    def smoothed_prob(self, gram: tuple[str, ...]) -> float:
        """P(gram) under Lidstone smoothing. Always positive."""
        count = self.bigram_counts.get(gram, 0)
        return (count + self.alpha) / self.smoothed_denom


class KLDivergenceSequenceDetector(ISequenceAnomalyDetector):
    """Score a closed BlockTrace by KL(Q_trace || P_normal) in nats.

    Fires when the divergence exceeds ``threshold``. The score scales with
    how unusual the trace's bigram *distribution* is — not just whether
    unseen bigrams appear — so it captures distributional anomalies that
    set-membership detection misses.
    """

    def __init__(
        self,
        model: KLSequenceModel | None = None,
        threshold: float = DEFAULT_THRESHOLD,
        alpha: float = DEFAULT_ALPHA,
        min_trace_len: int = DEFAULT_MIN_TRACE_LEN,
        score_mode: str = DEFAULT_SCORE_MODE,
    ) -> None:
        if threshold < 0:
            raise ValueError("threshold must be >= 0")
        if alpha <= 0:
            raise ValueError("alpha must be > 0 (Lidstone smoothing)")
        if min_trace_len < 1:
            raise ValueError("min_trace_len must be >= 1")
        if score_mode not in {"kl", "max_contrib"}:
            raise ValueError("score_mode must be 'kl' or 'max_contrib'")
        self._model = model or KLSequenceModel.empty(alpha=alpha)
        self._threshold = threshold
        self._min_trace_len = min_trace_len
        self._score_mode = score_mode

    @property
    def name(self) -> str:
        return "sequence_kl"

    @property
    def model(self) -> KLSequenceModel:
        return self._model

    def load_model(self, path: str | Path) -> None:
        self._model = KLSequenceModel.load(path)

    def set_model(self, model: KLSequenceModel) -> None:
        self._model = model

    async def detect_trace(self, trace: BlockTrace) -> AnomalyEvent | None:
        n = self._model.n
        observed = list(ngrams(trace.event_sequence, n))
        if len(observed) < self._min_trace_len:
            return None

        # Build Q = trace bigram distribution.
        q_counts: dict[tuple[str, ...], int] = {}
        for g in observed:
            q_counts[g] = q_counts.get(g, 0) + 1
        total_q = len(observed)

        # Per-gram KL contribution and total. Each contribution is
        #     Q(g) · log( Q(g) / P(g) )
        # which can be negative when Q(g) < P(g). The total is KL(Q || P).
        contribs: list[tuple[tuple[str, ...], float]] = []
        kl = 0.0
        for g, c in q_counts.items():
            q_prob = c / total_q
            p_prob = self._model.smoothed_prob(g)
            contrib = q_prob * math.log(q_prob / p_prob)
            contribs.append((g, contrib))
            kl += contrib

        # ``max_contrib`` is more robust than KL for short traces: it asks
        # "is there *one* surprising bigram?" rather than "is the
        # distribution as a whole different?". Diffuse small deviations
        # across many bigrams don't trigger it, which lifts precision on
        # normal-but-noisy traces without giving up the recall KL provides.
        max_contrib = max((c for _, c in contribs), default=0.0)

        score = kl if self._score_mode == "kl" else max_contrib
        if score <= self._threshold:
            return None

        anchor = trace.record_ids[0] if trace.record_ids else AnomalyEvent().log_record_id
        top_contribs = sorted(contribs, key=lambda pair: pair[1], reverse=True)[:10]

        return AnomalyEvent(
            log_record_id=anchor,
            detector_name=self.name,
            severity_score=score,
            metadata={
                "block_id": trace.block_id,
                "n": n,
                "score_mode": self._score_mode,
                "kl_divergence": kl,
                "max_contribution": max_contrib,
                "top_contributors": [
                    {"ngram": list(g), "contribution": round(c, 6)}
                    for g, c in top_contribs
                ],
                "observed_bigrams": total_q,
                "unique_bigrams": len(q_counts),
                "event_sequence_length": len(trace.event_sequence),
                "closed_reason": trace.closed_reason,
                "alpha": self._model.alpha,
            },
        )
