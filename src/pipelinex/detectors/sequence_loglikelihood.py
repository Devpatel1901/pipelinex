"""SequenceLogLikelihoodDetector — statistical n-gram detection for HDFS.

Algorithm
---------
A *normal* block trace is one whose event transitions are likely under the
distribution learned from labeled-Normal blocks. For each closed
``BlockTrace`` we compute the mean log-probability of its bigrams under a
Laplace-smoothed transition model::

    P(b | a) = (count(a, b) + alpha) / (count(a) + alpha * V)

where ``V = |vocabulary|`` and ``alpha`` is a small smoothing constant
(default 0.1). The trace fires when ``mean_log_prob < threshold`` (the
threshold is a negative scalar; lower = more anomalous).

Why this beats set-membership novelty
-------------------------------------
The original ``SequenceAnomalyDetector`` fires only when a 2-gram is
*literally absent* from the trained normal set, so it cannot flag
anomalies that reuse normal bigrams in unusual positions or frequencies.
Likelihood scoring weights bigrams by *how rare* they are under the
normal distribution, so rare-but-seen transitions still drag the score
down. The same v1 corpus that gives the original detector P=1.0, R=0.285
gives this detector a real precision/recall trade-off that can be tuned
along a single threshold.

Why not PCA / scikit-learn
--------------------------
Bigram likelihood is a ``dict[str, dict[str, int]]`` with a closed-form
score — ~150 LOC, no ML deps. PCA would force pickle persistence and a
much heavier defence in interview. The trade-off justification mirrors
the one already documented in ``docs/design-patterns.md`` for the
n-gram-vs-PCA decision.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Final

from pipelinex.core.interfaces import ISequenceAnomalyDetector
from pipelinex.core.models import AnomalyEvent, BlockTrace

DEFAULT_THRESHOLD: Final = -5.0  # mean log-prob below this fires
DEFAULT_ALPHA: Final = 0.1


@dataclass(frozen=True)
class SequenceStatsModel:
    """Persisted bigram-counts model for the likelihood detector."""

    n: int
    vocabulary: frozenset[str]
    # transitions[a][b] = count of times event a was followed by event b in
    # labeled-Normal training blocks.
    transitions: dict[str, dict[str, int]]
    # unigram_counts[a] = sum over b of transitions[a][b]; pre-computed so
    # detection is O(trace_length) and not O(trace_length * |vocab|).
    unigram_counts: dict[str, int]

    @classmethod
    def empty(cls, n: int = 2) -> SequenceStatsModel:
        return cls(n=n, vocabulary=frozenset(), transitions={}, unigram_counts={})

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> SequenceStatsModel:
        raw_n = data.get("n", 2)
        if not isinstance(raw_n, int):
            raise ValueError("malformed stats model: n must be an int")
        vocab_field = data.get("vocabulary", [])
        if not isinstance(vocab_field, list):
            raise ValueError("malformed stats model: vocabulary must be a list")
        trans_field = data.get("transitions", {})
        unigram_field = data.get("unigram_counts", {})
        if not isinstance(trans_field, dict) or not isinstance(unigram_field, dict):
            raise ValueError("malformed stats model: transitions/unigram_counts must be dicts")
        transitions = {
            str(a): {str(b): int(c) for b, c in inner.items()}
            for a, inner in trans_field.items()
            if isinstance(inner, dict)
        }
        unigram_counts = {str(a): int(c) for a, c in unigram_field.items()}
        return cls(
            n=raw_n,
            vocabulary=frozenset(str(v) for v in vocab_field),
            transitions=transitions,
            unigram_counts=unigram_counts,
        )

    @classmethod
    def load(cls, path: str | Path) -> SequenceStatsModel:
        with Path(path).open(encoding="utf-8") as fh:
            data = json.load(fh)
        return cls.from_dict(data)

    def to_dict(self) -> dict[str, object]:
        return {
            "n": self.n,
            "vocabulary": sorted(self.vocabulary),
            "transitions": {a: dict(inner) for a, inner in self.transitions.items()},
            "unigram_counts": dict(self.unigram_counts),
        }


class SequenceLogLikelihoodDetector(ISequenceAnomalyDetector):
    """Score a closed BlockTrace by mean log-probability under a bigram model."""

    def __init__(
        self,
        model: SequenceStatsModel | None = None,
        threshold: float = DEFAULT_THRESHOLD,
        alpha: float = DEFAULT_ALPHA,
    ) -> None:
        if alpha <= 0:
            raise ValueError("alpha must be > 0")
        self._model = model or SequenceStatsModel.empty()
        self._threshold = threshold
        self._alpha = alpha

    @property
    def name(self) -> str:
        return "sequence_loglikelihood"

    def load_model(self, path: str | Path) -> None:
        self._model = SequenceStatsModel.load(path)

    def set_model(self, model: SequenceStatsModel) -> None:
        self._model = model

    @property
    def model(self) -> SequenceStatsModel:
        return self._model

    async def detect_trace(self, trace: BlockTrace) -> AnomalyEvent | None:
        events = trace.event_sequence
        # Need at least 2 events for any bigram.
        if len(events) < 2:
            return None

        vocab_size = max(1, len(self._model.vocabulary))
        alpha = self._alpha
        log_probs: list[float] = []
        worst_logp = math.inf
        worst_pair: tuple[str, str] | None = None

        for i in range(len(events) - 1):
            a, b = events[i], events[i + 1]
            row = self._model.transitions.get(a, {})
            num = row.get(b, 0) + alpha
            denom = self._model.unigram_counts.get(a, 0) + alpha * vocab_size
            # denom is always >= alpha * vocab_size > 0 (vocab_size >= 1),
            # so this is safe.
            logp = math.log(num / denom)
            log_probs.append(logp)
            if logp < worst_logp:
                worst_logp = logp
                worst_pair = (a, b)

        if not log_probs:
            return None

        mean_logp = sum(log_probs) / len(log_probs)
        if mean_logp >= self._threshold:
            return None

        anchor = trace.record_ids[0] if trace.record_ids else AnomalyEvent().log_record_id
        return AnomalyEvent(
            log_record_id=anchor,
            detector_name=self.name,
            # Higher = more anomalous, consistent with other detectors.
            severity_score=-mean_logp,
            metadata={
                "block_id": trace.block_id,
                "mean_log_prob": mean_logp,
                "trace_length": len(events),
                "min_transition_logprob": worst_logp,
                "min_transition": list(worst_pair) if worst_pair else None,
                "threshold": self._threshold,
                "alpha": alpha,
                "closed_reason": trace.closed_reason,
            },
        )
