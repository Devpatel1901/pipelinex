"""Tests for SequenceLogLikelihoodDetector and its model."""

from __future__ import annotations

import json
import math
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest

from pipelinex.core.models import BlockTrace
from pipelinex.detectors.sequence_loglikelihood import (
    SequenceLogLikelihoodDetector,
    SequenceStatsModel,
)


def _trace(events: tuple[str, ...]) -> BlockTrace:
    return BlockTrace(
        block_id="blk_1",
        event_sequence=events,
        record_ids=(uuid4(),),
        first_timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        last_timestamp=datetime(2024, 1, 1, 0, 5, tzinfo=UTC),
        record_count=len(events),
        pipeline_run_id=uuid4(),
        closed_reason="terminal",
    )


def _tiny_model() -> SequenceStatsModel:
    """Trained on 100 'normal' traces of A→B→A→B."""
    # Counts: A→B = 200, B→A = 100; unigrams: A=200, B=100.
    return SequenceStatsModel(
        n=2,
        vocabulary=frozenset({"A", "B", "X"}),
        transitions={"A": {"B": 200}, "B": {"A": 100}},
        unigram_counts={"A": 200, "B": 100},
    )


class TestModel:
    def test_round_trips_through_dict(self) -> None:
        m = _tiny_model()
        d = m.to_dict()
        m2 = SequenceStatsModel.from_dict(d)
        assert m2.n == m.n
        assert m2.vocabulary == m.vocabulary
        assert m2.transitions == m.transitions
        assert m2.unigram_counts == m.unigram_counts

    def test_load_from_file(self, tmp_path: Path) -> None:
        path = tmp_path / "stats.json"
        path.write_text(json.dumps(_tiny_model().to_dict()))
        loaded = SequenceStatsModel.load(path)
        assert loaded.transitions == _tiny_model().transitions

    def test_malformed_dict_raises(self) -> None:
        with pytest.raises(ValueError, match="malformed"):
            SequenceStatsModel.from_dict({"n": 2, "vocabulary": "nope"})

    def test_empty_is_valid(self) -> None:
        m = SequenceStatsModel.empty()
        assert m.n == 2
        assert len(m.vocabulary) == 0


class TestConstruction:
    def test_rejects_zero_alpha(self) -> None:
        with pytest.raises(ValueError, match="alpha"):
            SequenceLogLikelihoodDetector(alpha=0.0)

    def test_rejects_negative_alpha(self) -> None:
        with pytest.raises(ValueError, match="alpha"):
            SequenceLogLikelihoodDetector(alpha=-0.5)

    def test_default_threshold(self) -> None:
        # Sanity: defaults don't crash.
        d = SequenceLogLikelihoodDetector(model=_tiny_model())
        assert d.name == "sequence_loglikelihood"


class TestDetect:
    async def test_short_trace_returns_none(self) -> None:
        d = SequenceLogLikelihoodDetector(model=_tiny_model(), threshold=0.0)
        assert (await d.detect_trace(_trace(("A",)))) is None
        assert (await d.detect_trace(_trace(()))) is None

    async def test_normal_trace_high_likelihood_no_fire(self) -> None:
        # Trace uses only frequent bigrams. With threshold deeply negative,
        # the mean log-prob will be above it, so nothing fires.
        d = SequenceLogLikelihoodDetector(
            model=_tiny_model(), threshold=-100.0, alpha=0.1
        )
        out = await d.detect_trace(_trace(("A", "B", "A", "B")))
        assert out is None

    async def test_anomalous_trace_low_likelihood_fires(self) -> None:
        # Trace contains X→A and A→X, both bigrams whose count is 0 in the
        # model. With smoothing the score is very low → should fire even at
        # a generous threshold.
        d = SequenceLogLikelihoodDetector(
            model=_tiny_model(), threshold=-2.0, alpha=0.1
        )
        out = await d.detect_trace(_trace(("X", "A", "X")))
        assert out is not None
        assert out.detector_name == "sequence_loglikelihood"
        assert out.metadata["block_id"] == "blk_1"
        assert out.metadata["trace_length"] == 3
        assert out.metadata["min_transition"] in (["X", "A"], ["A", "X"])
        assert out.severity_score > 0  # = -mean_log_prob, mean_log_prob < 0

    async def test_severity_ordering(self) -> None:
        # The trace with more rare transitions should score worse.
        model = _tiny_model()
        d = SequenceLogLikelihoodDetector(
            model=model, threshold=-100.0, alpha=0.1
        )
        # Compute scores directly via metadata; both traces fire only at very
        # generous thresholds — use a high threshold so even normal traces fire.
        d2 = SequenceLogLikelihoodDetector(
            model=model, threshold=100.0, alpha=0.1
        )
        normal_ev = await d2.detect_trace(_trace(("A", "B", "A", "B")))
        weird_ev = await d2.detect_trace(_trace(("X", "X", "X", "X")))
        assert normal_ev is not None and weird_ev is not None
        assert weird_ev.metadata["mean_log_prob"] < normal_ev.metadata["mean_log_prob"]
        assert weird_ev.severity_score > normal_ev.severity_score

    async def test_threshold_gates_fire(self) -> None:
        model = _tiny_model()
        # Score one trace, then assert fire/no-fire across thresholds.
        d_loose = SequenceLogLikelihoodDetector(
            model=model, threshold=100.0, alpha=0.1
        )
        ev = await d_loose.detect_trace(_trace(("A", "B", "A")))
        assert ev is not None
        score = ev.metadata["mean_log_prob"]
        d_tight = SequenceLogLikelihoodDetector(
            model=model, threshold=score - 1.0, alpha=0.1
        )
        # threshold below score → no fire
        assert (await d_tight.detect_trace(_trace(("A", "B", "A")))) is None

    async def test_metadata_keys_present(self) -> None:
        d = SequenceLogLikelihoodDetector(
            model=_tiny_model(), threshold=100.0, alpha=0.1
        )
        ev = await d.detect_trace(_trace(("A", "B", "A", "B")))
        assert ev is not None
        for k in (
            "block_id",
            "mean_log_prob",
            "trace_length",
            "min_transition_logprob",
            "min_transition",
            "threshold",
            "alpha",
            "closed_reason",
        ):
            assert k in ev.metadata

    async def test_mean_log_prob_within_range(self) -> None:
        # All probabilities are in (0, 1] so log-probs are in (-inf, 0].
        d = SequenceLogLikelihoodDetector(
            model=_tiny_model(), threshold=100.0, alpha=0.1
        )
        ev = await d.detect_trace(_trace(("A", "B", "A", "B", "A")))
        assert ev is not None
        assert ev.metadata["mean_log_prob"] <= 0
        assert math.isfinite(ev.metadata["mean_log_prob"])
