"""Tests for KLDivergenceSequenceDetector and KLSequenceModel."""

from __future__ import annotations

import json
import math
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest

from pipelinex.core.models import BlockTrace
from pipelinex.detectors.sequence_kl import (
    KLDivergenceSequenceDetector,
    KLSequenceModel,
    ngrams,
)


def _trace(events: tuple[str, ...]) -> BlockTrace:
    return BlockTrace(
        block_id="blk_1",
        event_sequence=events,
        record_ids=(uuid4(), uuid4()),
        first_timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        last_timestamp=datetime(2024, 1, 1, 0, 5, tzinfo=UTC),
        record_count=len(events),
        pipeline_run_id=uuid4(),
        closed_reason="terminal",
    )


class TestNgrams:
    def test_2_grams(self) -> None:
        out = list(ngrams(["A", "B", "C", "D"], 2))
        assert out == [("A", "B"), ("B", "C"), ("C", "D")]

    def test_short_sequence_yields_nothing(self) -> None:
        assert list(ngrams(["A"], 2)) == []

    def test_empty_yields_nothing(self) -> None:
        assert list(ngrams([], 3)) == []


class TestModel:
    def test_roundtrip(self) -> None:
        m = KLSequenceModel(
            n=2,
            bigram_counts={("A", "B"): 10, ("B", "C"): 5},
            vocabulary=frozenset({"A", "B", "C"}),
            total_bigrams=15,
            alpha=0.5,
        )
        m2 = KLSequenceModel.from_dict(m.to_dict())
        assert m2.n == m.n
        assert dict(m2.bigram_counts) == dict(m.bigram_counts)
        assert m2.vocabulary == m.vocabulary
        assert m2.total_bigrams == m.total_bigrams
        assert m2.alpha == m.alpha

    def test_load_from_file(self, tmp_path: Path) -> None:
        m = KLSequenceModel(
            n=2,
            bigram_counts={("E22", "E5"): 50, ("E5", "E11"): 50},
            vocabulary=frozenset({"E22", "E5", "E11"}),
            total_bigrams=100,
            alpha=0.5,
        )
        path = tmp_path / "model.json"
        path.write_text(json.dumps(m.to_dict()))
        loaded = KLSequenceModel.load(path)
        assert dict(loaded.bigram_counts) == dict(m.bigram_counts)

    def test_malformed_dict_raises(self) -> None:
        with pytest.raises(ValueError, match="malformed"):
            KLSequenceModel.from_dict({"n": 2, "bigram_counts": "nope"})

    def test_smoothed_prob_is_positive_even_for_unseen(self) -> None:
        m = KLSequenceModel(
            n=2,
            bigram_counts={("A", "B"): 100},
            vocabulary=frozenset({"A", "B"}),
            total_bigrams=100,
            alpha=0.5,
        )
        # Unseen bigram still has nonzero probability thanks to Lidstone smoothing.
        assert m.smoothed_prob(("B", "A")) > 0.0

    def test_smoothed_prob_sums_close_to_one(self) -> None:
        m = KLSequenceModel(
            n=2,
            bigram_counts={("A", "B"): 60, ("B", "A"): 40},
            vocabulary=frozenset({"A", "B"}),
            total_bigrams=100,
            alpha=0.5,
        )
        # V=2 ⇒ V^n=4 possible bigrams; Lidstone should sum to 1.0 across them.
        possible = [(a, b) for a in m.vocabulary for b in m.vocabulary]
        total = sum(m.smoothed_prob(g) for g in possible)
        assert total == pytest.approx(1.0)


class TestDetect:
    async def test_empty_trace_returns_none(self) -> None:
        m = KLSequenceModel(
            n=2,
            bigram_counts={("A", "B"): 10},
            vocabulary=frozenset({"A", "B"}),
            total_bigrams=10,
            alpha=0.5,
        )
        d = KLDivergenceSequenceDetector(model=m, threshold=0.0)
        # Length-1 sequence has no 2-grams.
        assert (await d.detect_trace(_trace(("A",)))) is None

    async def test_identical_distribution_zero_kl(self) -> None:
        # When Q == P exactly, KL(Q||P) = 0. The trace "A,B,A,B,A,B" produces
        # bigrams (A,B),(B,A),(A,B),(B,A),(A,B) ⇒ Q(A,B)=3/5, Q(B,A)=2/5.
        # Build P with the same 3:2 ratio so Q ≡ P and KL = 0.
        m = KLSequenceModel(
            n=2,
            bigram_counts={("A", "B"): 600, ("B", "A"): 400},
            vocabulary=frozenset({"A", "B"}),
            total_bigrams=1000,
            alpha=1e-9,  # near-zero smoothing so smoothed P matches counts.
        )
        d = KLDivergenceSequenceDetector(model=m, threshold=0.01, alpha=1e-9)
        result = await d.detect_trace(_trace(("A", "B", "A", "B", "A", "B")))
        # Q matches P ⇒ KL ≈ 0 ⇒ does not fire (or fires with ε severity).
        assert result is None or result.severity_score < 0.01

    async def test_divergent_distribution_fires(self) -> None:
        # P heavily favors (A,B); Q favors (B,A) — different distribution
        # but using the same vocabulary, so this exercises *distributional*
        # divergence, the whole reason KL replaces set-membership.
        m = KLSequenceModel(
            n=2,
            bigram_counts={("A", "B"): 990, ("B", "A"): 10},
            vocabulary=frozenset({"A", "B"}),
            total_bigrams=1000,
            alpha=0.5,
        )
        d = KLDivergenceSequenceDetector(model=m, threshold=0.5, alpha=0.5)
        # Trace strongly favors (B,A).
        result = await d.detect_trace(_trace(("B", "A", "B", "A", "B", "A", "B", "A")))
        assert result is not None
        assert result.detector_name == "sequence_kl"
        assert result.severity_score > 0.5
        assert result.metadata["block_id"] == "blk_1"
        assert result.metadata["n"] == 2

    async def test_smoothing_prevents_infinity(self) -> None:
        # Model has never seen the bigram ("X", "Y").
        m = KLSequenceModel(
            n=2,
            bigram_counts={("A", "B"): 100},
            vocabulary=frozenset({"A", "B", "X", "Y"}),
            total_bigrams=100,
            alpha=0.5,
        )
        d = KLDivergenceSequenceDetector(model=m, threshold=0.0, alpha=0.5)
        # Trace contains only never-seen bigrams.
        result = await d.detect_trace(_trace(("X", "Y", "X", "Y")))
        assert result is not None
        assert math.isfinite(result.severity_score)

    async def test_severity_score_equals_kl(self) -> None:
        # Trace ("B","A","B","A") produces 3 bigrams: (B,A),(A,B),(B,A)
        # ⇒ Q(B,A)=2/3, Q(A,B)=1/3. KL = Σ Q · log(Q/P).
        # In ``score_mode='kl'`` the severity is the full KL value.
        m = KLSequenceModel(
            n=2,
            bigram_counts={("A", "B"): 800, ("B", "A"): 200},
            vocabulary=frozenset({"A", "B"}),
            total_bigrams=1000,
            alpha=0.5,
        )
        d = KLDivergenceSequenceDetector(
            model=m, threshold=0.0, alpha=0.5, score_mode="kl"
        )
        result = await d.detect_trace(_trace(("B", "A", "B", "A")))
        assert result is not None
        p_ab = m.smoothed_prob(("A", "B"))
        p_ba = m.smoothed_prob(("B", "A"))
        q_ab = 1 / 3
        q_ba = 2 / 3
        expected_kl = q_ab * math.log(q_ab / p_ab) + q_ba * math.log(q_ba / p_ba)
        assert result.severity_score == pytest.approx(expected_kl)
        assert result.metadata["kl_divergence"] == pytest.approx(expected_kl)

    async def test_severity_score_equals_max_contrib(self) -> None:
        # Same setup. In ``score_mode='max_contrib'`` the severity is the
        # largest per-bigram contribution to KL.
        m = KLSequenceModel(
            n=2,
            bigram_counts={("A", "B"): 800, ("B", "A"): 200},
            vocabulary=frozenset({"A", "B"}),
            total_bigrams=1000,
            alpha=0.5,
        )
        d = KLDivergenceSequenceDetector(
            model=m, threshold=0.0, alpha=0.5, score_mode="max_contrib"
        )
        result = await d.detect_trace(_trace(("B", "A", "B", "A")))
        assert result is not None
        # Q(B,A) = 2/3 dominates: contribution = (2/3) · log((2/3) / P(B,A)).
        p_ba = m.smoothed_prob(("B", "A"))
        expected_max = (2 / 3) * math.log((2 / 3) / p_ba)
        assert result.severity_score == pytest.approx(expected_max)
        assert result.metadata["max_contribution"] == pytest.approx(expected_max)

    async def test_threshold_suppresses_low_divergence(self) -> None:
        # P spreads mass across all 4 bigrams; Q does too in a similar
        # proportion ⇒ KL is small ⇒ threshold suppresses firing.
        m = KLSequenceModel(
            n=2,
            bigram_counts={
                ("A", "B"): 300,
                ("B", "A"): 300,
                ("A", "A"): 200,
                ("B", "B"): 200,
            },
            vocabulary=frozenset({"A", "B"}),
            total_bigrams=1000,
            alpha=0.5,
        )
        d = KLDivergenceSequenceDetector(model=m, threshold=1.0, alpha=0.5)
        # Trace produces bigrams (A,B),(B,B),(B,A),(A,A),(A,B),(B,B),(B,A):
        # Q(A,B)=2/7, Q(B,B)=2/7, Q(B,A)=2/7, Q(A,A)=1/7 ≈ same shape as P.
        result = await d.detect_trace(_trace(("A", "B", "B", "A", "A", "B", "B", "A")))
        assert result is None

    async def test_min_trace_len_below_floor(self) -> None:
        m = KLSequenceModel(
            n=2,
            bigram_counts={("A", "B"): 10},
            vocabulary=frozenset({"A", "B"}),
            total_bigrams=10,
            alpha=0.5,
        )
        d = KLDivergenceSequenceDetector(
            model=m, threshold=0.0, alpha=0.5, min_trace_len=5
        )
        # Sequence yields only 2 bigrams (< min_trace_len=5).
        result = await d.detect_trace(_trace(("A", "B", "A")))
        assert result is None


class TestConstruction:
    def test_rejects_negative_threshold(self) -> None:
        with pytest.raises(ValueError, match="threshold"):
            KLDivergenceSequenceDetector(threshold=-0.1)

    def test_rejects_nonpositive_alpha(self) -> None:
        with pytest.raises(ValueError, match="alpha"):
            KLDivergenceSequenceDetector(alpha=0.0)

    def test_rejects_zero_min_trace_len(self) -> None:
        with pytest.raises(ValueError, match="min_trace_len"):
            KLDivergenceSequenceDetector(min_trace_len=0)

    def test_rejects_invalid_score_mode(self) -> None:
        with pytest.raises(ValueError, match="score_mode"):
            KLDivergenceSequenceDetector(score_mode="bogus")
