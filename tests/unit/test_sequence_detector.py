"""Tests for SequenceAnomalyDetector and the persistence shape of the model."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest

from pipelinex.core.models import BlockTrace
from pipelinex.detectors.sequence import (
    SequenceAnomalyDetector,
    SequenceModel,
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
    def test_round_trips_through_dict(self) -> None:
        m = SequenceModel(
            n=2,
            normal_ngrams=frozenset({("A", "B"), ("B", "C")}),
            vocabulary=frozenset({"A", "B", "C"}),
        )
        d = m.to_dict()
        m2 = SequenceModel.from_dict(d)
        assert m2.n == 2
        assert m2.normal_ngrams == m.normal_ngrams
        assert m2.vocabulary == m.vocabulary

    def test_load_from_file(self, tmp_path: Path) -> None:
        m = SequenceModel(
            n=2,
            normal_ngrams=frozenset({("E22", "E5"), ("E5", "E11"), ("E11", "E9")}),
            vocabulary=frozenset({"E22", "E5", "E11", "E9"}),
        )
        path = tmp_path / "model.json"
        path.write_text(json.dumps(m.to_dict()))
        loaded = SequenceModel.load(path)
        assert loaded.normal_ngrams == m.normal_ngrams

    def test_malformed_dict_raises(self) -> None:
        with pytest.raises(ValueError, match="malformed"):
            SequenceModel.from_dict({"n": 2, "normal_ngrams": "nope"})


class TestDetect:
    async def test_normal_trace_no_anomaly(self) -> None:
        # Train: any 2-gram seen in this trace is normal.
        normal_seq = ("E22", "E5", "E11", "E9")
        normal_ngrams = frozenset(ngrams(normal_seq, 2))
        model = SequenceModel(
            n=2,
            normal_ngrams=normal_ngrams,
            vocabulary=frozenset(normal_seq),
        )
        d = SequenceAnomalyDetector(model=model, threshold=0.0)
        result = await d.detect_trace(_trace(normal_seq))
        assert result is None

    async def test_novel_ngram_fires_anomaly(self) -> None:
        normal = frozenset({("E22", "E5"), ("E5", "E11"), ("E11", "E9")})
        model = SequenceModel(
            n=2, normal_ngrams=normal, vocabulary=frozenset({"E22", "E5", "E11", "E9", "E4"})
        )
        d = SequenceAnomalyDetector(model=model, threshold=0.0)
        # Inject an exception event in the middle.
        anomalous = ("E22", "E5", "E4", "E11", "E9")
        result = await d.detect_trace(_trace(anomalous))
        assert result is not None
        assert result.detector_name == "sequence_ngram"
        assert result.metadata["block_id"] == "blk_1"
        assert result.severity_score > 0
        # The novel n-grams should be the ones touching E4.
        novel = {tuple(g) for g in result.metadata["novel_ngrams"]}
        assert ("E5", "E4") in novel or ("E4", "E11") in novel

    async def test_threshold_suppresses_low_ratio(self) -> None:
        # Long normal sequence with one novel pair: ratio should be small.
        normal = frozenset(
            {("A", "B"), ("B", "A")}
        )
        model = SequenceModel(n=2, normal_ngrams=normal, vocabulary=frozenset({"A", "B", "X"}))
        long_seq = ("A", "B", "A", "B", "A", "B", "A", "X")
        # 7 unique 2-grams, 1 novel. ratio = 1/7 ≈ 0.14.
        # With threshold 0.5 it should NOT fire.
        d = SequenceAnomalyDetector(model=model, threshold=0.5)
        assert (await d.detect_trace(_trace(long_seq))) is None

    async def test_short_trace_yields_no_ngrams(self) -> None:
        model = SequenceModel.empty(n=2)
        d = SequenceAnomalyDetector(model=model)
        # Length-1 sequence has no 2-grams → cannot be scored.
        assert (await d.detect_trace(_trace(("A",)))) is None


class TestConstruction:
    def test_rejects_negative_threshold(self) -> None:
        with pytest.raises(ValueError, match="threshold"):
            SequenceAnomalyDetector(threshold=-0.1)
