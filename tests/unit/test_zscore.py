"""Unit tests for the Z-Score detector and DetectorStage."""

from __future__ import annotations

import pytest

from pipelinex.core.models import LogRecord
from pipelinex.detectors.detector_stage import DetectorStage
from pipelinex.detectors.zscore import ZScoreDetector
from pipelinex.persistence.memory_repo import InMemoryLogRepository


def _record(value: float | int | None) -> LogRecord:
    rec = LogRecord()
    if value is not None:
        rec.add_enrichment("metric", value)
    return rec


@pytest.fixture
def detector() -> ZScoreDetector:
    # Small thresholds to keep tests fast and deterministic.
    return ZScoreDetector(threshold=3.0, window_size=100, min_samples=30)


class TestConstruction:
    def test_rejects_window_too_small(self) -> None:
        with pytest.raises(ValueError, match="window_size"):
            ZScoreDetector(window_size=1)

    def test_rejects_negative_threshold(self) -> None:
        with pytest.raises(ValueError, match="threshold"):
            ZScoreDetector(threshold=-1.0)

    def test_rejects_min_samples_too_small(self) -> None:
        with pytest.raises(ValueError, match="min_samples"):
            ZScoreDetector(min_samples=1)

    def test_rejects_unknown_source_dict(self) -> None:
        with pytest.raises(ValueError, match="source_dict"):
            ZScoreDetector(source_dict="garbage")


class TestDetect:
    async def test_no_metric_no_anomaly(self, detector: ZScoreDetector) -> None:
        result = await detector.detect(_record(None))
        assert result is None

    async def test_warmup_period_yields_no_detections(
        self, detector: ZScoreDetector
    ) -> None:
        # First 30 records (min_samples) cannot produce anomalies.
        for v in range(29):
            result = await detector.detect(_record(v))
            assert result is None

    async def test_steady_state_no_anomaly(self, detector: ZScoreDetector) -> None:
        # Constant value: sigma=0, detector returns None (documented behavior).
        for _ in range(50):
            result = await detector.detect(_record(10.0))
            assert result is None

    async def test_outlier_after_warmup_is_flagged(
        self, detector: ZScoreDetector
    ) -> None:
        # 50 records of small noise around 10, then a huge spike.
        for i in range(50):
            await detector.detect(_record(10.0 + (i % 5) * 0.1))

        anomaly = await detector.detect(_record(1000.0))
        assert anomaly is not None
        assert anomaly.detector_name == "z_score"
        assert anomaly.severity_score > 3.0
        assert anomaly.metadata["value"] == 1000.0

    async def test_inlier_is_not_flagged(self, detector: ZScoreDetector) -> None:
        for i in range(50):
            await detector.detect(_record(10.0 + (i % 5) * 0.1))

        anomaly = await detector.detect(_record(10.05))
        assert anomaly is None

    async def test_reset_clears_window(self, detector: ZScoreDetector) -> None:
        for i in range(50):
            await detector.detect(_record(10.0 + i * 0.01))
        detector.reset()
        # After reset, we're back in warmup → no detection.
        result = await detector.detect(_record(1e6))
        assert result is None

    async def test_boolean_value_is_ignored(self, detector: ZScoreDetector) -> None:
        # bool is a subclass of int in Python; we must not treat True/False
        # as numeric metrics.
        result = await detector.detect(_record(True))  # type: ignore[arg-type]
        assert result is None

    async def test_string_value_is_ignored(self, detector: ZScoreDetector) -> None:
        rec = LogRecord()
        rec.add_enrichment("metric", "not a number")
        result = await detector.detect(rec)
        assert result is None


class TestDetectorStage:
    async def test_persists_anomalies(self) -> None:
        detector = ZScoreDetector(threshold=2.0, window_size=20, min_samples=10)
        repo = InMemoryLogRepository()
        stage = DetectorStage(detector, repo)

        # Establish baseline.
        for i in range(15):
            await stage.process(_record(10.0 + (i % 3) * 0.1))

        # Inject a clear outlier.
        rec = _record(500.0)
        out = await stage.process(rec)
        # Stage returns the same (mutated) record.
        assert out.id == rec.id
        anomalies = await repo.all_anomalies()
        assert len(anomalies) == 1
        assert stage.anomaly_count == 1
        # The record gets marked with the anomaly.
        assert "anomaly:z_score" in rec.enrichment

    async def test_stage_name_includes_detector(self) -> None:
        stage = DetectorStage(ZScoreDetector(), InMemoryLogRepository())
        assert stage.name == "detect:z_score"
