"""Tests for the IQR and CUSUM detectors."""

from __future__ import annotations

import pytest

from pipelinex.core.models import LogRecord
from pipelinex.detectors.cusum import CUSUMDetector
from pipelinex.detectors.iqr import IQRDetector


def _record(value: float | int | None) -> LogRecord:
    rec = LogRecord()
    if value is not None:
        rec.add_enrichment("metric", value)
    return rec


# ---------- IQR ----------


class TestIQRConstruction:
    def test_rejects_negative_k(self) -> None:
        with pytest.raises(ValueError, match="k"):
            IQRDetector(k=-1.0)

    def test_rejects_window_too_small(self) -> None:
        with pytest.raises(ValueError, match="window_size"):
            IQRDetector(window_size=1)

    def test_rejects_min_samples_under_4(self) -> None:
        with pytest.raises(ValueError, match="min_samples"):
            IQRDetector(min_samples=3)


class TestIQRDetect:
    async def test_warmup_no_detection(self) -> None:
        d = IQRDetector(window_size=50, min_samples=20, k=1.5)
        for v in range(15):
            assert (await d.detect(_record(v))) is None

    async def test_inlier_passes(self) -> None:
        d = IQRDetector(window_size=100, min_samples=20, k=1.5)
        for i in range(60):
            await d.detect(_record(10.0 + (i % 5) * 0.1))
        assert (await d.detect(_record(10.2))) is None

    async def test_outlier_above_fences_fires(self) -> None:
        d = IQRDetector(window_size=100, min_samples=20, k=1.5)
        for i in range(60):
            await d.detect(_record(10.0 + (i % 5) * 0.1))
        out = await d.detect(_record(1000.0))
        assert out is not None
        assert out.metadata["side"] == "above"
        assert out.metadata["value"] == 1000.0

    async def test_outlier_below_fences_fires(self) -> None:
        d = IQRDetector(window_size=100, min_samples=20, k=1.5)
        for i in range(60):
            await d.detect(_record(10.0 + (i % 5) * 0.1))
        out = await d.detect(_record(-1000.0))
        assert out is not None
        assert out.metadata["side"] == "below"

    async def test_constant_window_returns_none(self) -> None:
        d = IQRDetector(window_size=100, min_samples=10, k=1.5)
        for _ in range(50):
            await d.detect(_record(5.0))
        # IQR == 0 → no fences possible.
        assert (await d.detect(_record(1e6))) is None

    async def test_reset_clears_state(self) -> None:
        d = IQRDetector(window_size=50, min_samples=10, k=1.5)
        for i in range(40):
            await d.detect(_record(10.0 + i * 0.01))
        d.reset()
        # Back to warmup; one big spike should not fire yet.
        assert (await d.detect(_record(1e6))) is None


# ---------- CUSUM ----------


class TestCUSUMConstruction:
    def test_rejects_zero_threshold(self) -> None:
        with pytest.raises(ValueError, match="threshold"):
            CUSUMDetector(threshold=0)

    def test_rejects_negative_slack(self) -> None:
        with pytest.raises(ValueError, match="slack"):
            CUSUMDetector(slack=-0.1)


class TestCUSUMDetect:
    async def test_warmup_no_detection(self) -> None:
        d = CUSUMDetector(warmup_samples=20, threshold=2.0)
        for v in range(15):
            assert (await d.detect(_record(v))) is None

    async def test_steady_no_detection(self) -> None:
        d = CUSUMDetector(warmup_samples=30, threshold=5.0, slack=0.5)
        # Long warmup of small noise.
        for i in range(60):
            await d.detect(_record(10.0 + (i % 3) * 0.01))
        # More small noise — no anomaly.
        for i in range(50):
            res = await d.detect(_record(10.0 + (i % 3) * 0.01))
            assert res is None

    async def test_drift_detected(self) -> None:
        d = CUSUMDetector(warmup_samples=30, threshold=4.0, slack=0.25)
        # Warmup at mean 10.
        for _ in range(40):
            await d.detect(_record(10.0))
        # Slow drift up: each step is +0.5 stddev; CUSUM should fire well
        # before any single point would look obviously anomalous.
        fired = False
        for i in range(60):
            res = await d.detect(_record(10.0 + i * 0.1))
            if res is not None:
                fired = True
                assert res.metadata["direction"] in {"up", "down"}
                break
        assert fired

    async def test_reset_clears_state(self) -> None:
        d = CUSUMDetector(warmup_samples=30, threshold=4.0, slack=0.25)
        for _ in range(40):
            await d.detect(_record(10.0))
        d.reset()
        # Need a fresh warmup.
        assert (await d.detect(_record(1e6))) is None
