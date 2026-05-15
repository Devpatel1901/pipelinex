"""Tests for FeatureWindowDetector."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from pipelinex.core.models import LogRecord, Severity
from pipelinex.detectors.feature_window import FeatureWindowDetector, _entropy


def _record(
    *,
    ts: datetime,
    source: str = "compA",
    severity: Severity = Severity.INFO,
    event_id: str | None = "E1",
) -> LogRecord:
    rec = LogRecord(timestamp=ts, source=source, severity=severity)
    if event_id is not None:
        rec.add_enrichment("event_id", event_id)
    return rec


class TestEntropy:
    def test_single_bin_is_zero(self) -> None:
        assert _entropy([10]) == 0.0

    def test_uniform_two_bins_is_log2(self) -> None:
        import math

        assert _entropy([5, 5]) == pytest.approx(math.log(2))

    def test_empty_is_zero(self) -> None:
        assert _entropy([]) == 0.0

    def test_zero_counts_skipped(self) -> None:
        # Should equal entropy of just the non-zero element → 0.
        assert _entropy([0, 7, 0]) == 0.0


class TestConstruction:
    def test_rejects_zero_window_seconds(self) -> None:
        with pytest.raises(ValueError, match="window_seconds"):
            FeatureWindowDetector(window_seconds=0)

    def test_rejects_small_feature_history(self) -> None:
        with pytest.raises(ValueError, match="feature_history"):
            FeatureWindowDetector(feature_history=1)

    def test_rejects_small_min_samples(self) -> None:
        with pytest.raises(ValueError, match="min_samples"):
            FeatureWindowDetector(min_samples=3)

    def test_rejects_unknown_feature(self) -> None:
        with pytest.raises(ValueError, match="unknown features"):
            FeatureWindowDetector(features=["rate", "no_such_feature"])

    def test_name(self) -> None:
        assert FeatureWindowDetector().name == "feature_window"


class TestDetect:
    async def test_no_timestamp_returns_none(self) -> None:
        d = FeatureWindowDetector(window_seconds=60)
        rec = LogRecord(timestamp=None)
        assert (await d.detect(rec)) is None

    async def test_records_in_same_window_dont_fire(self) -> None:
        d = FeatureWindowDetector(window_seconds=60, min_samples=4)
        ts = datetime(2024, 1, 1, 12, 0, 5, tzinfo=UTC)
        for i in range(5):
            out = await d.detect(_record(ts=ts + timedelta(seconds=i)))
            assert out is None

    async def test_warmup_no_fire(self) -> None:
        # Before min_samples windows have closed, no scoring possible.
        d = FeatureWindowDetector(
            window_seconds=60, min_samples=10, feature_history=50, iqr_k=1.5
        )
        ts = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
        # Close 5 quiet windows.
        for w in range(5):
            base = ts + timedelta(minutes=w)
            for i in range(3):
                out = await d.detect(_record(ts=base + timedelta(seconds=i)))
            # Crossing to the next window flushes the prior one — but with
            # only 5 windows of history, min_samples=10 not yet met.
            assert out is None

    async def test_outlier_window_fires(self) -> None:
        d = FeatureWindowDetector(
            window_seconds=60,
            min_samples=10,
            feature_history=50,
            iqr_k=1.5,
            features=["rate"],
        )
        base = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
        # 30 windows of low rate (2-5 records) so IQR is non-zero. If the
        # warmup is perfectly constant IQR collapses to 0 and the detector
        # cannot score — mirrors IQRDetector's constant-window behaviour.
        for w in range(30):
            for i in range(2 + w % 4):
                await d.detect(_record(ts=base + timedelta(minutes=w, seconds=i)))
        # Window 30: huge spike (50 records). The detector emits the
        # anomaly when window 31 starts.
        spike_window_start = base + timedelta(minutes=30)
        for i in range(50):
            await d.detect(_record(ts=spike_window_start + timedelta(seconds=i % 60)))
        # Trigger close of the spike window by crossing into window 31.
        out = await d.detect(_record(ts=base + timedelta(minutes=31, seconds=0)))
        assert out is not None
        assert out.detector_name == "feature_window"
        assert "rate" in out.metadata["breached_features"]
        assert out.metadata["features"]["rate"] == 50.0
        assert out.severity_score > 0

    async def test_flush_emits_pending_window(self) -> None:
        d = FeatureWindowDetector(
            window_seconds=60,
            min_samples=10,
            feature_history=50,
            iqr_k=1.5,
            features=["rate"],
        )
        base = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
        for w in range(30):
            for i in range(2 + w % 4):
                await d.detect(_record(ts=base + timedelta(minutes=w, seconds=i)))
        # Last window has a spike, but we never cross into a new window.
        last_start = base + timedelta(minutes=30)
        for i in range(50):
            await d.detect(_record(ts=last_start + timedelta(seconds=i % 60)))
        out = d.flush()
        assert out is not None
        assert out.metadata["features"]["rate"] == 50.0

    async def test_flush_when_empty(self) -> None:
        d = FeatureWindowDetector()
        assert d.flush() is None

    async def test_reset_clears_state(self) -> None:
        d = FeatureWindowDetector(
            window_seconds=60,
            min_samples=10,
            feature_history=50,
            iqr_k=1.5,
            features=["rate"],
        )
        base = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
        for w in range(30):
            for i in range(2 + w % 4):
                await d.detect(_record(ts=base + timedelta(minutes=w, seconds=i)))
        d.reset()
        # Same outlier should no longer fire — history is gone.
        spike_start = base + timedelta(minutes=30)
        for i in range(50):
            await d.detect(_record(ts=spike_start + timedelta(seconds=i % 60)))
        out = await d.detect(_record(ts=base + timedelta(minutes=31)))
        assert out is None  # warmup again

    async def test_severity_features_tracked(self) -> None:
        d = FeatureWindowDetector(
            window_seconds=60,
            min_samples=4,
            feature_history=20,
            iqr_k=1.5,
            features=["severity_warn_ratio"],
        )
        base = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
        # Mostly INFO across 12 windows, with a varying small amount of WARN
        # (1 in 5) so warn_ratio history has non-zero IQR.
        for w in range(12):
            mix = [Severity.INFO] * 4 + [Severity.WARNING] * (w % 3)
            for i, sev in enumerate(mix):
                await d.detect(
                    _record(
                        ts=base + timedelta(minutes=w, seconds=i),
                        severity=sev,
                    )
                )
        # Window 12: all ERROR — warn_ratio jumps to 1.0.
        bad_start = base + timedelta(minutes=12)
        for i in range(5):
            await d.detect(
                _record(
                    ts=bad_start + timedelta(seconds=i),
                    severity=Severity.ERROR,
                )
            )
        # Crossing into window 13 closes the bad window.
        out = await d.detect(_record(ts=base + timedelta(minutes=13)))
        assert out is not None
        assert "severity_warn_ratio" in out.metadata["breached_features"]


class TestFactoryIntegration:
    def test_registered_in_default_factory(self) -> None:
        from pipelinex.detectors.factory import default_factory

        f = default_factory()
        assert "feature_window" in f.known_numeric()
        assert "sequence_loglikelihood" in f.known_sequence()

    def test_factory_creates_feature_window(self) -> None:
        from pipelinex.detectors.factory import default_factory

        det = default_factory().create_numeric(
            "feature_window",
            window_seconds=60,
            features=["rate"],
        )
        assert det.name == "feature_window"

    def test_factory_creates_loglikelihood(self) -> None:
        from pipelinex.detectors.factory import default_factory

        det = default_factory().create_sequence(
            "sequence_loglikelihood",
            threshold=-5.0,
            alpha=0.1,
        )
        assert det.name == "sequence_loglikelihood"
