"""Tests for LabelAnomalyDetector."""

from __future__ import annotations

import pytest

from pipelinex.core.models import LogRecord
from pipelinex.detectors.label import LabelAnomalyDetector


def _record(label: object) -> LogRecord:
    r = LogRecord()
    if label is not None:
        r.add_enrichment("bgl_label", label)
    return r


class TestLabelDetect:
    async def test_fires_on_non_dash_label(self) -> None:
        d = LabelAnomalyDetector()
        event = await d.detect(_record("KERNDTLB"))
        assert event is not None
        assert event.detector_name == "label"
        assert event.severity_score == 1.0
        assert event.metadata["label"] == "KERNDTLB"

    async def test_silent_on_dash_label(self) -> None:
        d = LabelAnomalyDetector()
        event = await d.detect(_record("-"))
        assert event is None

    async def test_silent_when_label_missing(self) -> None:
        d = LabelAnomalyDetector()
        # No enrichment at all.
        event = await d.detect(LogRecord())
        assert event is None

    async def test_silent_when_label_empty_string(self) -> None:
        d = LabelAnomalyDetector()
        event = await d.detect(_record(""))
        assert event is None

    async def test_silent_when_label_non_string(self) -> None:
        d = LabelAnomalyDetector()
        event = await d.detect(_record(42))
        assert event is None

    async def test_custom_label_key(self) -> None:
        d = LabelAnomalyDetector(label_key="alert_code")
        r = LogRecord()
        r.add_enrichment("alert_code", "FAULT_5")
        event = await d.detect(r)
        assert event is not None
        assert event.metadata["label_key"] == "alert_code"

    async def test_custom_normal_value(self) -> None:
        d = LabelAnomalyDetector(normal_value="ok")
        r1 = _record("ok")
        r2 = _record("alert")
        assert (await d.detect(r1)) is None
        e = await d.detect(r2)
        assert e is not None and e.metadata["label"] == "alert"

    async def test_reads_from_raw_payload(self) -> None:
        d = LabelAnomalyDetector(source_dict="raw_payload")
        r = LogRecord(raw_payload={"bgl_label": "APPSEV"})
        event = await d.detect(r)
        assert event is not None
        assert event.metadata["label"] == "APPSEV"


class TestLabelConstruction:
    def test_rejects_empty_label_key(self) -> None:
        with pytest.raises(ValueError, match="label_key"):
            LabelAnomalyDetector(label_key="")

    def test_rejects_invalid_source_dict(self) -> None:
        with pytest.raises(ValueError, match="source_dict"):
            LabelAnomalyDetector(source_dict="metadata")

    def test_reset_is_stateless(self) -> None:
        d = LabelAnomalyDetector()
        # No-op shouldn't raise.
        d.reset()
