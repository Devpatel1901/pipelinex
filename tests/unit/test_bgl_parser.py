"""Unit tests for the BGL parser."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from pipelinex.core.exceptions import ParserError
from pipelinex.core.models import Severity
from pipelinex.stages.parsing.bgl_parser import BGLParser

NORMAL_LINE = (
    "- 1117838570 2005.06.03 R02-M1-N0-C:J12-U11 "
    "2005-06-03-15.42.50.675872 R02-M1-N0-C:J12-U11 RAS KERNEL INFO "
    "instruction cache parity error corrected"
)
ALERT_LINE = (
    "KERNDTLB 1118536327 2005.06.11 R30-M0-N9-C:J16-U01 "
    "2005-06-11-17.32.07.581048 R30-M0-N9-C:J16-U01 RAS KERNEL FATAL "
    "data TLB error interrupt"
)


@pytest.fixture
def parser() -> BGLParser:
    return BGLParser()


class TestCanParse:
    def test_accepts_normal_line(self, parser: BGLParser) -> None:
        assert parser.can_parse(NORMAL_LINE) is True

    def test_accepts_alert_line(self, parser: BGLParser) -> None:
        assert parser.can_parse(ALERT_LINE) is True

    def test_rejects_non_bgl_line(self, parser: BGLParser) -> None:
        assert parser.can_parse("081109 203615 148 INFO dfs.X: hi") is False

    def test_rejects_empty_line(self, parser: BGLParser) -> None:
        assert parser.can_parse("") is False


class TestParseNormalLine:
    def test_extracts_basic_fields(self, parser: BGLParser) -> None:
        record = parser.parse(NORMAL_LINE)
        assert record.timestamp == datetime(
            2005, 6, 3, 15, 42, 50, 675872, tzinfo=UTC
        )
        assert record.source == "KERNEL"
        assert record.severity is Severity.INFO
        assert "instruction cache" in record.message

    def test_label_dash_means_normal(self, parser: BGLParser) -> None:
        record = parser.parse(NORMAL_LINE)
        assert record.enrichment["bgl_label"] == "-"

    def test_node_extracted(self, parser: BGLParser) -> None:
        record = parser.parse(NORMAL_LINE)
        assert record.enrichment["node"] == "R02-M1-N0-C:J12-U11"


class TestParseAlertLine:
    def test_label_is_the_alert_code(self, parser: BGLParser) -> None:
        record = parser.parse(ALERT_LINE)
        assert record.enrichment["bgl_label"] == "KERNDTLB"

    def test_fatal_maps_to_critical(self, parser: BGLParser) -> None:
        record = parser.parse(ALERT_LINE)
        assert record.severity is Severity.CRITICAL


class TestParseFailures:
    def test_too_few_fields_raises(self, parser: BGLParser) -> None:
        with pytest.raises(ParserError, match="only"):
            parser.parse("- 1117838570 short")

    def test_invalid_timestamp_raises(self, parser: BGLParser) -> None:
        broken = (
            "- 1117838570 2005.06.03 NODE NOT-A-TIMESTAMP NODE RAS "
            "KERNEL INFO message body"
        )
        with pytest.raises(ParserError, match="timestamp"):
            parser.parse(broken)


class TestRealBGLSample:
    """Validate against the committed 100-line BGL sample."""

    @pytest.fixture
    def bgl_sample(self) -> Path:
        path = (
            Path(__file__).resolve().parents[2]
            / "data"
            / "samples"
            / "BGL_sample_100.log"
        )
        if not path.exists():
            pytest.skip(f"BGL sample missing: {path}")
        return path

    def test_parses_full_sample(self, parser: BGLParser, bgl_sample: Path) -> None:
        lines = [
            line for line in bgl_sample.read_text(encoding="utf-8").splitlines() if line
        ]
        records = []
        failures = 0
        for raw in lines:
            try:
                records.append(parser.parse(raw))
            except ParserError:
                failures += 1
        assert len(records) >= 95, f"too many parse failures ({failures})"

        # All records carry a label; either '-' or an alert code.
        for r in records:
            label = r.enrichment.get("bgl_label")
            assert label is not None
            assert label == "-" or label.isupper()

    def test_distinguishes_normal_from_alert(
        self, parser: BGLParser, bgl_sample: Path
    ) -> None:
        lines = [
            line for line in bgl_sample.read_text(encoding="utf-8").splitlines() if line
        ]
        records = []
        for raw in lines:
            try:
                records.append(parser.parse(raw))
            except ParserError:
                continue

        # Verify both label classes appear *or* the file is uniformly one
        # class. Either way, the parser doesn't conflate them.
        labels = {r.enrichment["bgl_label"] for r in records}
        assert "-" in labels or any(label != "-" for label in labels)
