"""Unit tests for the HDFS parser."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from pipelinex.core.exceptions import ParserError
from pipelinex.core.models import Severity
from pipelinex.stages.parsing.hdfs_parser import HDFSParser


@pytest.fixture
def parser() -> HDFSParser:
    return HDFSParser()


SAMPLE_LINE = (
    "081109 203615 148 INFO dfs.DataNode$PacketResponder: "
    "PacketResponder 1 for block blk_38865049064139660 terminating"
)


class TestCanParse:
    def test_accepts_well_formed_line(self, parser: HDFSParser) -> None:
        assert parser.can_parse(SAMPLE_LINE) is True

    def test_rejects_non_hdfs_line(self, parser: HDFSParser) -> None:
        assert parser.can_parse("- 1117838570 2005.06.03 ... RAS KERNEL INFO") is False

    def test_rejects_empty_line(self, parser: HDFSParser) -> None:
        assert parser.can_parse("") is False


class TestParse:
    def test_parses_canonical_line(self, parser: HDFSParser) -> None:
        record = parser.parse(SAMPLE_LINE)
        assert record.timestamp == datetime(2008, 11, 9, 20, 36, 15, tzinfo=UTC)
        assert record.severity is Severity.INFO
        assert record.source == "dfs.DataNode$PacketResponder"
        assert "PacketResponder 1 for block" in record.message
        assert record.raw_payload["pid"] == "148"
        assert record.raw_payload["raw"] == SAMPLE_LINE

    def test_extracts_block_id(self, parser: HDFSParser) -> None:
        record = parser.parse(SAMPLE_LINE)
        assert record.enrichment["block_id"] == "blk_38865049064139660"

    def test_extracts_negative_block_id(self, parser: HDFSParser) -> None:
        line = (
            "081109 203807 222 INFO dfs.DataNode$PacketResponder: "
            "PacketResponder 0 for block blk_-6952295868487656571 terminating"
        )
        record = parser.parse(line)
        assert record.enrichment["block_id"] == "blk_-6952295868487656571"

    def test_warn_level_maps_to_warning(self, parser: HDFSParser) -> None:
        line = (
            "081109 204015 308 WARN dfs.FSNamesystem: "
            "BLOCK* NameSystem.addStoredBlock: Redundant addStoredBlock for blk_1"
        )
        record = parser.parse(line)
        assert record.severity is Severity.WARNING

    def test_unknown_level_falls_back_with_warning(self, parser: HDFSParser) -> None:
        line = "081109 204015 308 NOTICE dfs.FSNamesystem: some message"
        record = parser.parse(line)
        assert record.severity is Severity.INFO
        assert "parse_warning" in record.enrichment

    def test_invalid_format_raises(self, parser: HDFSParser) -> None:
        with pytest.raises(ParserError):
            parser.parse("not an hdfs line at all")

    def test_invalid_timestamp_raises(self, parser: HDFSParser) -> None:
        # Date 991399 is invalid (month 13, day 99).
        with pytest.raises(ParserError):
            parser.parse("991399 999999 1 INFO foo: bar")

    def test_line_without_block_id_omits_enrichment(self, parser: HDFSParser) -> None:
        line = "081109 204015 308 INFO dfs.FSNamesystem: starting up"
        record = parser.parse(line)
        assert "block_id" not in record.enrichment
