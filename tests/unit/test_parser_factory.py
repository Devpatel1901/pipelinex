"""Unit tests for ParserFactory."""

from __future__ import annotations

import pytest

from pipelinex.core.exceptions import ConfigurationError, ParserError
from pipelinex.core.interfaces import IParser
from pipelinex.core.models import LogRecord
from pipelinex.stages.parsing.factory import ParserFactory, default_factory
from pipelinex.stages.parsing.hdfs_parser import HDFSParser


class _StubParser(IParser):
    @property
    def name(self) -> str:
        return "stub"

    def can_parse(self, raw: str) -> bool:
        return raw.startswith("STUB:")

    def parse(self, raw: str) -> LogRecord:
        return LogRecord(message=raw[5:])


class TestRegistration:
    def test_register_then_create(self) -> None:
        f = ParserFactory()
        f.register("stub", _StubParser)
        parser = f.create("stub")
        assert isinstance(parser, _StubParser)

    def test_double_registration_raises(self) -> None:
        f = ParserFactory()
        f.register("stub", _StubParser)
        with pytest.raises(ConfigurationError, match="already registered"):
            f.register("stub", _StubParser)

    def test_unknown_kind_raises(self) -> None:
        f = ParserFactory()
        with pytest.raises(ConfigurationError, match="unknown parser kind"):
            f.create("nope")

    def test_known_kinds_listed(self) -> None:
        f = ParserFactory()
        f.register("stub", _StubParser)
        assert f.known_kinds() == ["stub"]


class TestDetect:
    def test_detect_returns_matching_parser(self) -> None:
        f = ParserFactory()
        f.register("hdfs", HDFSParser)
        f.register("stub", _StubParser)
        parser = f.detect("STUB:hello")
        assert parser.name == "stub"

    def test_detect_with_no_match_raises(self) -> None:
        f = ParserFactory()
        f.register("stub", _StubParser)
        with pytest.raises(ParserError, match="no registered parser"):
            f.detect("not a stub line")


class TestDefaultFactory:
    def test_default_includes_known_parsers(self) -> None:
        f = default_factory()
        kinds = f.known_kinds()
        assert "hdfs" in kinds
        assert "hdfs_structured_csv" in kinds
        assert "bgl" in kinds
