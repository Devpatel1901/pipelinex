"""Property-based tests for parsers.

The contract enforced by these tests is the project's robustness claim:
**a parser either succeeds or raises ParserError — never any other
exception.** Crashing on malformed input would mean the worker dies and
the queue stalls.

We use Hypothesis to generate adversarial inputs that we *might* never
think to write by hand: random text, near-valid strings with mutated
fields, and well-formed lines with mutated timestamps.
"""

from __future__ import annotations

from datetime import UTC, datetime

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from pipelinex.core.exceptions import ParserError
from pipelinex.stages.parsing.bgl_parser import BGLParser
from pipelinex.stages.parsing.hdfs_parser import HDFSParser

_HDFS_PARSER = HDFSParser()
_BGL_PARSER = BGLParser()


@st.composite
def _hdfs_like_lines(draw: st.DrawFn) -> str:
    """Generate strings that *look* like HDFS lines, with optional mutation.

    Guaranteed to be in HDFS shape some of the time (so the parser hits the
    happy path) and outright junk other times.
    """
    if draw(st.booleans()):
        # Random text — should fail to parse, but must not crash.
        return draw(st.text(max_size=200))

    # Build a "real-looking" HDFS line and optionally mutate one field.
    date = draw(st.integers(min_value=0, max_value=999999)).__str__().zfill(6)[:6]
    time = draw(st.integers(min_value=0, max_value=999999)).__str__().zfill(6)[:6]
    pid = draw(st.integers(min_value=0, max_value=99999))
    level = draw(st.sampled_from(["INFO", "WARN", "ERROR", "FATAL", "WEIRD"]))
    component = draw(st.sampled_from(["dfs.X", "dfs.Y$Z", "x.y.z"]))
    content = draw(st.text(max_size=80))
    return f"{date} {time} {pid} {level} {component}: {content}"


@st.composite
def _bgl_like_lines(draw: st.DrawFn) -> str:
    if draw(st.booleans()):
        return draw(st.text(max_size=200))

    label = draw(st.sampled_from(["-", "KERNDTLB", "APPSEV"]))
    epoch = draw(st.integers(min_value=0, max_value=10**10))
    date = "2005.06.03"
    node = "R02-M1-N0-C:J12-U11"
    time_str = "2005-06-03-15.42.50.675872"
    log_type = "RAS"
    component = "KERNEL"
    level = draw(st.sampled_from(["INFO", "WARNING", "FATAL", "WEIRD"]))
    content = draw(st.text(max_size=80))
    return (
        f"{label} {epoch} {date} {node} {time_str} {node} "
        f"{log_type} {component} {level} {content}"
    )


class TestHDFSParserRobustness:
    @given(_hdfs_like_lines())
    @settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow])
    def test_parser_either_succeeds_or_raises_parser_error(self, line: str) -> None:
        try:
            record = _HDFS_PARSER.parse(line)
        except ParserError:
            return
        # If it succeeded, the result must be coherent.
        assert isinstance(record.message, str)
        assert isinstance(record.timestamp, datetime)
        assert record.timestamp.tzinfo is UTC

    @given(st.text(max_size=300))
    @settings(max_examples=200)
    def test_can_parse_never_raises(self, line: str) -> None:
        # can_parse is a cheap shape check — must always return a bool.
        result = _HDFS_PARSER.can_parse(line)
        assert isinstance(result, bool)


class TestBGLParserRobustness:
    @given(_bgl_like_lines())
    @settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow])
    def test_parser_either_succeeds_or_raises_parser_error(self, line: str) -> None:
        try:
            record = _BGL_PARSER.parse(line)
        except ParserError:
            return
        assert isinstance(record.message, str)
        assert "bgl_label" in record.enrichment

    @given(st.text(max_size=300))
    @settings(max_examples=200)
    def test_can_parse_never_raises(self, line: str) -> None:
        result = _BGL_PARSER.can_parse(line)
        assert isinstance(result, bool)
