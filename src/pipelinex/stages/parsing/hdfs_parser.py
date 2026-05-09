"""HDFS log parser.

Parses raw Hadoop HDFS log lines like::

    081109 203615 148 INFO dfs.DataNode$PacketResponder: PacketResponder 1 for block blk_38865049064139660 terminating

into ``LogRecord`` instances. The block-id (``blk_<n>``) is extracted into
``enrichment["block_id"]`` so downstream stages (sessionizer, sequence
detector) can group lines by block.

Template matching is *not* done here — that is a separate enrichment stage
(``TemplateMatcherStage``) so the matcher can be timing-decorated and toggled
in YAML independently of parsing.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Final

from pipelinex.core.exceptions import ParserError
from pipelinex.core.interfaces import IParser
from pipelinex.core.models import LogRecord, Severity

_LINE_RE: Final = re.compile(
    r"^(?P<date>\d{6}) (?P<time>\d{6}) (?P<pid>\d+) "
    r"(?P<level>\w+) (?P<component>[\w$.]+): (?P<content>.*)$"
)
_BLOCK_RE: Final = re.compile(r"blk_(-?\d+)")
_SHAPE_RE: Final = re.compile(r"^\d{6} \d{6} \d+ ")

_LEVEL_MAP: Final[dict[str, Severity]] = {
    "DEBUG": Severity.DEBUG,
    "INFO": Severity.INFO,
    "WARN": Severity.WARNING,
    "WARNING": Severity.WARNING,
    "ERROR": Severity.ERROR,
    "FATAL": Severity.CRITICAL,
}


class HDFSParser(IParser):
    """Strategy: parse HDFS log lines."""

    @property
    def name(self) -> str:
        return "hdfs"

    def can_parse(self, raw: str) -> bool:
        return bool(_SHAPE_RE.match(raw))

    def parse(self, raw: str) -> LogRecord:
        match = _LINE_RE.match(raw)
        if match is None:
            raise ParserError(f"line does not match HDFS format: {raw[:80]!r}")

        date = match.group("date")
        time = match.group("time")
        try:
            timestamp = datetime.strptime(date + time, "%y%m%d%H%M%S").replace(tzinfo=UTC)
        except ValueError as e:
            raise ParserError(f"invalid HDFS timestamp {date}{time}: {e}") from e

        level_str = match.group("level")
        severity = _LEVEL_MAP.get(level_str, Severity.INFO)
        parse_warning = level_str not in _LEVEL_MAP

        component = match.group("component")
        content = match.group("content")

        record = LogRecord(
            timestamp=timestamp,
            source=component,
            severity=severity,
            message=content,
            raw_payload={
                "pid": match.group("pid"),
                "level_raw": level_str,
                "raw": raw,
            },
        )

        block_match = _BLOCK_RE.search(content)
        if block_match:
            record.add_enrichment("block_id", "blk_" + block_match.group(1))

        if parse_warning:
            record.add_enrichment("parse_warning", f"unknown level: {level_str}")

        return record
