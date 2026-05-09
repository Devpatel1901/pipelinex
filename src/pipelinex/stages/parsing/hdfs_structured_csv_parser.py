"""Parser for the *pre-parsed* HDFS structured CSV format.

This exists as the **regression oracle** for ``HDFSParser``: parse a raw
``HDFS.log`` line through ``HDFSParser`` and parse the matching row of
``HDFS.log_structured.csv`` through this parser, then assert the resulting
``LogRecord``s are identical. Any drift signals a parser bug.

The structured CSV header is::

    LineId,Date,Time,Pid,Level,Component,Content,EventId,EventTemplate

Each row is one already-parsed log line plus an event-id assigned by Drain.
"""

from __future__ import annotations

import csv
import re
from datetime import UTC, datetime
from io import StringIO
from typing import Final

from pipelinex.core.exceptions import ParserError
from pipelinex.core.interfaces import IParser
from pipelinex.core.models import LogRecord, Severity

_BLOCK_RE: Final = re.compile(r"blk_(-?\d+)")

_LEVEL_MAP: Final[dict[str, Severity]] = {
    "DEBUG": Severity.DEBUG,
    "INFO": Severity.INFO,
    "WARN": Severity.WARNING,
    "WARNING": Severity.WARNING,
    "ERROR": Severity.ERROR,
    "FATAL": Severity.CRITICAL,
}

_EXPECTED_FIELDS: Final = (
    "LineId",
    "Date",
    "Time",
    "Pid",
    "Level",
    "Component",
    "Content",
    "EventId",
    "EventTemplate",
)


class HDFSStructuredCSVParser(IParser):
    """Strategy: parse a single CSV row of HDFS structured logs.

    Note: callers feed one CSV row at a time (header is consumed externally).
    Use ``parse_rows`` for batch processing of an entire file.
    """

    @property
    def name(self) -> str:
        return "hdfs_structured_csv"

    def can_parse(self, raw: str) -> bool:
        # First field is a numeric LineId.
        first = raw.split(",", 1)[0]
        return first.isdigit()

    def parse(self, raw: str) -> LogRecord:
        reader = csv.reader(StringIO(raw))
        try:
            row = next(reader)
        except StopIteration as e:
            raise ParserError("empty CSV row") from e

        if len(row) < 9:
            raise ParserError(f"expected >=9 CSV fields, got {len(row)}: {raw[:80]!r}")

        line_id, date, time, pid, level, component, content, event_id, _template = row[:9]

        try:
            timestamp = datetime.strptime(date + time, "%y%m%d%H%M%S").replace(tzinfo=UTC)
        except ValueError as e:
            raise ParserError(f"invalid HDFS timestamp {date}{time}: {e}") from e

        severity = _LEVEL_MAP.get(level, Severity.INFO)
        parse_warning = level not in _LEVEL_MAP

        record = LogRecord(
            timestamp=timestamp,
            source=component,
            severity=severity,
            message=content,
            raw_payload={
                "pid": pid,
                "level_raw": level,
                "line_id": line_id,
                "raw": _reconstruct_raw(date, time, pid, level, component, content),
            },
        )

        block_match = _BLOCK_RE.search(content)
        if block_match:
            record.add_enrichment("block_id", "blk_" + block_match.group(1))

        if event_id:
            record.add_enrichment("event_id", event_id)

        if parse_warning:
            record.add_enrichment("parse_warning", f"unknown level: {level}")

        return record


def _reconstruct_raw(
    date: str, time: str, pid: str, level: str, component: str, content: str
) -> str:
    """Reconstruct the original raw line from its parsed parts.

    This makes ``raw_payload["raw"]`` byte-equal to what ``HDFSParser`` would
    produce, enabling the oracle equality test.
    """
    return f"{date} {time} {pid} {level} {component}: {content}"


def expected_csv_fields() -> tuple[str, ...]:
    return _EXPECTED_FIELDS
