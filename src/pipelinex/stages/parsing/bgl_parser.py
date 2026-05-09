"""BGL (Blue Gene/L) supercomputer log parser.

Parses raw BGL log lines like::

    - 1117838570 2005.06.03 R02-M1-N0-C:J12-U11 2005-06-03-15.42.50.675872 R02-M1-N0-C:J12-U11 RAS KERNEL INFO instruction cache parity error corrected

Format (whitespace-separated, free-form tail)::

    Label Timestamp Date Node Time NodeRepeat Type Component Level Content...

The first field is the **per-line ground-truth label**: ``-`` for normal,
otherwise an alert category code (e.g. ``KERNDTLB``, ``APPREAD``,
``KERNSTOR``, ``APPCHILD``). This makes BGL the natural evaluation dataset
for the point-anomaly detectors (Z-Score, IQR, CUSUM).

We use ``str.split(maxsplit=9)`` rather than a fat regex because the
content tail is free-form and may contain colons, slashes, equals signs,
and IPs. Splitting up to a fixed column count is both simpler and faster.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Final

from pipelinex.core.exceptions import ParserError
from pipelinex.core.interfaces import IParser
from pipelinex.core.models import LogRecord, Severity

_LEVEL_MAP: Final[dict[str, Severity]] = {
    "DEBUG": Severity.DEBUG,
    "INFO": Severity.INFO,
    "WARN": Severity.WARNING,
    "WARNING": Severity.WARNING,
    "ERROR": Severity.ERROR,
    "FATAL": Severity.CRITICAL,
    "SEVERE": Severity.CRITICAL,
    "FAILURE": Severity.CRITICAL,
}


class BGLParser(IParser):
    """Strategy: parse BGL log lines."""

    @property
    def name(self) -> str:
        return "bgl"

    def can_parse(self, raw: str) -> bool:
        if not raw:
            return False
        first = raw.split(maxsplit=1)[0]
        # Either '-' (normal) or an all-uppercase alert code.
        return first == "-" or (first.isupper() and first.isalpha())

    def parse(self, raw: str) -> LogRecord:
        parts = raw.split(maxsplit=9)
        if len(parts) < 10:
            raise ParserError(f"BGL line has only {len(parts)} fields: {raw[:80]!r}")

        (
            label,
            _epoch,
            _date_simple,
            node,
            time_str,
            node_repeat,
            log_type,
            component,
            level,
            content,
        ) = parts

        try:
            timestamp = _parse_bgl_time(time_str)
        except ValueError as e:
            raise ParserError(f"invalid BGL timestamp {time_str!r}: {e}") from e

        severity = _LEVEL_MAP.get(level, Severity.INFO)
        parse_warning = level not in _LEVEL_MAP

        record = LogRecord(
            timestamp=timestamp,
            source=component,
            severity=severity,
            message=content,
            raw_payload={
                "type": log_type,
                "level_raw": level,
                "node_repeat": node_repeat,
                "raw": raw,
            },
        )
        record.add_enrichment("bgl_label", label)
        record.add_enrichment("node", node)

        if parse_warning:
            record.add_enrichment("parse_warning", f"unknown level: {level}")

        return record


def _parse_bgl_time(time_str: str) -> datetime:
    """Parse the detailed BGL timestamp ``2005-06-03-15.42.50.675872``."""
    return datetime.strptime(time_str, "%Y-%m-%d-%H.%M.%S.%f").replace(tzinfo=UTC)
