"""Oracle test: HDFSParser must agree with the structured-CSV reference.

The Loghub HDFS_v1 dataset ships a pre-parsed structured CSV (one row per
log line) alongside the raw log. We parse the raw 2k sample with
``HDFSParser`` and the corresponding CSV rows with
``HDFSStructuredCSVParser``, then assert every field matches.

This is a regression-quality test: any future change to the regex, severity
mapping, or block-id extraction in either parser will surface here.

The plan requires ≥99.9% coverage. The 2k sample has 2000 lines, so we allow
at most 2 mismatches (covered by the COVERAGE_THRESHOLD constant).
"""

from __future__ import annotations

import csv
from pathlib import Path

import pytest

from pipelinex.core.models import LogRecord
from pipelinex.stages.parsing.hdfs_parser import HDFSParser
from pipelinex.stages.parsing.hdfs_structured_csv_parser import HDFSStructuredCSVParser

REPO_ROOT = Path(__file__).resolve().parents[2]
RAW_LOG = REPO_ROOT / "HDFS" / "HDFS_2k.log"
STRUCT_CSV = REPO_ROOT / "HDFS" / "HDFS_2k.log_structured.csv"

COVERAGE_THRESHOLD = 0.999  # 99.9% per the plan


@pytest.fixture(scope="module")
def raw_lines() -> list[str]:
    if not RAW_LOG.exists():
        pytest.skip(f"raw 2k sample missing: {RAW_LOG}")
    return [
        line.rstrip("\n")
        for line in RAW_LOG.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


@pytest.fixture(scope="module")
def csv_rows() -> list[dict[str, str]]:
    if not STRUCT_CSV.exists():
        pytest.skip(f"structured CSV missing: {STRUCT_CSV}")
    with STRUCT_CSV.open(encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        return list(reader)


def _records_match(a: LogRecord, b: LogRecord) -> bool:
    """Field-by-field comparison ignoring identity (id, run_id) and any
    fields uniquely produced by one parser (e.g., line_id, event_id).
    """
    if a.timestamp != b.timestamp:
        return False
    if a.severity != b.severity:
        return False
    if a.source != b.source:
        return False
    if a.message != b.message:
        return False
    if a.raw_payload.get("pid") != b.raw_payload.get("pid"):
        return False
    return a.enrichment.get("block_id") == b.enrichment.get("block_id")


class TestOracle:
    def test_line_count_matches(
        self, raw_lines: list[str], csv_rows: list[dict[str, str]]
    ) -> None:
        # Both should describe the same set of log lines.
        assert len(raw_lines) == len(csv_rows), (
            f"raw lines={len(raw_lines)} vs csv rows={len(csv_rows)}"
        )

    def test_parsers_agree_on_each_line(
        self, raw_lines: list[str], csv_rows: list[dict[str, str]]
    ) -> None:
        hdfs = HDFSParser()
        struct = HDFSStructuredCSVParser()

        matches = 0
        mismatches: list[tuple[int, str]] = []

        for i, (raw, row) in enumerate(zip(raw_lines, csv_rows, strict=True)):
            try:
                a = hdfs.parse(raw)
            except Exception as e:
                mismatches.append((i, f"HDFSParser failed: {e}"))
                continue

            csv_line = (
                f"{row['LineId']},{row['Date']},{row['Time']},{row['Pid']},"
                f"{row['Level']},{row['Component']},{_quote_csv(row['Content'])},"
                f"{row['EventId']},{_quote_csv(row['EventTemplate'])}"
            )

            try:
                b = struct.parse(csv_line)
            except Exception as e:
                mismatches.append((i, f"HDFSStructuredCSVParser failed: {e}"))
                continue

            if _records_match(a, b):
                matches += 1
            else:
                mismatches.append(
                    (
                        i,
                        f"mismatch:\n  raw: ts={a.timestamp} sev={a.severity} "
                        f"src={a.source} msg={a.message[:60]} "
                        f"blk={a.enrichment.get('block_id')}\n"
                        f"  csv: ts={b.timestamp} sev={b.severity} "
                        f"src={b.source} msg={b.message[:60]} "
                        f"blk={b.enrichment.get('block_id')}",
                    )
                )

        coverage = matches / len(raw_lines)
        threshold_str = f"{COVERAGE_THRESHOLD * 100:.1f}%"
        sample = "\n".join(f"  line {i}: {msg}" for i, msg in mismatches[:5])
        assert coverage >= COVERAGE_THRESHOLD, (
            f"oracle coverage {coverage * 100:.2f}% below threshold {threshold_str}"
            f" ({matches}/{len(raw_lines)} matched). First mismatches:\n{sample}"
        )


def _quote_csv(field: str) -> str:
    """Re-quote a CSV field that DictReader has already unescaped."""
    if "," in field or '"' in field or "\n" in field:
        escaped = field.replace('"', '""')
        return f'"{escaped}"'
    return field
