"""Core domain models for PipelineX.

These are the data types that flow through the pipeline. Every stage receives
and returns one of these. The mutability decisions matter:

- LogRecord is mutable (frozen=False) — stages enrich it in place; copying at
  every stage would be O(stages * record_size) memory allocation.
- AnomalyEvent and BlockTrace are immutable (frozen=True) — they describe
  facts about the past and are never modified once produced.

The BlockTrace contains record_ids (UUIDs), NOT raw LogRecord references.
At ~575K HDFS blocks * ~19 lines each, holding full records would pin 11M
LogRecords in RAM. Detectors that need full records must query the repository.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Literal
from uuid import UUID, uuid4


class Severity(StrEnum):
    """Log severity levels, ordered from least to most critical."""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


def _utcnow() -> datetime:
    return datetime.now(UTC)


@dataclass
class LogRecord:
    """A single log line as it flows through the pipeline.

    Mutable by design: each stage adds fields to ``enrichment`` and appends to
    ``stage_history``. Stages must not retain references after returning — the
    next stage may mutate the record.
    """

    id: UUID = field(default_factory=uuid4)
    pipeline_run_id: UUID | None = None

    timestamp: datetime | None = None
    source: str = ""
    severity: Severity = Severity.INFO
    message: str = ""

    raw_payload: dict[str, Any] = field(default_factory=dict)
    enrichment: dict[str, Any] = field(default_factory=dict)

    stage_history: list[str] = field(default_factory=list)

    def add_enrichment(self, key: str, value: Any) -> None:
        self.enrichment[key] = value

    def mark_stage(self, stage_name: str) -> None:
        self.stage_history.append(stage_name)


@dataclass(frozen=True)
class AnomalyEvent:
    """A detected anomaly. Immutable — anomalies are facts about the past."""

    id: UUID = field(default_factory=uuid4)
    log_record_id: UUID = field(default_factory=uuid4)
    detector_name: str = ""
    severity_score: float = 0.0
    detected_at: datetime = field(default_factory=_utcnow)
    metadata: dict[str, Any] = field(default_factory=dict)


ClosedReason = Literal["terminal", "timeout", "shutdown", "evicted"]


@dataclass(frozen=True)
class BlockTrace:
    """A grouped sequence of events for one HDFS block.

    Stores ``record_ids`` only, not full ``LogRecord``s. Detectors that need
    raw records must query the repository by these ids. This keeps the
    sessionizer's open-trace state small enough to handle the full HDFS_v1
    corpus (~575K blocks) on a laptop.
    """

    block_id: str
    event_sequence: tuple[str, ...]
    record_ids: tuple[UUID, ...]
    first_timestamp: datetime
    last_timestamp: datetime
    record_count: int
    pipeline_run_id: UUID
    closed_reason: ClosedReason


@dataclass(frozen=True)
class PipelineRunResult:
    """Summary returned by ``PipelineExecutor.run()``."""

    run_id: UUID
    records_processed: int
    records_failed: int
    started_at: datetime
    completed_at: datetime
    anomalies_detected: int = 0
