"""Core interfaces (abstract base classes) for PipelineX.

This module is the contract surface of the system. Every concrete class
implements one of these interfaces. The pipeline core depends only on these
abstractions — that's the Dependency Inversion Principle made physical.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from pipelinex.core.models import AnomalyEvent, BlockTrace, LogRecord

# ---------- Pipeline stage ----------


class IPipelineStage(ABC):
    """Every stage in the pipeline implements this contract."""

    @abstractmethod
    async def process(self, record: LogRecord) -> LogRecord:
        """Process a record and return it (or a transformed copy)."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable stage name (used in logs and stage_history)."""


# ---------- Parsers ----------


class IParser(ABC):
    """Strategy: parse a raw line into a LogRecord."""

    @abstractmethod
    def parse(self, raw: str) -> LogRecord:
        """Parse one line. Raise ParserError on unrecoverable failure."""

    @abstractmethod
    def can_parse(self, raw: str) -> bool:
        """Cheap shape check — does this parser claim this line format?"""

    @property
    @abstractmethod
    def name(self) -> str: ...


# ---------- Validators (Chain of Responsibility) ----------


@dataclass(frozen=True)
class ValidationResult:
    """Outcome of a single validator's check."""

    valid: bool
    reason: str = ""
    validator_name: str = ""


class IValidator(ABC):
    """One link in the validation chain."""

    @abstractmethod
    def validate(self, record: LogRecord) -> ValidationResult: ...

    @property
    @abstractmethod
    def name(self) -> str: ...


# ---------- Detectors ----------


class IAnomalyDetector(ABC):
    """Strategy: detect anomalies in a stream of records (point/numeric)."""

    @abstractmethod
    async def detect(self, record: LogRecord) -> AnomalyEvent | None:
        """Return an AnomalyEvent if this record is anomalous, else None."""

    @abstractmethod
    def reset(self) -> None:
        """Clear sliding-window / cumulative state. Used between runs."""

    @property
    @abstractmethod
    def name(self) -> str: ...


class ISequenceAnomalyDetector(ABC):
    """Strategy: detect anomalies in a closed BlockTrace (sequence-based).

    Sibling of IAnomalyDetector. Trace-based detection has a fundamentally
    different shape than per-record detection — the input is a closed
    sequence, not a stream. Subscribers wire to BlockTraceClosed events on
    the EventBus rather than running inline in the pipeline.
    """

    @abstractmethod
    async def detect_trace(self, trace: BlockTrace) -> AnomalyEvent | None: ...

    @property
    @abstractmethod
    def name(self) -> str: ...


# ---------- Repository ----------


@dataclass(frozen=True)
class QueryFilters:
    """Filters for ``ILogRepository.query()``."""

    severity: str | None = None
    source: str | None = None
    pipeline_run_id: str | None = None
    after: datetime | None = None
    before: datetime | None = None
    limit: int = 1000


@dataclass(frozen=True)
class Aggregation:
    """Aggregation request for ``ILogRepository.aggregate()``."""

    group_by: str  # e.g., "severity", "source"
    metric: str = "count"  # currently supports "count"


@dataclass(frozen=True)
class AggregateResult:
    """Result of an aggregation."""

    rows: list[dict[str, Any]] = field(default_factory=list)


class ILogRepository(ABC):
    """Persistence abstraction. PostgreSQL and InMemory implementations both
    pass the same test suite (Liskov substitution as a hard guarantee).
    """

    @abstractmethod
    async def save(self, record: LogRecord) -> None: ...

    @abstractmethod
    async def save_batch(self, records: list[LogRecord]) -> None: ...

    @abstractmethod
    async def save_trace(self, trace: BlockTrace) -> None: ...

    @abstractmethod
    async def save_trace_batch(self, traces: list[BlockTrace]) -> None: ...

    @abstractmethod
    async def save_anomaly(self, anomaly: AnomalyEvent) -> None: ...

    @abstractmethod
    async def query(self, filters: QueryFilters) -> list[LogRecord]: ...

    @abstractmethod
    async def aggregate(self, agg: Aggregation) -> AggregateResult: ...


# ---------- Sources ----------


class ILogSource(ABC):
    """Async iterator of raw log lines."""

    @abstractmethod
    def stream(self) -> AsyncIterator[str]: ...


# ---------- Events / Listeners ----------


class IEventListener(ABC):
    """Observer: react to events on the EventBus."""

    @abstractmethod
    async def on_event(self, event: Any) -> None: ...

    @property
    @abstractmethod
    def name(self) -> str: ...
