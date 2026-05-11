"""Pydantic schemas for the FastAPI query layer.

Separate from the domain models in ``core/models.py`` because the API
contract is a different concern than the in-process types — we want to be
able to evolve them independently. Pydantic also gives us automatic
OpenAPI / JSON Schema export for free.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from pipelinex.core.models import LogRecord


class LogRecordOut(BaseModel):
    """Public projection of a ``LogRecord``."""

    model_config = ConfigDict(extra="forbid")

    id: UUID
    pipeline_run_id: UUID | None = None
    timestamp: datetime | None = None
    source: str = ""
    severity: str = "INFO"
    message: str = ""
    raw_payload: dict[str, Any] = Field(default_factory=dict)
    enrichment: dict[str, Any] = Field(default_factory=dict)
    stage_history: list[str] = Field(default_factory=list)

    @classmethod
    def from_record(cls, record: LogRecord) -> LogRecordOut:
        return cls(
            id=record.id,
            pipeline_run_id=record.pipeline_run_id,
            timestamp=record.timestamp,
            source=record.source,
            severity=str(record.severity),
            message=record.message,
            raw_payload=dict(record.raw_payload),
            enrichment=dict(record.enrichment),
            stage_history=list(record.stage_history),
        )


class AnomalyOut(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: UUID
    log_record_id: UUID
    detector_name: str
    severity_score: float
    detected_at: datetime
    metadata: dict[str, Any] = Field(default_factory=dict)


class AggregateRow(BaseModel):
    model_config = ConfigDict(extra="forbid")

    key: str
    count: int


class HealthOut(BaseModel):
    status: str = "ok"
    version: str
