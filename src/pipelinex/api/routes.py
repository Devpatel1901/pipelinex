"""FastAPI routes for the query layer.

The router accepts a single ``ILogRepository`` dependency-injected via the
app's state object. This keeps the API agnostic of which repository
implementation is in use (memory vs postgres) and makes integration
testing trivial — just point the app at an InMemoryLogRepository
pre-loaded with fixtures.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from pipelinex import __version__
from pipelinex.api.schemas import (
    AggregateRow,
    AnomalyOut,
    HealthOut,
    LogRecordOut,
)
from pipelinex.core.interfaces import (
    Aggregation,
    ILogRepository,
    QueryFilters,
)
from pipelinex.observability.metrics import default_registry

router = APIRouter()


def get_repository(request: Request) -> ILogRepository:
    repo = getattr(request.app.state, "repository", None)
    if repo is None:
        raise HTTPException(status_code=503, detail="repository not configured")
    if not isinstance(repo, ILogRepository):
        raise HTTPException(status_code=503, detail="repository misconfigured")
    return repo


RepoDep = Annotated[ILogRepository, Depends(get_repository)]


@router.get("/health", response_model=HealthOut, tags=["meta"])
async def health() -> HealthOut:
    return HealthOut(status="ok", version=__version__)


@router.get("/logs", response_model=list[LogRecordOut], tags=["logs"])
async def list_logs(
    repo: RepoDep,
    severity: str | None = None,
    source: str | None = None,
    pipeline_run_id: str | None = None,
    after: datetime | None = None,
    before: datetime | None = None,
    limit: int = Query(100, ge=1, le=10_000),
) -> list[LogRecordOut]:
    filters = QueryFilters(
        severity=severity,
        source=source,
        pipeline_run_id=pipeline_run_id,
        after=after,
        before=before,
        limit=limit,
    )
    records = await repo.query(filters)
    return [LogRecordOut.from_record(r) for r in records]


@router.get("/anomalies", response_model=list[AnomalyOut], tags=["anomalies"])
async def list_anomalies(
    repo: RepoDep,
    detector: str | None = None,
    limit: int = Query(100, ge=1, le=10_000),
) -> list[AnomalyOut]:
    all_fn = getattr(repo, "all_anomalies", None)
    if all_fn is None:
        raise HTTPException(
            status_code=501, detail="this repository does not expose anomalies"
        )
    anomalies = await all_fn()
    if detector is not None:
        anomalies = [a for a in anomalies if a.detector_name == detector]
    anomalies.sort(key=lambda a: a.detected_at, reverse=True)
    return [
        AnomalyOut(
            id=a.id,
            log_record_id=a.log_record_id,
            detector_name=a.detector_name,
            severity_score=a.severity_score,
            detected_at=a.detected_at,
            metadata=dict(a.metadata),
        )
        for a in anomalies[:limit]
    ]


@router.get("/aggregate", response_model=list[AggregateRow], tags=["logs"])
async def aggregate(
    repo: RepoDep,
    group_by: str = Query("severity", pattern="^(severity|source)$"),
) -> list[AggregateRow]:
    result = await repo.aggregate(Aggregation(group_by=group_by))
    return [
        AggregateRow(key=str(row[group_by]), count=int(row["count"]))
        for row in result.rows
    ]


@router.get("/metrics", tags=["meta"])
async def metrics() -> dict[str, object]:
    """Expose the in-process metrics registry as JSON.

    Not Prometheus format on purpose — we don't want a hard dep on a
    metric backend, and the JSON output is fine for an interview demo.
    """
    return default_registry.snapshot()
