"""Integration tests for the FastAPI query layer.

These run against an InMemoryLogRepository pre-loaded with fixtures, so
they're fast and don't need docker.
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from pipelinex.api.app import create_app
from pipelinex.core.models import AnomalyEvent, LogRecord, Severity
from pipelinex.persistence.memory_repo import InMemoryLogRepository


@pytest.fixture
async def populated_repo() -> InMemoryLogRepository:
    repo = InMemoryLogRepository()
    base = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
    run_id = uuid4()
    await repo.save_batch(
        [
            LogRecord(
                pipeline_run_id=run_id,
                timestamp=base,
                source="api",
                severity=Severity.INFO,
                message="alpha",
            ),
            LogRecord(
                pipeline_run_id=run_id,
                timestamp=base,
                source="api",
                severity=Severity.ERROR,
                message="beta",
            ),
            LogRecord(
                pipeline_run_id=run_id,
                timestamp=base,
                source="db",
                severity=Severity.INFO,
                message="gamma",
            ),
        ]
    )
    await repo.save_anomaly(
        AnomalyEvent(detector_name="z_score", severity_score=4.2)
    )
    await repo.save_anomaly(
        AnomalyEvent(detector_name="iqr", severity_score=2.1)
    )
    return repo


@pytest.fixture
def client(populated_repo: InMemoryLogRepository) -> TestClient:
    app = create_app(populated_repo)
    return TestClient(app)


class TestHealth:
    def test_health_returns_ok(self, client: TestClient) -> None:
        res = client.get("/health")
        assert res.status_code == 200
        body = res.json()
        assert body["status"] == "ok"
        assert "version" in body


class TestLogs:
    def test_list_all(self, client: TestClient) -> None:
        res = client.get("/logs")
        assert res.status_code == 200
        assert len(res.json()) == 3

    def test_filter_by_severity(self, client: TestClient) -> None:
        res = client.get("/logs?severity=ERROR")
        assert res.status_code == 200
        body = res.json()
        assert len(body) == 1
        assert body[0]["severity"] == "ERROR"

    def test_filter_by_source(self, client: TestClient) -> None:
        res = client.get("/logs?source=db")
        assert res.status_code == 200
        body = res.json()
        assert len(body) == 1
        assert body[0]["source"] == "db"

    def test_limit_param(self, client: TestClient) -> None:
        res = client.get("/logs?limit=2")
        assert res.status_code == 200
        assert len(res.json()) == 2

    def test_invalid_limit_rejected(self, client: TestClient) -> None:
        # The limit cap is 10000 in the schema.
        res = client.get("/logs?limit=999999")
        assert res.status_code == 422


class TestAnomalies:
    def test_list_all(self, client: TestClient) -> None:
        res = client.get("/anomalies")
        assert res.status_code == 200
        body = res.json()
        assert len(body) == 2
        # Sorted by detected_at desc — both have ~now, so order may vary,
        # but both detector names should appear.
        names = {a["detector_name"] for a in body}
        assert names == {"z_score", "iqr"}

    def test_filter_by_detector(self, client: TestClient) -> None:
        res = client.get("/anomalies?detector=z_score")
        assert res.status_code == 200
        body = res.json()
        assert len(body) == 1
        assert body[0]["detector_name"] == "z_score"


class TestAggregate:
    def test_count_by_severity(self, client: TestClient) -> None:
        res = client.get("/aggregate?group_by=severity")
        assert res.status_code == 200
        body = res.json()
        rows = {row["key"]: row["count"] for row in body}
        assert rows == {"INFO": 2, "ERROR": 1}

    def test_count_by_source(self, client: TestClient) -> None:
        res = client.get("/aggregate?group_by=source")
        assert res.status_code == 200
        rows = {row["key"]: row["count"] for row in res.json()}
        assert rows == {"api": 2, "db": 1}

    def test_unknown_group_by_rejected(self, client: TestClient) -> None:
        res = client.get("/aggregate?group_by=garbage")
        assert res.status_code == 422


class TestMetrics:
    def test_returns_snapshot(self, client: TestClient) -> None:
        res = client.get("/metrics")
        assert res.status_code == 200
        body = res.json()
        assert "counters" in body
        assert "gauges" in body
        assert "timers" in body


class TestRepositoryDep:
    def test_missing_repository_returns_503(self) -> None:
        from fastapi import FastAPI

        from pipelinex.api.routes import router

        app = FastAPI()
        app.include_router(router)
        client = TestClient(app, raise_server_exceptions=False)
        res = client.get("/logs")
        assert res.status_code == 503
