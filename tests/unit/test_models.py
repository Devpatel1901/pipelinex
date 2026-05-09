"""Unit tests for core domain models."""

from __future__ import annotations

import dataclasses
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from pipelinex.core.models import (
    AnomalyEvent,
    BlockTrace,
    LogRecord,
    PipelineRunResult,
    Severity,
)


class TestLogRecord:
    def test_default_construction(self) -> None:
        record = LogRecord()
        assert record.id is not None
        assert record.severity is Severity.INFO
        assert record.raw_payload == {}
        assert record.enrichment == {}
        assert record.stage_history == []

    def test_each_record_has_unique_id(self) -> None:
        a = LogRecord()
        b = LogRecord()
        assert a.id != b.id

    def test_add_enrichment_mutates_in_place(self) -> None:
        record = LogRecord()
        record.add_enrichment("block_id", "blk_123")
        record.add_enrichment("event_id", "E10")
        assert record.enrichment == {"block_id": "blk_123", "event_id": "E10"}

    def test_mark_stage_appends_to_history(self) -> None:
        record = LogRecord()
        record.mark_stage("parser")
        record.mark_stage("validator")
        record.mark_stage("enrichment")
        assert record.stage_history == ["parser", "validator", "enrichment"]

    def test_record_is_mutable_by_design(self) -> None:
        # The plan calls out: LogRecord is mutable for performance.
        # This test is the literal specification — it must pass.
        record = LogRecord(message="original")
        record.message = "mutated"
        assert record.message == "mutated"


class TestAnomalyEvent:
    def test_default_construction(self) -> None:
        event = AnomalyEvent(detector_name="z_score", severity_score=3.5)
        assert event.detector_name == "z_score"
        assert event.severity_score == 3.5
        assert isinstance(event.detected_at, datetime)

    def test_is_immutable(self) -> None:
        # The plan calls out: AnomalyEvent is frozen — facts about the past.
        event = AnomalyEvent(detector_name="z_score")
        with pytest.raises(dataclasses.FrozenInstanceError):
            event.detector_name = "iqr"  # type: ignore[misc]


class TestBlockTrace:
    def _make_trace(self, **overrides: object) -> BlockTrace:
        base = {
            "block_id": "blk_-1608999687919862906",
            "event_sequence": ("E22", "E5", "E11", "E9"),
            "record_ids": (uuid4(), uuid4(), uuid4(), uuid4()),
            "first_timestamp": datetime(2008, 11, 9, 20, 36, 15, tzinfo=UTC),
            "last_timestamp": datetime(2008, 11, 9, 20, 38, 15, tzinfo=UTC),
            "record_count": 4,
            "pipeline_run_id": uuid4(),
            "closed_reason": "terminal",
        }
        base.update(overrides)
        return BlockTrace(**base)  # type: ignore[arg-type]

    def test_construction(self) -> None:
        trace = self._make_trace()
        assert trace.block_id == "blk_-1608999687919862906"
        assert len(trace.event_sequence) == 4
        assert trace.record_count == 4
        assert trace.closed_reason == "terminal"

    def test_is_immutable(self) -> None:
        trace = self._make_trace()
        with pytest.raises(dataclasses.FrozenInstanceError):
            trace.block_id = "blk_other"  # type: ignore[misc]

    def test_holds_record_ids_not_records(self) -> None:
        # Critical OOM-prevention decision documented in the plan: BlockTrace
        # stores UUIDs, not LogRecord references. This test pins it.
        trace = self._make_trace()
        for rid in trace.record_ids:
            assert not isinstance(rid, LogRecord)


class TestSeverity:
    def test_string_enum(self) -> None:
        assert Severity.INFO == "INFO"
        assert Severity.CRITICAL.value == "CRITICAL"

    def test_all_levels_present(self) -> None:
        names = {s.name for s in Severity}
        assert names == {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}


class TestPipelineRunResult:
    def test_construction(self) -> None:
        now = datetime.now(UTC)
        result = PipelineRunResult(
            run_id=uuid4(),
            records_processed=1000,
            records_failed=2,
            started_at=now,
            completed_at=now,
            anomalies_detected=15,
        )
        assert result.records_processed == 1000
        assert result.anomalies_detected == 15
