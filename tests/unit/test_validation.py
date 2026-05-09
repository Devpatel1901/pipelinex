"""Tests for validators and the ValidationStage chain."""

from __future__ import annotations

import pytest

from pipelinex.core.exceptions import ValidationError
from pipelinex.core.interfaces import IValidator, ValidationResult
from pipelinex.core.models import LogRecord, Severity
from pipelinex.stages.validation.chain import ValidationStage
from pipelinex.stages.validation.rate_limit_validator import RateLimitValidator
from pipelinex.stages.validation.schema_validator import SchemaValidator
from pipelinex.stages.validation.size_validator import SizeValidator


class _NoopValidator(IValidator):
    @property
    def name(self) -> str:
        return "noop"

    def validate(self, record: LogRecord) -> ValidationResult:
        return ValidationResult(valid=True, validator_name=self.name)


class _RejectAll(IValidator):
    @property
    def name(self) -> str:
        return "reject"

    def validate(self, record: LogRecord) -> ValidationResult:
        return ValidationResult(
            valid=False, reason="always rejects", validator_name=self.name
        )


class TestSchemaValidator:
    def test_accepts_complete_record(self) -> None:
        rec = LogRecord(message="hello", source="api")
        result = SchemaValidator().validate(rec)
        assert result.valid

    def test_rejects_missing_message(self) -> None:
        rec = LogRecord(message="", source="api")
        result = SchemaValidator(require_message=True).validate(rec)
        assert not result.valid
        assert "message" in result.reason

    def test_rejects_disallowed_severity(self) -> None:
        rec = LogRecord(message="x", severity=Severity.DEBUG)
        result = SchemaValidator(allowed_severities=[Severity.INFO]).validate(rec)
        assert not result.valid


class TestSizeValidator:
    def test_accepts_small_message(self) -> None:
        rec = LogRecord(message="x" * 100)
        result = SizeValidator(max_message_bytes=200).validate(rec)
        assert result.valid

    def test_rejects_huge_message(self) -> None:
        rec = LogRecord(message="x" * 10_000)
        result = SizeValidator(max_message_bytes=1000).validate(rec)
        assert not result.valid

    def test_rejects_zero_cap(self) -> None:
        with pytest.raises(ValueError, match="max_message_bytes"):
            SizeValidator(max_message_bytes=0)


class TestRateLimitValidator:
    def test_first_record_passes(self) -> None:
        clock = [0.0]
        v = RateLimitValidator(capacity=2, refill_per_sec=1.0, clock=lambda: clock[0])
        rec = LogRecord(source="app")
        assert v.validate(rec).valid

    def test_burst_within_capacity_passes(self) -> None:
        clock = [0.0]
        v = RateLimitValidator(capacity=3, refill_per_sec=1.0, clock=lambda: clock[0])
        rec = LogRecord(source="app")
        assert v.validate(rec).valid
        assert v.validate(rec).valid
        assert v.validate(rec).valid

    def test_burst_exceeding_capacity_is_rejected(self) -> None:
        clock = [0.0]
        v = RateLimitValidator(capacity=2, refill_per_sec=0.001, clock=lambda: clock[0])
        rec = LogRecord(source="app")
        v.validate(rec)
        v.validate(rec)
        assert not v.validate(rec).valid

    def test_refill_after_time_passes(self) -> None:
        clock = [0.0]
        v = RateLimitValidator(capacity=1, refill_per_sec=10.0, clock=lambda: clock[0])
        rec = LogRecord(source="app")
        v.validate(rec)
        # Bucket empty.
        assert not v.validate(rec).valid
        # Advance clock by 0.5s → 5 tokens refilled, capped at 1.
        clock[0] = 0.5
        assert v.validate(rec).valid

    def test_separate_sources_have_separate_buckets(self) -> None:
        clock = [0.0]
        v = RateLimitValidator(capacity=1, refill_per_sec=0.001, clock=lambda: clock[0])
        a = LogRecord(source="A")
        b = LogRecord(source="B")
        assert v.validate(a).valid
        assert v.validate(b).valid


class TestValidationStage:
    async def test_passes_when_all_validators_accept(self) -> None:
        stage = ValidationStage([_NoopValidator(), _NoopValidator()])
        rec = LogRecord(message="hi")
        out = await stage.process(rec)
        assert out is rec

    async def test_short_circuits_on_first_rejection(self) -> None:
        # A rejector in position 1 should run before the second one is even
        # consulted. Use a subclass that records calls.
        class _Counting(IValidator):
            def __init__(self, name: str) -> None:
                self._name = name
                self.calls = 0

            @property
            def name(self) -> str:
                return self._name

            def validate(self, record: LogRecord) -> ValidationResult:
                self.calls += 1
                return ValidationResult(valid=True, validator_name=self._name)

        first = _Counting("first")
        second = _Counting("second")
        stage = ValidationStage([first, _RejectAll(), second])

        with pytest.raises(ValidationError, match="reject"):
            await stage.process(LogRecord(message="hi"))

        assert first.calls == 1
        assert second.calls == 0  # short-circuited

    async def test_realistic_chain(self) -> None:
        # Schema → Size → RateLimit, all passing.
        stage = ValidationStage(
            [
                SchemaValidator(require_message=True),
                SizeValidator(max_message_bytes=1024),
                RateLimitValidator(capacity=10, refill_per_sec=10.0),
            ]
        )
        rec = LogRecord(message="hello", source="api")
        out = await stage.process(rec)
        assert out is rec

    async def test_empty_validator_list_rejected(self) -> None:
        with pytest.raises(ValueError, match="at least one"):
            ValidationStage([])
