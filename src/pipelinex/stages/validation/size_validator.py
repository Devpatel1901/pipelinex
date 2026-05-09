"""SizeValidator: rejects records whose message exceeds a byte limit.

Defends against pathological inputs (multi-MB log lines from a buggy app)
that would otherwise bloat memory or take long to write to PostgreSQL.
"""

from __future__ import annotations

from pipelinex.core.interfaces import IValidator, ValidationResult
from pipelinex.core.models import LogRecord


class SizeValidator(IValidator):
    """Reject records whose message length (bytes) exceeds a cap."""

    def __init__(self, max_message_bytes: int = 64 * 1024) -> None:
        if max_message_bytes <= 0:
            raise ValueError("max_message_bytes must be > 0")
        self._max = max_message_bytes

    @property
    def name(self) -> str:
        return "size"

    def validate(self, record: LogRecord) -> ValidationResult:
        size = len(record.message.encode("utf-8"))
        if size > self._max:
            return ValidationResult(
                valid=False,
                reason=f"message size {size} > {self._max}",
                validator_name=self.name,
            )
        return ValidationResult(valid=True, validator_name=self.name)
