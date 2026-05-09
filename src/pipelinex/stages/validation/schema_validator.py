"""SchemaValidator: enforces required fields and severity allowlist.

This is the first link in the validation chain. Records missing essential
fields (e.g. message text) or carrying disallowed severities are rejected
before downstream stages waste effort on them.
"""

from __future__ import annotations

from collections.abc import Iterable

from pipelinex.core.interfaces import IValidator, ValidationResult
from pipelinex.core.models import LogRecord, Severity


class SchemaValidator(IValidator):
    """Validate that a record has the minimum required shape."""

    def __init__(
        self,
        require_message: bool = True,
        require_source: bool = False,
        allowed_severities: Iterable[Severity] | None = None,
    ) -> None:
        self._require_message = require_message
        self._require_source = require_source
        self._allowed = (
            set(allowed_severities) if allowed_severities is not None else None
        )

    @property
    def name(self) -> str:
        return "schema"

    def validate(self, record: LogRecord) -> ValidationResult:
        if self._require_message and not record.message:
            return ValidationResult(
                valid=False, reason="missing message", validator_name=self.name
            )
        if self._require_source and not record.source:
            return ValidationResult(
                valid=False, reason="missing source", validator_name=self.name
            )
        if self._allowed is not None and record.severity not in self._allowed:
            return ValidationResult(
                valid=False,
                reason=f"severity {record.severity} not in allowlist",
                validator_name=self.name,
            )
        return ValidationResult(valid=True, validator_name=self.name)
