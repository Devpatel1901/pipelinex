"""ValidationStage: runs a chain of validators against each record.

Chain of Responsibility — the stage walks the validators in order and
short-circuits at the first failure. Failed records are dropped (raised as
``ValidationError`` so the pipeline executor counts them as failures).

Validator order matters: cheap checks should come first, so expensive ones
(rate-limit lookup, schema validation against an external registry) only
run on records that have already cleared the cheap ones.
"""

from __future__ import annotations

from collections.abc import Sequence

from pipelinex.core.exceptions import ValidationError
from pipelinex.core.interfaces import IPipelineStage, IValidator
from pipelinex.core.models import LogRecord


class ValidationStage(IPipelineStage):
    """Run a chain of ``IValidator``s, short-circuiting on the first failure."""

    def __init__(self, validators: Sequence[IValidator]) -> None:
        if not validators:
            raise ValueError("ValidationStage requires at least one validator")
        self._validators = tuple(validators)

    @property
    def name(self) -> str:
        names = ",".join(v.name for v in self._validators)
        return f"validate:{names}"

    @property
    def validators(self) -> tuple[IValidator, ...]:
        return self._validators

    async def process(self, record: LogRecord) -> LogRecord:
        for validator in self._validators:
            result = validator.validate(record)
            if not result.valid:
                raise ValidationError(
                    f"validator {result.validator_name} rejected record: {result.reason}"
                )
        return record
