"""RateLimitValidator: caps records-per-second per source.

Implements a token-bucket rate limit keyed by ``record.source``. Useful for
demos showing protection against a misbehaving log producer flooding the
pipeline.
"""

from __future__ import annotations

import time
from collections import defaultdict
from collections.abc import Callable

from pipelinex.core.interfaces import IValidator, ValidationResult
from pipelinex.core.models import LogRecord


class RateLimitValidator(IValidator):
    """Token-bucket rate limiter, per source."""

    def __init__(
        self,
        capacity: int = 1000,
        refill_per_sec: float = 1000.0,
        clock: Callable[[], float] | None = None,
    ) -> None:
        if capacity <= 0:
            raise ValueError("capacity must be > 0")
        if refill_per_sec <= 0:
            raise ValueError("refill_per_sec must be > 0")
        self._capacity = capacity
        self._refill_per_sec = refill_per_sec
        self._clock = clock or time.monotonic
        self._buckets: dict[str, tuple[float, float]] = defaultdict(
            lambda: (float(capacity), self._clock())
        )

    @property
    def name(self) -> str:
        return "rate_limit"

    def validate(self, record: LogRecord) -> ValidationResult:
        key = record.source or "<unknown>"
        now = self._clock()
        tokens, last = self._buckets[key]

        elapsed = max(0.0, now - last)
        tokens = min(self._capacity, tokens + elapsed * self._refill_per_sec)

        if tokens >= 1.0:
            self._buckets[key] = (tokens - 1.0, now)
            return ValidationResult(valid=True, validator_name=self.name)

        self._buckets[key] = (tokens, now)
        return ValidationResult(
            valid=False,
            reason=f"rate limit exceeded for {key}",
            validator_name=self.name,
        )
