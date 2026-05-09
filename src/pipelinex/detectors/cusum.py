"""CUSUM (Cumulative Sum) drift detector.

CUSUM tracks the cumulative deviation of a stream from a target mean and
fires when that deviation exceeds a threshold — catching gradual drift that
point detectors (Z-Score, IQR) miss.

The classic two-sided form maintains both an upper and a lower CUSUM:

    S_high = max(0, S_high_prev + (x - target) - slack)
    S_low  = max(0, S_low_prev  + (target - x) - slack)
    fire if S_high > threshold or S_low > threshold

After a fire, both sums are reset to 0 (the "alarm and reset" convention)
so a second fire signals fresh drift, not stale accumulation.

Why this matters for the project
--------------------------------
CUSUM is used in real semiconductor process control — exactly KLA's
domain. Including it lets the interview narrative connect "different
mathematical foundations" directly to a real industrial use case.
"""

from __future__ import annotations

from collections import deque
from typing import Final

from pipelinex.core.interfaces import IAnomalyDetector
from pipelinex.core.models import AnomalyEvent, LogRecord

DEFAULT_SLACK: Final = 0.5  # in stddev units
DEFAULT_THRESHOLD: Final = 5.0
DEFAULT_WARMUP: Final = 30


class CUSUMDetector(IAnomalyDetector):
    """Two-sided CUSUM with auto-calibrated target/sigma during warmup."""

    def __init__(
        self,
        metric_key: str = "metric",
        threshold: float = DEFAULT_THRESHOLD,
        slack: float = DEFAULT_SLACK,
        warmup_samples: int = DEFAULT_WARMUP,
        source_dict: str = "enrichment",
    ) -> None:
        if threshold <= 0:
            raise ValueError("threshold must be > 0")
        if slack < 0:
            raise ValueError("slack must be >= 0")
        if warmup_samples < 2:
            raise ValueError("warmup_samples must be >= 2")
        if source_dict not in {"enrichment", "raw_payload"}:
            raise ValueError("source_dict must be 'enrichment' or 'raw_payload'")

        self._metric_key = metric_key
        self._threshold = threshold
        self._slack = slack
        self._warmup_samples = warmup_samples
        self._source_dict = source_dict
        self._warmup: deque[float] = deque(maxlen=warmup_samples)
        self._target: float | None = None
        self._sigma: float | None = None
        self._s_high = 0.0
        self._s_low = 0.0

    @property
    def name(self) -> str:
        return "cusum"

    def reset(self) -> None:
        self._warmup.clear()
        self._target = None
        self._sigma = None
        self._s_high = 0.0
        self._s_low = 0.0

    async def detect(self, record: LogRecord) -> AnomalyEvent | None:
        bag = record.enrichment if self._source_dict == "enrichment" else record.raw_payload
        raw = bag.get(self._metric_key)
        if not isinstance(raw, (int, float)) or isinstance(raw, bool):
            return None
        value = float(raw)

        # Warmup: collect samples to estimate target and sigma.
        if self._target is None or self._sigma is None:
            self._warmup.append(value)
            if len(self._warmup) < self._warmup_samples:
                return None
            self._target = sum(self._warmup) / len(self._warmup)
            mean = self._target
            variance = sum((x - mean) ** 2 for x in self._warmup) / len(self._warmup)
            self._sigma = max(variance**0.5, 1e-12)
            return None

        target = self._target
        sigma = self._sigma
        deviation = (value - target) / sigma
        slack = self._slack

        self._s_high = max(0.0, self._s_high + deviation - slack)
        self._s_low = max(0.0, self._s_low - deviation - slack)

        if self._s_high > self._threshold or self._s_low > self._threshold:
            direction = "up" if self._s_high > self._threshold else "down"
            score = max(self._s_high, self._s_low)
            event = AnomalyEvent(
                log_record_id=record.id,
                detector_name=self.name,
                severity_score=score,
                metadata={
                    "metric_key": self._metric_key,
                    "value": value,
                    "target": target,
                    "sigma": sigma,
                    "s_high": self._s_high,
                    "s_low": self._s_low,
                    "direction": direction,
                    "threshold": self._threshold,
                },
            )
            # Alarm-and-reset.
            self._s_high = 0.0
            self._s_low = 0.0
            return event

        return None
