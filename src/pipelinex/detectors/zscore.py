"""Z-Score anomaly detector.

Maintains a sliding window of recent numeric values and flags any incoming
value whose z-score (``(x - mu) / sigma``) exceeds a configured threshold.

Algorithm
---------
For each record we read a single numeric field from the record (configurable
key, default ``enrichment["metric"]``). We push it into a fixed-size deque,
recompute mean and standard deviation, and compute the z-score for the new
value. If ``|z| > threshold``, an ``AnomalyEvent`` is emitted.

Pre-warmup behaviour: the first ``min_samples`` records are silently
absorbed — there is no statistical basis for declaring an anomaly until the
window has enough samples. ``reset()`` clears the window for the next run.

Why z-score
~~~~~~~~~~~
- Cheap (O(n) per detection where n is window size).
- Easy to defend in interview: well-understood baseline.
- Documented weakness (assumes normal distribution) motivates the IQR and
  CUSUM detectors that follow on Day 13.
"""

from __future__ import annotations

import math
from collections import deque
from typing import Final

from pipelinex.core.interfaces import IAnomalyDetector
from pipelinex.core.models import AnomalyEvent, LogRecord

DEFAULT_WINDOW: Final = 100
DEFAULT_THRESHOLD: Final = 3.0
DEFAULT_MIN_SAMPLES: Final = 30


class ZScoreDetector(IAnomalyDetector):
    """Sliding-window z-score detector for a single numeric metric."""

    def __init__(
        self,
        metric_key: str = "metric",
        threshold: float = DEFAULT_THRESHOLD,
        window_size: int = DEFAULT_WINDOW,
        min_samples: int = DEFAULT_MIN_SAMPLES,
        source_dict: str = "enrichment",
    ) -> None:
        if window_size <= 1:
            raise ValueError("window_size must be > 1")
        if threshold <= 0:
            raise ValueError("threshold must be > 0")
        if min_samples < 2:
            raise ValueError("min_samples must be >= 2")
        if source_dict not in {"enrichment", "raw_payload"}:
            raise ValueError("source_dict must be 'enrichment' or 'raw_payload'")

        self._metric_key = metric_key
        self._threshold = threshold
        self._window_size = window_size
        self._min_samples = min_samples
        self._source_dict = source_dict
        self._window: deque[float] = deque(maxlen=window_size)

    @property
    def name(self) -> str:
        return "z_score"

    def reset(self) -> None:
        self._window.clear()

    async def detect(self, record: LogRecord) -> AnomalyEvent | None:
        bag = (
            record.enrichment if self._source_dict == "enrichment" else record.raw_payload
        )
        raw_value = bag.get(self._metric_key)
        if not isinstance(raw_value, (int, float)) or isinstance(raw_value, bool):
            return None  # nothing to score

        value = float(raw_value)

        # First, score the *current* point against the *prior* window so the
        # decision is independent of this very value. Then push it.
        score = self._z_score(value)
        self._window.append(value)

        if score is None or abs(score) <= self._threshold:
            return None

        return AnomalyEvent(
            log_record_id=record.id,
            detector_name=self.name,
            severity_score=abs(score),
            metadata={
                "metric_key": self._metric_key,
                "value": value,
                "z_score": score,
                "window_size": len(self._window),
                "threshold": self._threshold,
            },
        )

    # ---------- private ----------

    def _z_score(self, value: float) -> float | None:
        n = len(self._window)
        if n < self._min_samples:
            return None
        mean = sum(self._window) / n
        variance = sum((x - mean) ** 2 for x in self._window) / n
        sigma = math.sqrt(variance)
        if sigma == 0.0:
            # Constant window: any deviation is "infinite" z, but a robust
            # implementation reports None and lets a downstream rule handle
            # the constant-stream edge case.
            return None
        return (value - mean) / sigma
