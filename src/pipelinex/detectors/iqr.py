"""Inter-Quartile-Range (IQR) anomaly detector.

Robust to outliers in the training data itself. Maintains a sliding window
and flags any point falling outside ``[Q1 - k*IQR, Q3 + k*IQR]`` (Tukey's
fence). Standard ``k`` is 1.5 (mild outliers); 3.0 marks "far out" outliers.

When does this beat Z-Score?
- Heavy-tailed distributions where the standard deviation is dominated by
  prior outliers, masking new ones.
- Skewed distributions where the mean is not the natural centre.

When does it lose to Z-Score?
- Gaussian-ish, tightly-clustered streams: Z-Score is sharper.
"""

from __future__ import annotations

import bisect
from collections import deque
from typing import Final

from pipelinex.core.interfaces import IAnomalyDetector
from pipelinex.core.models import AnomalyEvent, LogRecord

DEFAULT_WINDOW: Final = 100
DEFAULT_K: Final = 1.5
DEFAULT_MIN_SAMPLES: Final = 30


class IQRDetector(IAnomalyDetector):
    """Sliding-window IQR-based outlier detector."""

    def __init__(
        self,
        metric_key: str = "metric",
        k: float = DEFAULT_K,
        window_size: int = DEFAULT_WINDOW,
        min_samples: int = DEFAULT_MIN_SAMPLES,
        source_dict: str = "enrichment",
    ) -> None:
        if window_size <= 1:
            raise ValueError("window_size must be > 1")
        if k <= 0:
            raise ValueError("k must be > 0")
        if min_samples < 4:
            raise ValueError("min_samples must be >= 4 (need quartiles)")
        if source_dict not in {"enrichment", "raw_payload"}:
            raise ValueError("source_dict must be 'enrichment' or 'raw_payload'")

        self._metric_key = metric_key
        self._k = k
        self._window_size = window_size
        self._min_samples = min_samples
        self._source_dict = source_dict
        # Maintain TWO views of the window: an insertion-ordered deque for
        # eviction, and a sorted list for O(log n) percentile lookup.
        self._window: deque[float] = deque(maxlen=window_size)
        self._sorted: list[float] = []

    @property
    def name(self) -> str:
        return "iqr"

    def reset(self) -> None:
        self._window.clear()
        self._sorted.clear()

    async def detect(self, record: LogRecord) -> AnomalyEvent | None:
        bag = record.enrichment if self._source_dict == "enrichment" else record.raw_payload
        raw = bag.get(self._metric_key)
        if not isinstance(raw, (int, float)) or isinstance(raw, bool):
            return None

        value = float(raw)

        # Score current point against the prior window, then update.
        bounds = self._bounds()
        self._add(value)

        if bounds is None:
            return None
        lower, upper = bounds
        if lower <= value <= upper:
            return None

        # Distance from the nearest fence as a severity score.
        if value < lower:
            distance = lower - value
            side = "below"
        else:
            distance = value - upper
            side = "above"
        return AnomalyEvent(
            log_record_id=record.id,
            detector_name=self.name,
            severity_score=distance,
            metadata={
                "metric_key": self._metric_key,
                "value": value,
                "lower_fence": lower,
                "upper_fence": upper,
                "side": side,
                "window_size": len(self._window),
            },
        )

    # ---------- private ----------

    def _add(self, value: float) -> None:
        # Evict the oldest sample from both views if the window is full.
        if len(self._window) == self._window.maxlen:
            old = self._window[0]
            idx = bisect.bisect_left(self._sorted, old)
            if idx < len(self._sorted) and self._sorted[idx] == old:
                del self._sorted[idx]
        self._window.append(value)
        bisect.insort(self._sorted, value)

    def _bounds(self) -> tuple[float, float] | None:
        n = len(self._sorted)
        if n < self._min_samples:
            return None
        q1 = _percentile(self._sorted, 0.25)
        q3 = _percentile(self._sorted, 0.75)
        iqr = q3 - q1
        if iqr == 0:
            return None
        return q1 - self._k * iqr, q3 + self._k * iqr


def _percentile(sorted_values: list[float], p: float) -> float:
    """Linear-interpolation percentile on a pre-sorted list."""
    n = len(sorted_values)
    if n == 1:
        return sorted_values[0]
    rank = p * (n - 1)
    lo = int(rank)
    hi = min(lo + 1, n - 1)
    frac = rank - lo
    return sorted_values[lo] + frac * (sorted_values[hi] - sorted_values[lo])
