"""FeatureWindowDetector — multi-feature point detector for BGL.

Motivation
----------
The legacy BGL evaluation reduces each 1-minute window to a single number
(``event_rate``) and feeds it to Z-Score / IQR / CUSUM. BGL anomalies,
however, are scattered single lines that do not reliably move the rate
— all three legacy detectors land at F1 ~0.10-0.20.

This detector consumes the *same* per-line ``LogRecord`` stream but
keeps a small in-memory aggregator and emits an ``AnomalyEvent`` at
window boundaries. Per closed window it computes a tiny feature vector::

    rate                  count of records in the window
    distinct_components   |{ rec.source for rec in window }|
    severity_warn_ratio   fraction with severity >= WARNING
    template_entropy      Shannon entropy of enrichment["event_id"] dist

Each feature has its own sliding history of length ``feature_history``.
When a window closes, every feature is scored against its own history
using the same Tukey IQR fence as ``IQRDetector``. The window fires if
*any* feature breaches its fence.

This is honest signal: every feature is computable from a generic log
stream and none of them read the ``enrichment["bgl_label"]`` ground
truth (that would be label leakage).

Stream interface
----------------
``IAnomalyDetector.detect(record)`` is called once per record, but a
window detector needs to accumulate. The contract here:

- ``detect(record)`` returns ``None`` while building the current window.
- When a record arrives in a *new* window, the *prior* window is
  finalised and its ``AnomalyEvent | None`` is returned (so a single
  record-call can both close the old window *and* start a new one).
- ``flush()`` finalises the final pending window and returns its event.
  The eval script must call ``flush()`` at end of stream.
- ``reset()`` clears all state — used between runs.
"""

from __future__ import annotations

import bisect
import math
from collections import deque
from collections.abc import Iterable
from datetime import datetime
from typing import Final
from uuid import UUID, uuid4

from pipelinex.core.interfaces import IAnomalyDetector
from pipelinex.core.models import AnomalyEvent, LogRecord, Severity
from pipelinex.detectors.iqr import _percentile

DEFAULT_WINDOW_SECONDS: Final = 60
DEFAULT_FEATURE_HISTORY: Final = 200
DEFAULT_K: Final = 1.5
DEFAULT_MIN_SAMPLES: Final = 50

FEATURE_KEYS: Final = (
    "rate",
    "distinct_components",
    "severity_warn_ratio",
    "template_entropy",
)


class _WindowAcc:
    """Mutable accumulator for the current open window."""

    __slots__ = (
        "start_ts",
        "count",
        "components",
        "warn_count",
        "event_counts",
        "first_record_id",
    )

    def __init__(self, start_ts: datetime) -> None:
        self.start_ts = start_ts
        self.count = 0
        self.components: set[str] = set()
        self.warn_count = 0
        self.event_counts: dict[str, int] = {}
        self.first_record_id: UUID | None = None

    def add(self, record: LogRecord) -> None:
        self.count += 1
        if self.first_record_id is None:
            self.first_record_id = record.id
        if record.source:
            self.components.add(record.source)
        if record.severity in (Severity.WARNING, Severity.ERROR, Severity.CRITICAL):
            self.warn_count += 1
        eid = record.enrichment.get("event_id")
        if isinstance(eid, str):
            self.event_counts[eid] = self.event_counts.get(eid, 0) + 1

    def features(self) -> dict[str, float]:
        rate = float(self.count)
        distinct = float(len(self.components))
        warn_ratio = self.warn_count / self.count if self.count else 0.0
        entropy = _entropy(self.event_counts.values())
        return {
            "rate": rate,
            "distinct_components": distinct,
            "severity_warn_ratio": warn_ratio,
            "template_entropy": entropy,
        }


def _entropy(counts: Iterable[int]) -> float:
    """Shannon entropy (nats) of a multiset of integer counts."""
    counts = list(counts)
    total = sum(counts)
    if total <= 0:
        return 0.0
    h = 0.0
    for c in counts:
        if c <= 0:
            continue
        p = c / total
        h -= p * math.log(p)
    return h


class FeatureWindowDetector(IAnomalyDetector):
    """Multi-feature, time-windowed point detector."""

    def __init__(
        self,
        window_seconds: int = DEFAULT_WINDOW_SECONDS,
        feature_history: int = DEFAULT_FEATURE_HISTORY,
        iqr_k: float = DEFAULT_K,
        min_samples: int = DEFAULT_MIN_SAMPLES,
        features: tuple[str, ...] | list[str] | None = None,
    ) -> None:
        if window_seconds <= 0:
            raise ValueError("window_seconds must be > 0")
        if feature_history <= 1:
            raise ValueError("feature_history must be > 1")
        if iqr_k <= 0:
            raise ValueError("iqr_k must be > 0")
        if min_samples < 4:
            raise ValueError("min_samples must be >= 4 (need quartiles)")

        selected = tuple(features) if features is not None else FEATURE_KEYS
        unknown = set(selected) - set(FEATURE_KEYS)
        if unknown:
            raise ValueError(f"unknown features: {sorted(unknown)}")
        self._features = selected
        self._window_seconds = window_seconds
        self._feature_history = feature_history
        self._k = iqr_k
        self._min_samples = min_samples

        # Per-feature sliding window of past observations (oldest first).
        self._history: dict[str, deque[float]] = {
            k: deque(maxlen=feature_history) for k in self._features
        }
        self._sorted: dict[str, list[float]] = {k: [] for k in self._features}
        self._current: _WindowAcc | None = None

    @property
    def name(self) -> str:
        return "feature_window"

    def reset(self) -> None:
        for k in self._features:
            self._history[k].clear()
            self._sorted[k].clear()
        self._current = None

    async def detect(self, record: LogRecord) -> AnomalyEvent | None:
        ts = record.timestamp
        if ts is None:
            return None
        bucket = self._bucket(ts)
        if self._current is None:
            self._current = _WindowAcc(bucket)
            self._current.add(record)
            return None
        if bucket == self._current.start_ts:
            self._current.add(record)
            return None
        # New window — finalise the prior one and start a fresh accumulator.
        closed = self._current
        self._current = _WindowAcc(bucket)
        self._current.add(record)
        return self._score(closed)

    def flush(self) -> AnomalyEvent | None:
        """Finalise the pending window. Call at end of stream."""
        if self._current is None:
            return None
        closed = self._current
        self._current = None
        return self._score(closed)

    # ---------- private ----------

    def _bucket(self, ts: datetime) -> datetime:
        # Truncate to the start of the window. Works for any positive seconds
        # (1, 60, 300, ...) by flooring epoch seconds.
        epoch = int(ts.timestamp())
        start_epoch = epoch - (epoch % self._window_seconds)
        if ts.tzinfo is not None:
            return datetime.fromtimestamp(start_epoch, tz=ts.tzinfo)
        return datetime.fromtimestamp(start_epoch)

    def _score(self, window: _WindowAcc) -> AnomalyEvent | None:
        feats = window.features()
        breaches: dict[str, dict[str, float]] = {}
        max_distance = 0.0
        for key in self._features:
            value = feats[key]
            bounds = self._bounds(key)
            # Update history *after* scoring so the decision uses prior data.
            self._add(key, value)
            if bounds is None:
                continue
            lower, upper = bounds
            if lower <= value <= upper:
                continue
            distance = lower - value if value < lower else value - upper
            breaches[key] = {
                "value": value,
                "lower_fence": lower,
                "upper_fence": upper,
                "distance": distance,
            }
            if distance > max_distance:
                max_distance = distance

        if not breaches:
            return None

        anchor = window.first_record_id or uuid4()
        return AnomalyEvent(
            log_record_id=anchor,
            detector_name=self.name,
            severity_score=max_distance,
            metadata={
                "window_start": window.start_ts.isoformat(),
                "window_seconds": self._window_seconds,
                "record_count": window.count,
                "features": feats,
                "breaches": breaches,
                "breached_features": sorted(breaches.keys()),
            },
        )

    def _add(self, key: str, value: float) -> None:
        history = self._history[key]
        sorted_view = self._sorted[key]
        if len(history) == history.maxlen:
            old = history[0]
            idx = bisect.bisect_left(sorted_view, old)
            if idx < len(sorted_view) and sorted_view[idx] == old:
                del sorted_view[idx]
        history.append(value)
        bisect.insort(sorted_view, value)

    def _bounds(self, key: str) -> tuple[float, float] | None:
        sorted_view = self._sorted[key]
        n = len(sorted_view)
        if n < self._min_samples:
            return None
        q1 = _percentile(sorted_view, 0.25)
        q3 = _percentile(sorted_view, 0.75)
        iqr = q3 - q1
        if iqr == 0:
            return None
        return q1 - self._k * iqr, q3 + self._k * iqr
