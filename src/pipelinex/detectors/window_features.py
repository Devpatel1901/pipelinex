"""WindowFeatureDetector — unsupervised per-window scoring.

For datasets where individual lines aren't anomalous but *windows* of
traffic can be (alert spikes, severity mix shifts, novel components),
this detector buckets records by time window and scores each completed
window against a weighted combination of four features:

1. **Alert density** — fraction of records in the window whose
   ``level_raw`` is one of {ERROR, FATAL, SEVERE, FAILURE}.
2. **Severity entropy** — Shannon entropy over the window's
   ``level_raw`` distribution. Low entropy = uniform traffic; high
   entropy = mixed severity (interesting).
3. **Node diversity** — ``unique(node) / record_count``. A storm hitting
   one node looks different from a storm hitting many.
4. **Template novelty** — fraction of ``type`` values in the window not
   seen in the trailing ``recent_window_history`` windows. New event
   types appearing is a strong signal.

A weighted sum of the four normalised features is compared to
``score_threshold``. Windows above threshold fire one anomaly event,
anchored to the first record of the *next* window (i.e., the trigger
that caused the previous window to close). The slight temporal offset
is fine for evaluation; the metadata records the actual window key.
"""

from __future__ import annotations

import math
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Final
from uuid import UUID

from pipelinex.core.interfaces import IAnomalyDetector
from pipelinex.core.models import AnomalyEvent, LogRecord

DEFAULT_WINDOW_SECONDS: Final = 60.0
DEFAULT_RECENT_HISTORY: Final = 10
ALERT_LEVELS: Final = frozenset({"ERROR", "FATAL", "SEVERE", "FAILURE"})


@dataclass
class _WindowState:
    """Per-window accumulator."""

    key: int
    record_count: int = 0
    alert_count: int = 0
    level_counts: Counter[str] = field(default_factory=Counter)
    node_set: set[str] = field(default_factory=set)
    type_set: set[str] = field(default_factory=set)
    first_record_id: UUID | None = None
    first_timestamp: datetime | None = None

    def add(self, record: LogRecord) -> None:
        if self.first_record_id is None:
            self.first_record_id = record.id
            self.first_timestamp = record.timestamp
        self.record_count += 1
        level_raw = record.raw_payload.get("level_raw")
        if isinstance(level_raw, str):
            self.level_counts[level_raw] += 1
            if level_raw in ALERT_LEVELS:
                self.alert_count += 1
        node = record.enrichment.get("node")
        if isinstance(node, str):
            self.node_set.add(node)
        log_type = record.raw_payload.get("type")
        if isinstance(log_type, str):
            self.type_set.add(log_type)

    def features(self, recent_types: set[str]) -> dict[str, float]:
        n = max(self.record_count, 1)
        density = self.alert_count / n
        entropy = _entropy(self.level_counts.values())
        diversity = len(self.node_set) / n
        if not self.type_set:
            novelty = 0.0
        else:
            unseen = self.type_set - recent_types
            novelty = len(unseen) / len(self.type_set)
        return {
            "alert_density": density,
            "severity_entropy": entropy,
            "node_diversity": diversity,
            "template_novelty": novelty,
        }


def _entropy(counts: Any) -> float:
    """Shannon entropy in nats over a Counter's values."""
    vals = [c for c in counts if c > 0]
    total = sum(vals)
    if total == 0:
        return 0.0
    return float(-sum((c / total) * math.log(c / total) for c in vals))


class WindowFeatureDetector(IAnomalyDetector):
    """Score time-bucketed feature windows against a weighted threshold."""

    def __init__(
        self,
        window_seconds: float = DEFAULT_WINDOW_SECONDS,
        recent_window_history: int = DEFAULT_RECENT_HISTORY,
        alert_density_threshold: float = 0.10,
        entropy_threshold: float = 1.5,
        novelty_threshold: float = 0.30,
        weight_alert: float = 0.4,
        weight_entropy: float = 0.2,
        weight_node: float = 0.2,
        weight_novelty: float = 0.2,
        score_threshold: float = 0.5,
    ) -> None:
        if window_seconds <= 0:
            raise ValueError("window_seconds must be > 0")
        if recent_window_history < 0:
            raise ValueError("recent_window_history must be >= 0")
        if score_threshold < 0:
            raise ValueError("score_threshold must be >= 0")

        self._window_seconds = window_seconds
        self._recent_history = recent_window_history
        self._alert_density_threshold = alert_density_threshold
        self._entropy_threshold = entropy_threshold
        self._novelty_threshold = novelty_threshold
        self._w_alert = weight_alert
        self._w_entropy = weight_entropy
        self._w_node = weight_node
        self._w_novelty = weight_novelty
        self._score_threshold = score_threshold

        self._current: _WindowState | None = None
        self._recent_type_history: list[set[str]] = []

    @property
    def name(self) -> str:
        return "window_features"

    def reset(self) -> None:
        self._current = None
        self._recent_type_history = []

    async def detect(self, record: LogRecord) -> AnomalyEvent | None:
        if record.timestamp is None:
            return None
        key = int(record.timestamp.timestamp() // self._window_seconds)

        if self._current is None:
            self._current = _WindowState(key=key)
            self._current.add(record)
            return None

        if key == self._current.key:
            self._current.add(record)
            return None

        # Window rolled over — score the closed window before opening the next.
        closed = self._current
        recent_types = self._merged_recent_types()
        features = closed.features(recent_types)
        score = self._weighted_score(features)
        self._update_recent_history(closed.type_set)

        self._current = _WindowState(key=key)
        self._current.add(record)

        if score <= self._score_threshold:
            return None

        anchor = closed.first_record_id or record.id
        return AnomalyEvent(
            log_record_id=anchor,
            detector_name=self.name,
            severity_score=score,
            metadata={
                "window_key": closed.key,
                "window_seconds": self._window_seconds,
                "record_count": closed.record_count,
                "first_timestamp": (
                    closed.first_timestamp.isoformat()
                    if closed.first_timestamp is not None
                    else None
                ),
                "score": score,
                **features,
                "alert_density_threshold": self._alert_density_threshold,
                "entropy_threshold": self._entropy_threshold,
                "novelty_threshold": self._novelty_threshold,
            },
        )

    # ---------- private ----------

    def _merged_recent_types(self) -> set[str]:
        out: set[str] = set()
        for s in self._recent_type_history:
            out |= s
        return out

    def _update_recent_history(self, types: set[str]) -> None:
        self._recent_type_history.append(types)
        while len(self._recent_type_history) > self._recent_history:
            self._recent_type_history.pop(0)

    def _weighted_score(self, features: dict[str, float]) -> float:
        # Each component clipped to [0, 1] by dividing by its threshold;
        # > 1 contributes more than its threshold weight (anomaly-shaped).
        s = 0.0
        s += self._w_alert * (
            features["alert_density"] / self._alert_density_threshold
            if self._alert_density_threshold > 0
            else 0.0
        )
        s += self._w_entropy * (
            features["severity_entropy"] / self._entropy_threshold
            if self._entropy_threshold > 0
            else 0.0
        )
        # node_diversity has no threshold (0..1 already), use 1.0 as divisor.
        s += self._w_node * features["node_diversity"]
        s += self._w_novelty * (
            features["template_novelty"] / self._novelty_threshold
            if self._novelty_threshold > 0
            else 0.0
        )
        return s
