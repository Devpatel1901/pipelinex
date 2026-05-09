"""In-process metrics registry.

Lightweight counters and timers that decorators (and other components) can
write to. The API mirrors the subset of Prometheus / StatsD vocabulary that
matters for an interview demo: counters, gauges, and timers (which compute
percentiles on demand).

Thread-safe via an asyncio Lock. This is single-process only — distributed
metrics are explicit non-goal of v1.
"""

from __future__ import annotations

import asyncio
import bisect
from dataclasses import dataclass, field
from typing import Any


@dataclass
class _Timer:
    samples: list[float] = field(default_factory=list)

    def record(self, value: float) -> None:
        self.samples.append(value)

    def count(self) -> int:
        return len(self.samples)

    def total(self) -> float:
        return sum(self.samples)

    def percentile(self, p: float) -> float:
        if not self.samples:
            return 0.0
        sorted_samples = sorted(self.samples)
        idx = max(0, min(len(sorted_samples) - 1, int(p * len(sorted_samples))))
        return sorted_samples[idx]


class MetricsRegistry:
    """Process-local registry for counters, gauges, and timers."""

    def __init__(self) -> None:
        self._counters: dict[str, int] = {}
        self._gauges: dict[str, float] = {}
        self._timers: dict[str, _Timer] = {}
        self._lock = asyncio.Lock()

    async def inc(self, name: str, by: int = 1) -> None:
        async with self._lock:
            self._counters[name] = self._counters.get(name, 0) + by

    async def gauge(self, name: str, value: float) -> None:
        async with self._lock:
            self._gauges[name] = value

    async def timing(self, name: str, seconds: float) -> None:
        async with self._lock:
            t = self._timers.setdefault(name, _Timer())
            bisect.insort(t.samples, seconds)

    def snapshot(self) -> dict[str, Any]:
        """Return a serializable snapshot of all metrics."""
        timers: dict[str, dict[str, float]] = {}
        for name, timer in self._timers.items():
            timers[name] = {
                "count": float(timer.count()),
                "total_s": timer.total(),
                "p50_s": timer.percentile(0.5),
                "p95_s": timer.percentile(0.95),
                "p99_s": timer.percentile(0.99),
            }
        return {
            "counters": dict(self._counters),
            "gauges": dict(self._gauges),
            "timers": timers,
        }

    def reset(self) -> None:
        self._counters.clear()
        self._gauges.clear()
        self._timers.clear()


# Process-wide default registry. Stages and tests can opt to use a private
# instance instead via the constructor argument on the timing decorator.
default_registry = MetricsRegistry()
