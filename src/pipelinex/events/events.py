"""Event types published on the EventBus.

Two event families today:

- ``AnomalyEventPublished``: produced by detectors (Day 6 onward).
- ``BlockTraceClosed``: produced by ``BlockSessionizerStage`` (Day 13);
  the ``KLDivergenceSequenceDetector`` subscribes to these to evaluate
  closed traces.

Adding a new event type does not require any change to the EventBus — that
is the entire point.
"""

from __future__ import annotations

from dataclasses import dataclass

from pipelinex.core.models import AnomalyEvent, BlockTrace


@dataclass(frozen=True)
class AnomalyEventPublished:
    """A detector flagged an anomaly."""

    anomaly: AnomalyEvent


@dataclass(frozen=True)
class BlockTraceClosed:
    """Sessionizer just emitted a closed BlockTrace."""

    trace: BlockTrace
