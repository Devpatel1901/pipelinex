"""Factory for detectors (Strategy + Factory).

Both numeric (``IAnomalyDetector``) and sequence
(``ISequenceAnomalyDetector``) detectors are registered here. The factory
keeps two separate registries because the dispatch interfaces differ — the
sessionizer subscribes the sequence variant to the EventBus while the
numeric variant runs inline as a pipeline stage.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from pipelinex.core.exceptions import ConfigurationError
from pipelinex.core.interfaces import IAnomalyDetector, ISequenceAnomalyDetector

NumericCtor = Callable[..., IAnomalyDetector]
SequenceCtor = Callable[..., ISequenceAnomalyDetector]


class DetectorFactory:
    """Registry mapping kind → constructor for both detector flavours."""

    def __init__(self) -> None:
        self._numeric: dict[str, NumericCtor] = {}
        self._sequence: dict[str, SequenceCtor] = {}

    def register_numeric(self, kind: str, ctor: NumericCtor) -> None:
        if kind in self._numeric:
            raise ConfigurationError(f"numeric detector already registered: {kind}")
        self._numeric[kind] = ctor

    def register_sequence(self, kind: str, ctor: SequenceCtor) -> None:
        if kind in self._sequence:
            raise ConfigurationError(f"sequence detector already registered: {kind}")
        self._sequence[kind] = ctor

    def create_numeric(self, kind: str, **kwargs: Any) -> IAnomalyDetector:
        try:
            return self._numeric[kind](**kwargs)
        except KeyError as e:
            raise ConfigurationError(
                f"unknown numeric detector: {kind!r} "
                f"(registered: {sorted(self._numeric)})"
            ) from e

    def create_sequence(self, kind: str, **kwargs: Any) -> ISequenceAnomalyDetector:
        try:
            return self._sequence[kind](**kwargs)
        except KeyError as e:
            raise ConfigurationError(
                f"unknown sequence detector: {kind!r} "
                f"(registered: {sorted(self._sequence)})"
            ) from e

    def known_numeric(self) -> list[str]:
        return sorted(self._numeric)

    def known_sequence(self) -> list[str]:
        return sorted(self._sequence)


def default_factory() -> DetectorFactory:
    """Return a DetectorFactory with all built-in detectors registered."""
    from pipelinex.detectors.cusum import CUSUMDetector
    from pipelinex.detectors.iqr import IQRDetector
    from pipelinex.detectors.sequence import SequenceAnomalyDetector
    from pipelinex.detectors.zscore import ZScoreDetector

    f = DetectorFactory()
    f.register_numeric("z_score", ZScoreDetector)
    f.register_numeric("iqr", IQRDetector)
    f.register_numeric("cusum", CUSUMDetector)
    f.register_sequence("sequence_ngram", SequenceAnomalyDetector)
    return f
