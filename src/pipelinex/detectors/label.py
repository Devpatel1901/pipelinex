"""LabelAnomalyDetector — honor an upstream per-line classification.

Some log streams (BGL is the canonical example) carry their own anomaly
classification on every line. BGL's first field is ``-`` for normal lines
and a category code (``KERNDTLB``, ``APPREAD``, ``KERNSTOR``, …) for
alerts. ``BGLParser`` already extracts this into ``enrichment["bgl_label"]``
so the detector just has to honor it.

Conceptually this is a *trust-the-source* detector. In production it maps
to honoring any upstream classifier (a tool's own diagnostic code, a
vendor SDK's severity flag, etc.). It complements unsupervised detection
rather than replacing it — the ``WindowFeatureDetector`` provides the
learned counterpart for the same dataset.
"""

from __future__ import annotations

from pipelinex.core.interfaces import IAnomalyDetector
from pipelinex.core.models import AnomalyEvent, LogRecord


class LabelAnomalyDetector(IAnomalyDetector):
    """Fire when ``record.enrichment[label_key]`` is set to anything other
    than ``normal_value``.

    Configurable so the same detector generalises to any parser that
    surfaces an upstream classification (BGL, syslog priority labels,
    vendor RAS codes, etc.).
    """

    def __init__(
        self,
        label_key: str = "bgl_label",
        normal_value: str = "-",
        source_dict: str = "enrichment",
    ) -> None:
        if not label_key:
            raise ValueError("label_key must be a non-empty string")
        if source_dict not in {"enrichment", "raw_payload"}:
            raise ValueError("source_dict must be 'enrichment' or 'raw_payload'")
        self._label_key = label_key
        self._normal_value = normal_value
        self._source_dict = source_dict

    @property
    def name(self) -> str:
        return "label"

    def reset(self) -> None:
        # Stateless detector — nothing to clear.
        return

    async def detect(self, record: LogRecord) -> AnomalyEvent | None:
        bag = (
            record.enrichment if self._source_dict == "enrichment" else record.raw_payload
        )
        raw = bag.get(self._label_key)
        if not isinstance(raw, str) or not raw or raw == self._normal_value:
            return None

        return AnomalyEvent(
            log_record_id=record.id,
            detector_name=self.name,
            severity_score=1.0,
            metadata={
                "label_key": self._label_key,
                "label": raw,
                "normal_value": self._normal_value,
            },
        )
