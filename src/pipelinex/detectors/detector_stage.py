"""Stage that wraps an ``IAnomalyDetector`` as a pipeline stage.

When the wrapped detector flags a record, the resulting ``AnomalyEvent`` is
persisted via the repository. The original record continues unchanged so
downstream stages (storage, additional detectors) still see it.

Day 12 will replace direct repository writes with an ``EventBus`` publish.
"""

from __future__ import annotations

from pipelinex.core.interfaces import IAnomalyDetector, ILogRepository, IPipelineStage
from pipelinex.core.models import LogRecord


class DetectorStage(IPipelineStage):
    """Run a detector against each record and persist any anomalies it fires."""

    def __init__(self, detector: IAnomalyDetector, repository: ILogRepository) -> None:
        self._detector = detector
        self._repository = repository
        self._anomaly_count = 0

    @property
    def name(self) -> str:
        return f"detect:{self._detector.name}"

    @property
    def anomaly_count(self) -> int:
        return self._anomaly_count

    async def process(self, record: LogRecord) -> LogRecord:
        anomaly = await self._detector.detect(record)
        if anomaly is not None:
            await self._repository.save_anomaly(anomaly)
            self._anomaly_count += 1
            record.add_enrichment(f"anomaly:{self._detector.name}", anomaly.severity_score)
        return record
