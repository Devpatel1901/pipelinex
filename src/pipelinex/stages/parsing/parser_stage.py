"""Stage that wraps an ``IParser`` as an ``IPipelineStage``.

The producer pushes records through the queue with ``raw_payload["raw"]``
set; this stage replaces the record with one fully parsed by the configured
parser. Failures are converted into ``FatalStageError`` (drop record) or
recorded as ``parse_warning`` enrichment depending on parser semantics.
"""

from __future__ import annotations

from pipelinex.core.exceptions import FatalStageError, ParserError
from pipelinex.core.interfaces import IParser, IPipelineStage
from pipelinex.core.models import LogRecord


class ParserStage(IPipelineStage):
    """Apply a single parser to ``raw_payload['raw']``."""

    def __init__(self, parser: IParser) -> None:
        self._parser = parser

    @property
    def name(self) -> str:
        return f"parse:{self._parser.name}"

    async def process(self, record: LogRecord) -> LogRecord:
        raw = record.raw_payload.get("raw")
        if not isinstance(raw, str):
            raise FatalStageError(
                f"ParserStage requires raw_payload['raw'] to be str, got {type(raw).__name__}"
            )
        try:
            parsed = self._parser.parse(raw)
        except ParserError as e:
            raise FatalStageError(f"{self._parser.name} failed to parse: {e}") from e

        # Preserve identity & pipeline metadata from the input record.
        parsed.id = record.id
        parsed.pipeline_run_id = record.pipeline_run_id
        parsed.stage_history = list(record.stage_history)
        return parsed
