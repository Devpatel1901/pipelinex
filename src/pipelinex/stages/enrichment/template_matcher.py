"""TemplateMatcherStage — assigns ``enrichment["event_id"]`` from templates.

Loads the loghub-style templates CSV (``EventId,EventTemplate``) at init,
compiles each template's ``<*>`` wildcards into a regex, and matches each
record's message against them in order. The first match wins; an unmatched
message gets no event_id.

Why a separate stage and not part of the parser
----------------------------------------------
- Template matching is enrichment, not parsing — the message is already
  parsed, we're just classifying it.
- Keeping it separate lets us decorate it independently (timing/retry).
- The set of templates can be swapped without touching parser code.
"""

from __future__ import annotations

import csv
import re
from pathlib import Path
from typing import Final

from pipelinex.core.exceptions import ConfigurationError
from pipelinex.core.interfaces import IPipelineStage
from pipelinex.core.models import LogRecord

# Loghub ships HDFS templates with two notations: ``<*>`` in the 2k sample
# (newer format) and ``[*]`` in the preprocessed full-corpus templates. We
# accept both — both behave as a "match any text" placeholder.
_WILDCARD: Final = re.compile(r"<\*>|\[\*\]")


class TemplateMatcherStage(IPipelineStage):
    """Tag records with the first matching event_id from a templates file."""

    def __init__(
        self,
        templates_path: str | Path,
        enrichment_key: str = "event_id",
    ) -> None:
        self._templates_path = Path(templates_path)
        if not self._templates_path.exists():
            raise ConfigurationError(
                f"templates file not found: {self._templates_path}"
            )
        self._enrichment_key = enrichment_key
        self._patterns: list[tuple[str, re.Pattern[str]]] = self._compile_templates()

    @property
    def name(self) -> str:
        return "enrich:template_match"

    @property
    def template_count(self) -> int:
        return len(self._patterns)

    def _compile_templates(self) -> list[tuple[str, re.Pattern[str]]]:
        out: list[tuple[str, re.Pattern[str]]] = []
        with self._templates_path.open(encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                eid = row.get("EventId")
                template = row.get("EventTemplate")
                if not eid or not template:
                    continue
                # Escape the template, replace <*> placeholders with .*?.
                escaped_parts = [
                    re.escape(part) for part in _WILDCARD.split(template)
                ]
                pattern = ".*?".join(escaped_parts)
                # Anchor at start to keep matches deterministic.
                out.append((eid, re.compile("^" + pattern, re.DOTALL)))
        return out

    async def process(self, record: LogRecord) -> LogRecord:
        message = record.message or ""
        for eid, pat in self._patterns:
            if pat.match(message):
                record.add_enrichment(self._enrichment_key, eid)
                return record
        return record
