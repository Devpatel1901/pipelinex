"""Tests for TemplateMatcherStage."""

from __future__ import annotations

from pathlib import Path

import pytest

from pipelinex.core.exceptions import ConfigurationError
from pipelinex.core.models import LogRecord
from pipelinex.stages.enrichment.template_matcher import TemplateMatcherStage


def _write_templates(tmp_path: Path) -> Path:
    path = tmp_path / "templates.csv"
    path.write_text(
        "EventId,EventTemplate\n"
        'E1,Adding an already existing block <*>\n'
        'E10,PacketResponder <*> for block blk_<*> terminating\n'
        'E22,BLOCK* NameSystem.allocateBlock: /<*>/part-<*>. blk_<*>\n'
    )
    return path


class TestTemplateMatcher:
    def test_loads_templates(self, tmp_path: Path) -> None:
        stage = TemplateMatcherStage(_write_templates(tmp_path))
        assert stage.template_count == 3

    def test_missing_templates_raises(self, tmp_path: Path) -> None:
        with pytest.raises(ConfigurationError, match="not found"):
            TemplateMatcherStage(tmp_path / "nope.csv")

    async def test_assigns_event_id_on_match(self, tmp_path: Path) -> None:
        stage = TemplateMatcherStage(_write_templates(tmp_path))
        rec = LogRecord(
            message=(
                "PacketResponder 1 for block blk_38865049064139660 terminating"
            )
        )
        await stage.process(rec)
        assert rec.enrichment["event_id"] == "E10"

    async def test_no_match_leaves_event_id_unset(self, tmp_path: Path) -> None:
        stage = TemplateMatcherStage(_write_templates(tmp_path))
        rec = LogRecord(message="completely unrelated message")
        await stage.process(rec)
        assert "event_id" not in rec.enrichment

    async def test_first_match_wins(self, tmp_path: Path) -> None:
        # Build a templates file where two templates could match the same
        # message; verify the order in the file decides.
        path = tmp_path / "ordered.csv"
        path.write_text(
            "EventId,EventTemplate\n"
            'EFIRST,The <*> message\n'
            'ESECOND,The quick message\n'
        )
        stage = TemplateMatcherStage(path)
        rec = LogRecord(message="The quick message")
        await stage.process(rec)
        assert rec.enrichment["event_id"] == "EFIRST"
