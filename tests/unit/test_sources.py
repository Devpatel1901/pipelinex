"""Unit tests for log sources."""

from __future__ import annotations

import gzip
from pathlib import Path

import pytest

from pipelinex.core.exceptions import SourceError
from pipelinex.core.sources import FileLogSource, IterableLogSource


class TestFileLogSource:
    async def test_reads_lines_from_plain_file(self, tmp_path: Path) -> None:
        log_file = tmp_path / "sample.log"
        log_file.write_text("line one\nline two\nline three\n")

        src = FileLogSource(log_file)
        lines = [line async for line in src.stream()]
        assert lines == ["line one", "line two", "line three"]

    async def test_skips_empty_lines(self, tmp_path: Path) -> None:
        log_file = tmp_path / "sparse.log"
        log_file.write_text("a\n\nb\n\n\nc\n")

        src = FileLogSource(log_file)
        lines = [line async for line in src.stream()]
        assert lines == ["a", "b", "c"]

    async def test_decompresses_gzip(self, tmp_path: Path) -> None:
        log_file = tmp_path / "compressed.log.gz"
        with gzip.open(log_file, "wt", encoding="utf-8") as fh:
            fh.write("compressed line 1\ncompressed line 2\n")

        src = FileLogSource(log_file)
        lines = [line async for line in src.stream()]
        assert lines == ["compressed line 1", "compressed line 2"]

    def test_missing_file_raises(self, tmp_path: Path) -> None:
        with pytest.raises(SourceError, match="not found"):
            FileLogSource(tmp_path / "nonexistent.log")

    async def test_throttle_rps_introduces_delay(self, tmp_path: Path) -> None:
        # Write 5 lines, throttle to 50 rps → expect ~0.1s minimum total wait.
        # Use a generous lower bound to avoid flakes on slow CI.
        import time

        log_file = tmp_path / "throttled.log"
        log_file.write_text("a\nb\nc\nd\ne\n")

        src = FileLogSource(log_file, throttle_rps=50.0)
        start = time.perf_counter()
        lines = [line async for line in src.stream()]
        elapsed = time.perf_counter() - start

        assert lines == ["a", "b", "c", "d", "e"]
        assert elapsed >= 0.05  # 5 sleeps of 0.02s each = 0.10s, but allow 50% slack


class TestIterableLogSource:
    async def test_yields_each_line(self) -> None:
        src = IterableLogSource(["one", "two", "three"])
        lines = [line async for line in src.stream()]
        assert lines == ["one", "two", "three"]

    async def test_works_with_generator(self) -> None:
        def gen() -> object:
            yield "x"
            yield "y"

        src = IterableLogSource(gen())  # type: ignore[arg-type]
        lines = [line async for line in src.stream()]
        assert lines == ["x", "y"]
