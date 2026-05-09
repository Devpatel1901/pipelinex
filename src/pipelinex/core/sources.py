"""Log sources: producers of raw lines that flow into the pipeline.

Three implementations of ``ILogSource``:

- ``FileLogSource``: read from disk, transparent ``.gz`` decompression.
- ``StdinLogSource``: read from stdin (for ``cat ... | pipelinex run``).
- ``SyntheticLogSource``: backed by the synthetic generator.

Throttle support is built in so benchmarks can replay a real file at a
controlled rate. Backpressure (the bounded asyncio.Queue) takes care of the
inverse direction.
"""

from __future__ import annotations

import asyncio
import gzip
import sys
from collections.abc import AsyncIterator, Iterable
from pathlib import Path

from pipelinex.core.exceptions import SourceError
from pipelinex.core.interfaces import ILogSource


class FileLogSource(ILogSource):
    """Read newline-separated log lines from a file.

    Auto-detects ``.gz`` and decompresses on the fly. Empty lines are skipped.
    """

    def __init__(
        self,
        path: str | Path,
        encoding: str = "utf-8",
        throttle_rps: float | None = None,
    ) -> None:
        self._path = Path(path)
        self._encoding = encoding
        self._throttle_rps = throttle_rps
        if not self._path.exists():
            raise SourceError(f"log file not found: {self._path}")

    async def stream(self) -> AsyncIterator[str]:
        delay = 1.0 / self._throttle_rps if self._throttle_rps else 0.0
        is_gzip = self._path.suffix == ".gz"

        try:
            if is_gzip:
                with gzip.open(self._path, "rt", encoding=self._encoding) as gh:
                    async for line in self._iter_lines(gh, delay):
                        yield line
            else:
                with self._path.open(encoding=self._encoding) as fh:
                    async for line in self._iter_lines(fh, delay):
                        yield line
        except OSError as e:
            raise SourceError(f"failed reading {self._path}: {e}") from e

    @staticmethod
    async def _iter_lines(handle: object, delay: float) -> AsyncIterator[str]:
        for raw in handle:  # type: ignore[attr-defined]
            line = raw.rstrip("\n")
            if not line:
                continue
            if delay > 0:
                await asyncio.sleep(delay)
            yield line


class StdinLogSource(ILogSource):
    """Read lines from stdin. Useful for ``cat ... | pipelinex run``."""

    async def stream(self) -> AsyncIterator[str]:
        loop = asyncio.get_running_loop()
        while True:
            line = await loop.run_in_executor(None, sys.stdin.readline)
            if not line:
                return
            yield line.rstrip("\n")


class IterableLogSource(ILogSource):
    """Source backed by any iterable of strings — used by tests and the
    synthetic generator wrapper.
    """

    def __init__(self, lines: Iterable[str]) -> None:
        self._lines = lines

    async def stream(self) -> AsyncIterator[str]:
        for line in self._lines:
            yield line
