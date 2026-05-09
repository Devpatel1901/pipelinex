"""Factory for parsers.

The Factory pattern translates a configuration string (``"hdfs"``, ``"bgl"``,
``"json"``) into a concrete ``IParser`` instance. New parsers are added by
calling ``register()``; no core-code changes required.

Auto-detection (via ``detect()``) tries each registered parser's
``can_parse`` until one matches — useful for tests and mixed inputs, but
production runs should pin a parser via YAML.
"""

from __future__ import annotations

from collections.abc import Callable

from pipelinex.core.exceptions import ConfigurationError, ParserError
from pipelinex.core.interfaces import IParser

ParserCtor = Callable[[], IParser]


class ParserFactory:
    """Registry mapping kind → parser constructor."""

    def __init__(self) -> None:
        self._registry: dict[str, ParserCtor] = {}

    def register(self, kind: str, ctor: ParserCtor) -> None:
        if kind in self._registry:
            raise ConfigurationError(f"parser already registered: {kind}")
        self._registry[kind] = ctor

    def create(self, kind: str) -> IParser:
        try:
            return self._registry[kind]()
        except KeyError as e:
            raise ConfigurationError(
                f"unknown parser kind: {kind!r} (registered: {sorted(self._registry)})"
            ) from e

    def detect(self, raw: str) -> IParser:
        """Return the first parser whose ``can_parse`` claims this line."""
        for kind, ctor in self._registry.items():
            parser = ctor()
            if parser.can_parse(raw):
                return parser
            del kind  # pragma: no cover  (no-op, satisfies linter on unused var)
        raise ParserError(f"no registered parser claims line: {raw[:80]!r}")

    def known_kinds(self) -> list[str]:
        return sorted(self._registry)


def default_factory() -> ParserFactory:
    """Return a ``ParserFactory`` with all built-in parsers registered."""
    from pipelinex.stages.parsing.hdfs_parser import HDFSParser
    from pipelinex.stages.parsing.hdfs_structured_csv_parser import HDFSStructuredCSVParser

    f = ParserFactory()
    f.register("hdfs", HDFSParser)
    f.register("hdfs_structured_csv", HDFSStructuredCSVParser)
    return f
