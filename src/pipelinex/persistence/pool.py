"""Postgres connection-pool sizing tied to the pipeline's ``num_workers``.

SQLAlchemy 2.0's async pool needs explicit sizing or it falls back to the
asyncpg defaults (typically 5-10 connections), which can saturate before
the bounded queue does and silently break backpressure. By tying pool
size to ``num_workers`` we make the saturation threshold predictable.

Default rule of thumb:

- ``pool_size = num_workers * 2``         # one connection per worker plus headroom
- ``max_overflow = num_workers``           # short-term burst capacity
- ``pool_timeout_s = 5.0``                 # raise TimeoutError after 5s waiting
- ``pool_pre_ping = True``                 # quietly drop dead connections

Override any of these via ``PoolConfig`` (e.g. from YAML).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Final

DEFAULT_POOL_TIMEOUT_S: Final = 5.0


@dataclass(frozen=True)
class PoolConfig:
    """Overrides for the SQLAlchemy async pool. ``None`` means "use the
    default derived from ``num_workers``".
    """

    pool_size: int | None = None
    max_overflow: int | None = None
    pool_timeout_s: float | None = None
    pool_pre_ping: bool | None = None


def build_pool_kwargs(num_workers: int, overrides: PoolConfig | None = None) -> dict[str, Any]:
    """Return the kwargs to pass into ``create_async_engine``.

    Sizing rule: ``pool_size = num_workers * 2``, ``max_overflow = num_workers``,
    timeout 5 s, pre-ping on. Any field on ``overrides`` (when not None)
    wins over the default.
    """
    if num_workers < 1:
        raise ValueError("num_workers must be >= 1")

    pool_size = num_workers * 2
    max_overflow = num_workers
    pool_timeout_s = DEFAULT_POOL_TIMEOUT_S
    pool_pre_ping = True

    if overrides is not None:
        if overrides.pool_size is not None:
            pool_size = overrides.pool_size
        if overrides.max_overflow is not None:
            max_overflow = overrides.max_overflow
        if overrides.pool_timeout_s is not None:
            pool_timeout_s = overrides.pool_timeout_s
        if overrides.pool_pre_ping is not None:
            pool_pre_ping = overrides.pool_pre_ping

    return {
        "pool_size": pool_size,
        "max_overflow": max_overflow,
        "pool_timeout": pool_timeout_s,
        "pool_pre_ping": pool_pre_ping,
    }
