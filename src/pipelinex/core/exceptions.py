"""Exception hierarchy for PipelineX.

The ``Transient`` vs ``Fatal`` split lets the ``RetryDecorator`` make smart
decisions: retry transient errors with backoff, propagate fatal ones
immediately. Stage authors choose which to raise based on the error semantics.
"""

from __future__ import annotations


class PipelineXError(Exception):
    """Base exception for all PipelineX errors."""


class ConfigurationError(PipelineXError):
    """Invalid configuration (e.g., unknown parser kind, bad YAML)."""


class StageError(PipelineXError):
    """Base for errors raised inside a pipeline stage."""


class TransientStageError(StageError):
    """Temporary failure — retry is appropriate (network blip, lock contention)."""


class FatalStageError(StageError):
    """Unrecoverable — pipeline should skip the record or halt."""


class ParserError(StageError):
    """Parsing failed."""


class ValidationError(StageError):
    """Validation rejected the record."""


class RepositoryError(PipelineXError):
    """Data persistence failure."""


class PoolSaturatedError(RepositoryError):
    """Connection-pool checkout timed out.

    Raised when SQLAlchemy's async pool exhausts both ``pool_size`` and
    ``max_overflow`` connections and the caller waits past
    ``pool_timeout_s`` for a free one. Translated from
    ``sqlalchemy.exc.TimeoutError`` so the pipeline can distinguish
    saturation from other repository failures.
    """


class LoadSheddingError(RepositoryError):
    """Circuit breaker is OPEN — request shed without touching the DB.

    Distinct from ``PoolSaturatedError``: the breaker has decided the
    repository is unhealthy and is fast-failing to protect the rest of
    the pipeline. The executor counts these as ``records_shed`` rather
    than ``records_failed`` so the conservation invariant remains
    ``processed + failed + shed == lines_in``.
    """


class SourceError(PipelineXError):
    """Log source failure (file not found, stream closed, etc.)."""
