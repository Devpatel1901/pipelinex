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


class SourceError(PipelineXError):
    """Log source failure (file not found, stream closed, etc.)."""
