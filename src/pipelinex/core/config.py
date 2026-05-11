"""YAML-driven configuration for the pipeline.

The config schema is intentionally explicit (rather than nested-dict free-
form) so Pydantic validation catches typos and type errors at load time —
fail fast with a clear message rather than crash mid-run.

Example::

    source:
      kind: file
      path: data/HDFS/HDFS.log

    parser:
      kind: hdfs

    validation:
      - { kind: schema, require_message: true }
      - { kind: size, max_message_bytes: 65536 }

    sessionizer:
      enabled: true
      idle_window_s: 30.0
      max_open_traces: 50000

    detectors:
      - { kind: z_score, threshold: 3.0, window_size: 200 }

    sequence_detector:
      kind: sequence_ngram
      model_path: models/hdfs_ngram_v1.json

    repository:
      kind: memory

    runtime:
      num_workers: 4
      queue_size: 1000
      batch_size: 100
      flush_interval_s: 1.0
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from pipelinex.core.exceptions import ConfigurationError


class SourceConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    kind: Literal["file", "stdin", "iterable"] = "file"
    path: str | None = None
    encoding: str = "utf-8"
    throttle_rps: float | None = None

    @model_validator(mode="after")
    def _path_required_for_file(self) -> SourceConfig:
        if self.kind == "file" and not self.path:
            raise ValueError("source.path is required when kind=file")
        return self


class ParserConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    kind: str = "hdfs"


class ValidatorConfig(BaseModel):
    model_config = ConfigDict(extra="allow")
    kind: Literal["schema", "size", "rate_limit"]


class DetectorConfig(BaseModel):
    model_config = ConfigDict(extra="allow")
    kind: str
    metric_key: str = "metric"


class SequenceDetectorConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    kind: str = "sequence_ngram"
    model_path: str | None = None
    threshold: float = 0.0


class SessionizerConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    enabled: bool = False
    idle_window_s: float = 30.0
    max_records_per_trace: int = 1000
    max_open_traces: int = 50_000
    flush_interval_s: float = 5.0


class TemplateMatcherConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    enabled: bool = False
    templates_path: str = "data/HDFS/preprocessed/HDFS.log_templates.csv"


class RepositoryConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    kind: Literal["memory", "postgres"] = "memory"
    dsn: str | None = None

    @model_validator(mode="after")
    def _dsn_required_for_postgres(self) -> RepositoryConfig:
        if self.kind == "postgres" and not self.dsn:
            raise ValueError("repository.dsn is required when kind=postgres")
        return self


class RuntimeConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    num_workers: int = Field(default=4, ge=1, le=64)
    queue_size: int = Field(default=1000, ge=1)
    batch_size: int = Field(default=100, ge=1)
    flush_interval_s: float = Field(default=1.0, gt=0.0)


class PipelineConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: SourceConfig
    parser: ParserConfig = Field(default_factory=ParserConfig)
    validation: list[ValidatorConfig] = Field(default_factory=list)
    template_matcher: TemplateMatcherConfig = Field(default_factory=TemplateMatcherConfig)
    sessionizer: SessionizerConfig = Field(default_factory=SessionizerConfig)
    detectors: list[DetectorConfig] = Field(default_factory=list)
    sequence_detector: SequenceDetectorConfig | None = None
    repository: RepositoryConfig = Field(default_factory=RepositoryConfig)
    runtime: RuntimeConfig = Field(default_factory=RuntimeConfig)


def load_config(path: str | Path) -> PipelineConfig:
    """Load and validate a pipeline YAML from disk."""
    p = Path(path)
    if not p.exists():
        raise ConfigurationError(f"config file not found: {p}")
    try:
        with p.open(encoding="utf-8") as fh:
            raw = yaml.safe_load(fh) or {}
    except yaml.YAMLError as e:
        raise ConfigurationError(f"failed to parse YAML at {p}: {e}") from e
    return parse_config(raw)


def parse_config(data: dict[str, Any]) -> PipelineConfig:
    """Validate a raw dict and return a typed ``PipelineConfig``."""
    try:
        return PipelineConfig.model_validate(data)
    except ValidationError as e:
        raise ConfigurationError(f"config validation failed: {e}") from e
