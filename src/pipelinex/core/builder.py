"""Build a fully-wired pipeline from a ``PipelineConfig``.

This is where YAML meets factories. The builder is the single place that
knows the order of stages and how each piece is constructed; the rest of
the system never sees configuration.

Standard stage order::

    parse  →  validate  →  template_matcher  →  sessionizer  →  detectors

The repository, sequence detector, and event bus are returned alongside
the executor so the caller can subscribe listeners or query results.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from pipelinex.core.config import PipelineConfig
from pipelinex.core.exceptions import ConfigurationError
from pipelinex.core.interfaces import (
    IAnomalyDetector,
    ILogRepository,
    ILogSource,
    IPipelineStage,
    ISequenceAnomalyDetector,
)
from pipelinex.core.pipeline import PipelineExecutor
from pipelinex.core.sources import FileLogSource, IterableLogSource, StdinLogSource
from pipelinex.detectors.detector_stage import DetectorStage
from pipelinex.detectors.factory import default_factory as default_detector_factory
from pipelinex.detectors.sequence import SequenceAnomalyDetector, SequenceModel
from pipelinex.detectors.sequence_loglikelihood import (
    SequenceLogLikelihoodDetector,
    SequenceStatsModel,
)
from pipelinex.events.bus import EventBus
from pipelinex.events.events import BlockTraceClosed
from pipelinex.persistence.memory_repo import InMemoryLogRepository
from pipelinex.stages.enrichment.template_matcher import TemplateMatcherStage
from pipelinex.stages.parsing.factory import default_factory as default_parser_factory
from pipelinex.stages.parsing.parser_stage import ParserStage
from pipelinex.stages.sessionizing.block_sessionizer import BlockSessionizerStage
from pipelinex.stages.validation.chain import ValidationStage
from pipelinex.stages.validation.rate_limit_validator import RateLimitValidator
from pipelinex.stages.validation.schema_validator import SchemaValidator
from pipelinex.stages.validation.size_validator import SizeValidator

logger = logging.getLogger(__name__)


@dataclass
class BuiltPipeline:
    """Aggregate returned by ``build_pipeline``."""

    executor: PipelineExecutor
    repository: ILogRepository
    bus: EventBus
    sessionizer: BlockSessionizerStage | None
    sequence_detector: ISequenceAnomalyDetector | None


def build_pipeline(config: PipelineConfig) -> BuiltPipeline:
    """Translate a typed config into a ready-to-run pipeline."""
    bus = EventBus()
    repository = _build_repository(config)

    source = _build_source(config)
    parser_stage = _build_parser_stage(config)
    validator_stage = _build_validation_stage(config)
    template_stage = _build_template_stage(config)
    sessionizer = _build_sessionizer(config, bus)
    detector_stages = _build_detector_stages(config, repository)
    sequence_detector = _build_sequence_detector(config, bus, repository)

    stages: list[IPipelineStage] = [parser_stage]
    if validator_stage is not None:
        stages.append(validator_stage)
    if template_stage is not None:
        stages.append(template_stage)
    if sessionizer is not None:
        stages.append(sessionizer)
    stages.extend(detector_stages)

    executor = PipelineExecutor(
        source=source,
        stages=stages,
        repository=repository,
        num_workers=config.runtime.num_workers,
        queue_size=config.runtime.queue_size,
        batch_size=config.runtime.batch_size,
        flush_interval_s=config.runtime.flush_interval_s,
    )

    return BuiltPipeline(
        executor=executor,
        repository=repository,
        bus=bus,
        sessionizer=sessionizer,
        sequence_detector=sequence_detector,
    )


# ---------- private builders ----------


def _build_source(config: PipelineConfig) -> ILogSource:
    sc = config.source
    if sc.kind == "file":
        assert sc.path is not None  # guarded by Pydantic validator
        return FileLogSource(sc.path, encoding=sc.encoding, throttle_rps=sc.throttle_rps)
    if sc.kind == "stdin":
        return StdinLogSource()
    if sc.kind == "iterable":
        # Only used by tests that override the source post-build.
        return IterableLogSource(())
    raise ConfigurationError(f"unsupported source.kind: {sc.kind}")


def _build_repository(config: PipelineConfig) -> ILogRepository:
    rc = config.repository
    if rc.kind == "memory":
        return InMemoryLogRepository()
    if rc.kind == "postgres":
        from pipelinex.persistence.postgres_repo import PostgresLogRepository

        assert rc.dsn is not None  # guarded by Pydantic validator
        return PostgresLogRepository.from_dsn(rc.dsn)
    raise ConfigurationError(f"unsupported repository.kind: {rc.kind}")


def _build_parser_stage(config: PipelineConfig) -> ParserStage:
    factory = default_parser_factory()
    parser = factory.create(config.parser.kind)
    return ParserStage(parser)


def _build_validation_stage(config: PipelineConfig) -> ValidationStage | None:
    if not config.validation:
        return None
    from pipelinex.core.interfaces import IValidator

    validators: list[IValidator] = []
    for vc in config.validation:
        kwargs = vc.model_dump(exclude={"kind"})
        if vc.kind == "schema":
            validators.append(SchemaValidator(**kwargs))
        elif vc.kind == "size":
            validators.append(SizeValidator(**kwargs))
        elif vc.kind == "rate_limit":
            validators.append(RateLimitValidator(**kwargs))
        else:  # pragma: no cover — Pydantic Literal already constrains this
            raise ConfigurationError(f"unsupported validator.kind: {vc.kind}")
    return ValidationStage(validators)


def _build_template_stage(config: PipelineConfig) -> TemplateMatcherStage | None:
    if not config.template_matcher.enabled:
        return None
    return TemplateMatcherStage(templates_path=config.template_matcher.templates_path)


def _build_sessionizer(
    config: PipelineConfig, bus: EventBus
) -> BlockSessionizerStage | None:
    if not config.sessionizer.enabled:
        return None
    sc = config.sessionizer
    from uuid import uuid4

    return BlockSessionizerStage(
        bus=bus,
        pipeline_run_id=uuid4(),  # the executor has its own run id; this is OK
        idle_window_s=sc.idle_window_s,
        max_records_per_trace=sc.max_records_per_trace,
        max_open_traces=sc.max_open_traces,
        flush_interval_s=sc.flush_interval_s,
    )


def _build_detector_stages(
    config: PipelineConfig, repository: ILogRepository
) -> list[IPipelineStage]:
    if not config.detectors:
        return []
    factory = default_detector_factory()
    stages: list[IPipelineStage] = []
    for dc in config.detectors:
        # Drop `None` kwargs so detectors that don't take a parameter
        # (e.g. feature_window has no metric_key) aren't fed it. Concrete
        # detectors keep their own default values for omitted fields.
        kwargs = {
            k: v
            for k, v in dc.model_dump(exclude={"kind"}).items()
            if v is not None
        }
        detector: IAnomalyDetector = factory.create_numeric(dc.kind, **kwargs)
        stages.append(DetectorStage(detector, repository))
    return stages


def _build_sequence_detector(
    config: PipelineConfig,
    bus: EventBus,
    repository: ILogRepository,
) -> ISequenceAnomalyDetector | None:
    if config.sequence_detector is None:
        return None
    sd_cfg = config.sequence_detector
    factory = default_detector_factory()
    # Forward detector-specific kwargs (e.g. `alpha` for loglikelihood) by
    # pulling them out of the model_extra bucket (extra="allow" on the schema).
    extra_kwargs = dict(sd_cfg.model_extra or {})
    extra_kwargs.pop("model_path", None)  # consumed below, not passed to ctor
    detector = factory.create_sequence(
        sd_cfg.kind, threshold=sd_cfg.threshold, **extra_kwargs
    )

    if sd_cfg.model_path:
        try:
            if isinstance(detector, SequenceAnomalyDetector):
                detector.set_model(SequenceModel.load(sd_cfg.model_path))
            elif isinstance(detector, SequenceLogLikelihoodDetector):
                detector.set_model(SequenceStatsModel.load(sd_cfg.model_path))
        except (FileNotFoundError, OSError) as e:
            logger.warning(
                "sequence model not available at %s (%s); detector starts empty",
                sd_cfg.model_path,
                e,
            )

    async def on_trace_closed(event: BlockTraceClosed) -> None:
        anomaly = await detector.detect_trace(event.trace)
        if anomaly is not None:
            await repository.save_anomaly(anomaly)

    bus.subscribe(BlockTraceClosed, on_trace_closed)
    return detector
