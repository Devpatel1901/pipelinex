# Extending PipelineX

The point of every interface in `core/interfaces.py` is to let you add
new behaviour without modifying core code. This page walks through the
concrete steps for each kind of extension.

## Add a new parser

1. Create `src/pipelinex/stages/parsing/my_parser.py`:

   ```python
   from pipelinex.core.interfaces import IParser
   from pipelinex.core.models import LogRecord, Severity

   class MyParser(IParser):
       @property
       def name(self) -> str:
           return "my_format"

       def can_parse(self, raw: str) -> bool:
           return raw.startswith("MYFMT|")

       def parse(self, raw: str) -> LogRecord:
           # ... build a LogRecord ...
           return LogRecord(message=raw, severity=Severity.INFO)
   ```

2. Register it in `src/pipelinex/stages/parsing/factory.py::default_factory`:

   ```python
   from pipelinex.stages.parsing.my_parser import MyParser
   f.register("my_format", MyParser)
   ```

3. Use it via YAML: `parser: { kind: my_format }`.

That's it. No core code changes. No detector changes. No repository
changes.

## Add a new validator

1. Implement `IValidator` in `src/pipelinex/stages/validation/`.
2. Add a literal to `ValidatorConfig` in `core/config.py`.
3. Add the case in `_build_validation_stage` in `core/builder.py`.
4. YAML: `validation: - { kind: my_validator, my_param: 42 }`.

## Add a new numeric detector

1. Implement `IAnomalyDetector` in `src/pipelinex/detectors/`.
2. Register in `default_factory()` of `detectors/factory.py`.
3. YAML: `detectors: - { kind: my_detector, threshold: 5.0 }`.

## Add a new sequence detector

The cleanest path mirrors `SequenceAnomalyDetector`:

1. Implement `ISequenceAnomalyDetector` in `src/pipelinex/detectors/`.
2. Register via `register_sequence` in `default_factory()`.
3. The builder will subscribe it to `BlockTraceClosed` automatically when
   you set `sequence_detector: { kind: my_seq_det }` in YAML.

## Add a new event listener

```python
from pipelinex.events.events import AnomalyEventPublished

async def slack_alert(event: AnomalyEventPublished) -> None:
    await send_to_slack(f"anomaly: {event.anomaly}")

bus.subscribe(AnomalyEventPublished, slack_alert)
```

No registry needed — listeners are subscribed at runtime.

## Swap the repository

Implement `ILogRepository` and pass it to `create_app(repository)` (for
the API) or to `PipelineExecutor(repository=...)` (for the pipeline
itself). The `default_factory()` machinery doesn't know about
repositories — that's deliberate, because repositories often need
runtime parameters (DSNs, credentials) that don't belong in YAML.

## Add a new YAML field

1. Edit the relevant section of `core/config.py` (e.g., `RuntimeConfig`).
2. Wire it into `core/builder.py` if it changes pipeline construction.
3. Add a regression test in `tests/unit/test_config.py`.

The Pydantic models in `config.py` use `extra="forbid"`, so
unrecognized YAML fields fail loudly at load time. This is intentional
— it catches typos before they cause silent misconfiguration.
