# Design Patterns in PipelineX

PipelineX uses six classical patterns. Each appears here exactly once, where
it earns its place. The "why this pattern, why here" justification matters
as much as the implementation — interview panels will probe both.

---

## 1. Strategy — Pluggable Algorithms

**Used for:** Parsers, Anomaly Detectors

**Why:** The set of log formats and the set of anomaly-detection algorithms
both grow over time. A naive design would put `if format == "hdfs": ...`
inside every stage that touches the format. Strategy inverts that: each
algorithm is a class implementing a small interface, and the rest of the
system depends only on the interface.

**Where:**
- `src/pipelinex/core/interfaces.py::IParser` — `parse(raw) -> LogRecord`
- `src/pipelinex/stages/parsing/hdfs_parser.py::HDFSParser`
- `src/pipelinex/core/interfaces.py::IAnomalyDetector` — `detect(record) -> AnomalyEvent | None`
- `src/pipelinex/detectors/zscore.py::ZScoreDetector`

**What would break without it:** Adding a fourth log format would require
modifying every detector, repository, and validation rule that branches on
format. The Open/Closed principle would be violated.

---

## 2. Factory — Centralized Object Creation

**Used for:** `ParserFactory` (more factories arrive on Day 13)

**Why:** Configuration arrives as strings (`{"parser": "hdfs"}`). Strings are
not classes. The Factory translates a string-keyed config into a concrete
object and centralizes the registry of which keys map to which classes —
a single place to register a new parser without modifying existing code.

**Where:**
- `src/pipelinex/stages/parsing/factory.py::ParserFactory`
- `default_factory()` registers all built-in parsers; user code calling
  `factory.register("custom", CustomParser)` plugs in a new one with zero
  modification to the factory itself.

**What would break without it:** Configuration loading would need a
hard-coded `match parser_kind: case "hdfs": ...` block updated for every new
parser — worse, that block would live in every place that constructs a
parser from config.

---

## 3. Decorator — Cross-Cutting Concerns

**Used for:** `TimingDecorator`, `RetryDecorator`, `LoggingDecorator`

**Why:** Stages have logical concerns (parse a record). Operational concerns
(time how long it took, retry on transient failure, log entry/exit) are
orthogonal — they apply to *any* stage. Putting them inside each stage
duplicates code and forces a single combination of behaviours per stage.
Decorators wrap the stage and themselves implement `IPipelineStage`, so
they stack arbitrarily.

**Where:**
- `src/pipelinex/stages/decorators/base.py::BaseStageDecorator`
- `src/pipelinex/stages/decorators/timing.py::TimingDecorator`
- `src/pipelinex/stages/decorators/retry.py::RetryDecorator`
- `src/pipelinex/stages/decorators/logging.py::LoggingDecorator`

**Composition example:**

```python
stage = TimingDecorator(
    RetryDecorator(
        LoggingDecorator(ParserStage(HDFSParser())),
        max_retries=3,
    )
)
```

The retry decorator only retries `TransientStageError`. Combined with the
custom exception hierarchy, the retry policy is precise without coupling to
the inner stage's logic.

**What would break without it:** Every stage would carry its own retry/
timing/logging code, and turning any of those off would require a
configuration flag in every stage class.

---

## 4. Observer — Anomaly Alerting (Day 12)

**Used for:** `EventBus` + `IEventListener` (incl. `BlockTraceClosed`)

**Why:** Anomaly detectors should not know about Slack, email, audit logs,
or whatever other consumers care. The EventBus inverts the dependency:
detectors publish events, listeners subscribe. New listeners require zero
changes to detectors.

**Where:**
- (Day 12) `src/pipelinex/events/bus.py::EventBus`
- (Day 12) `src/pipelinex/events/events.py` — defines `BlockTraceClosed`
- `src/pipelinex/core/interfaces.py::IEventListener`

The `BlockSessionizerStage` (Day 13) publishes `BlockTraceClosed`; the
`SequenceAnomalyDetector` subscribes as a listener — this lets the
sequence-detection path live entirely off the EventBus, without forcing a
new abstraction for fan-out.

**What would break without it:** Every detector would import every
notification mechanism, or worse, the pipeline would have to know about
every notification mechanism to wire them up.

---

## 5. Chain of Responsibility — Validation (Day 11)

**Used for:** `ValidationStage` running schema, size, and rate-limit
validators in order.

**Why:** Validation is naturally sequential and short-circuiting:
schema-invalid records should fail fast before being size-checked, size-
violators should fail before being rate-limit-checked. Each validator is
independent, reorderable in YAML, and can be added without modifying the
others.

**Where:** (Day 11) `src/pipelinex/stages/validation/`

---

## 6. Repository — Persistence Abstraction

**Used for:** `ILogRepository` with `InMemoryLogRepository` and
`PostgresLogRepository` implementations.

**Why:** Stages should not know about SQL, SQLAlchemy, or asyncpg. The
Repository hides persistence behind a clean interface, which (a) makes the
pipeline testable without a database and (b) makes the system swappable to
a different DB engine without touching pipeline code.

**Where:**
- `src/pipelinex/core/interfaces.py::ILogRepository`
- `src/pipelinex/persistence/memory_repo.py::InMemoryLogRepository`
- (Day 8) `src/pipelinex/persistence/postgres_repo.py::PostgresLogRepository`

**The Liskov substitution test:** Both repository implementations are
expected to pass an identical test suite — that is the load-bearing test of
the abstraction. If a test passes against InMemory but fails against
Postgres, either the abstraction is leaky or one implementation is wrong.

**What would break without it:** Replacing PostgreSQL with (say) ClickHouse
would mean modifying every place that touches `_records` or `_traces`. The
in-memory repo wouldn't exist, so unit tests would either need a real DB
or live with a flaky homemade fake.

---

## Considered alternatives (and why they're not in the codebase)

- **Visitor pattern over `LogRecord`** — would let stages dispatch on
  record subtypes, but `LogRecord` is intentionally a single mutable type;
  Strategy via stages is a better fit.
- **Builder pattern for `LogRecord`** — overkill; defaults via dataclass
  field defaults handle all current needs. We can switch to Pydantic
  `model_validate` if we ever take untrusted input.
- **PCA-based sequence detector** — considered for HDFS sequence anomaly
  detection. Rejected for v1 in favour of n-gram (Day 13) because PCA needs
  scikit-learn + careful fit/transform separation, and published HDFS
  results show n-gram is comparable on this corpus at a fraction of the
  complexity. PCA is documented here as a v2 alternative.
