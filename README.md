# PipelineX

> Extensible log analytics engine with pluggable stages and concurrent processing.

PipelineX ingests log streams, processes them through a configurable
pipeline of stages (parse → validate → enrich → sessionize → detect),
and exposes the results via a FastAPI query layer. Built as a portfolio
project to demonstrate concurrent log processing, SOLID architecture,
and quantitative anomaly detection on real-world labeled datasets.

```
   ┌──────────────┐  raw   ┌─────────────────────────────────────────────┐  batch   ┌──────────────┐
   │ FileLogSource├───────▶│  Pipeline (N async workers, bounded queue)  ├─────────▶│  Repository  │
   │ StdinSource  │        │  parse → validate → enrich → sessionize     │          │  Postgres or │
   │ Synthetic    │        │       → detectors                            │          │  in-memory   │
   └──────────────┘        └────────────────────┬────────────────────────┘          └──────────────┘
                                                │
                                          BlockTraceClosed
                                                │
                                                ▼
                                          ┌──────────┐    ┌────────────────────┐
                                          │ EventBus ├───▶│ SequenceDetector   │
                                          │          ├───▶│ ConsoleAlerts      │
                                          │          ├───▶│ MetricsListener    │
                                          └──────────┘    └────────────────────┘
```

## Highlights

- **Async concurrency engine**: bounded queue, batch persistence,
  graceful shutdown. ~46K records/sec sustained on 8 workers; verified
  zero record loss under backpressure by both property tests and a
  dedicated benchmark.
- **Six classical design patterns** each earning their place: Strategy,
  Factory, Decorator, Observer, Chain of Responsibility, Repository.
  See `docs/design-patterns.md`.
- **Four anomaly detectors** with deliberately different mathematical
  foundations: Z-Score, IQR, CUSUM (numeric), and a sequence-aware
  n-gram detector for HDFS block traces.
- **Two real labeled datasets** with quantitative F1 against published
  ground truth: HDFS_v1 (sequence-shaped anomalies) and BGL (point
  anomalies). See `BENCHMARKS.md`.
- **Mypy strict mode** clean. Ruff clean. >85% coverage. ~200 unit +
  property tests including a Hypothesis-based records-conservation
  invariant over the executor.

## Quickstart

```bash
# 1. Install
python3.12 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

# 2. Get datasets (~2.2 GB, one-time)
bash scripts/download_datasets.sh

# 3. Run an HDFS pipeline end-to-end against the 2k sample
pipelinex run configs/hdfs_v1.yaml \
    --source-path data/HDFS_v1_sample/HDFS_2k.log

# 4. Throughput benchmarks
python scripts/benchmark_throughput.py --all

# 5. Train + evaluate the sequence detector against HDFS_v1 ground truth
python scripts/train_sequence_model.py
python scripts/evaluate_hdfs_sequence.py

# 6. Evaluate point detectors against BGL
python scripts/evaluate_bgl_point.py

# 7. Start the query API
pipelinex serve
```

## Repository layout

```
src/pipelinex/
├── core/                # interfaces, models, pipeline executor, config, builder
├── stages/
│   ├── parsing/         # HDFSParser, BGLParser, factory, parser-stage adapter
│   ├── validation/      # schema/size/rate-limit validators + chain
│   ├── enrichment/      # template_matcher
│   ├── sessionizing/    # BlockSessionizerStage
│   └── decorators/      # timing, retry, logging
├── detectors/           # ZScore, IQR, CUSUM, Sequence + factory
├── persistence/         # InMemoryLogRepository, PostgresLogRepository, schema.sql
├── events/              # EventBus, listeners, BlockTraceClosed event
├── observability/       # MetricsRegistry
├── api/                 # FastAPI app + routes + schemas
└── cli.py               # `pipelinex` command-line entry

tests/
├── unit/                # ~190 fast tests
├── integration/         # Postgres + API tests
└── property/            # Hypothesis-based robustness tests

scripts/
├── download_datasets.sh
├── generate_synthetic_logs.py
├── train_sequence_model.py
├── evaluate_hdfs_sequence.py
├── evaluate_bgl_point.py
└── benchmark_throughput.py

configs/                 # YAML pipeline configurations
docs/                    # design-patterns / concurrency-model / extending / datasets
data/                    # downloaded datasets (gitignored except samples)
```

## Engineering principles

1. **Open/Closed.** Adding a new parser, validator, detector, or
   listener requires zero changes to core code. See
   `docs/extending-pipelinex.md`.
2. **Dependency Inversion.** The pipeline core depends only on
   interfaces in `core/interfaces.py`. Concrete implementations are
   wired in via factories or the YAML builder.
3. **Liskov substitution.** `InMemoryLogRepository` and
   `PostgresLogRepository` pass the same scenarios — verified by a
   shared integration test suite gated on `POSTGRES_DSN`.
4. **Backpressure over buffering.** The bounded `asyncio.Queue` is the
   only place producer/consumer are coupled. Verified by property
   tests and a dedicated `--backpressure` benchmark.
5. **Strict typing.** Mypy strict, no `Any` leaks, no untyped function
   defs. Bug surface measured-and-fixed at compile time.

## Documentation

- [`ARCHITECTURE.md`](ARCHITECTURE.md) — load-bearing decisions and
  rationale.
- [`docs/design-patterns.md`](docs/design-patterns.md) — six patterns
  with where, why, what-would-break.
- [`docs/concurrency-model.md`](docs/concurrency-model.md) — task
  layout, failure modes, backpressure.
- [`docs/extending-pipelinex.md`](docs/extending-pipelinex.md) — how
  to add new parsers, detectors, listeners.
- [`docs/datasets.md`](docs/datasets.md) — dataset provenance, format,
  why these two.
- [`BENCHMARKS.md`](BENCHMARKS.md) — measured throughput, latency, F1.
- [`CONTRIBUTING.md`](CONTRIBUTING.md) — dev setup, code style, PR
  expectations.

## Status

**Complete.** Build runs end-to-end from raw logs through to API
queries. All tests green on Python 3.12. F1 / throughput numbers in
`BENCHMARKS.md` are reproducible from a fresh checkout.

## License

MIT. See [`LICENSE`](LICENSE).
