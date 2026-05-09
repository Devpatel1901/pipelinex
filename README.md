# PipelineX

> Extensible log analytics engine with pluggable stages and concurrent processing.

PipelineX is a single-node async pipeline that ingests log streams, processes them through configurable stages (parse → validate → enrich → sessionize → detect anomalies → persist), and exposes results via a FastAPI query layer. Built to demonstrate concurrent log processing, SOLID architecture, and quantitative anomaly detection on real-world labeled datasets (HDFS_v1, BGL).

## Highlights

- **Async concurrency engine** with bounded queue, batch persistence, and graceful shutdown — no record loss under backpressure.
- **Pluggable parsers** (HDFS, BGL, JSON) registered via a factory — adding a new format requires zero core changes.
- **Four anomaly detectors** with different mathematical foundations: Z-Score, IQR, CUSUM (point/numeric), and a sequence-based n-gram detector for block-trace anomalies.
- **Two evaluation tracks** with quantitative F1 against published labels: HDFS_v1 (per-block-sequence) and BGL (per-line point).
- **Synthetic generator** for controlled throughput benchmarking — separates correctness from performance datasets.
- **Strict typing** (mypy strict) and **>85% test coverage** including property-based tests via `hypothesis`.

## Quickstart

```bash
# 1. Install
python3.11 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

# 2. Download datasets (~2.2 GB; one-time)
bash scripts/download_datasets.sh

# 3. Run end-to-end on the HDFS sample
pipelinex run --config configs/hdfs_v1.yaml --source-path data/samples/HDFS_sample_100.log

# 4. Run benchmarks
python scripts/benchmark_throughput.py --workers-sweep
```

## Architecture

See `ARCHITECTURE.md` for the full design rationale and `docs/design-patterns.md` for the six design patterns used.

```
   FileLogSource ──┐
                   ├──▶ Pipeline ──▶ [parse → validate → enrich → sessionize → detect] ──▶ Repository
   StdinSource    ─┤        │                                            │
   SyntheticSource┘   bounded asyncio.Queue                              ▼
                                                                      EventBus ──▶ Listeners
```

## Datasets

| Dataset | Size | Anomaly Model | Purpose |
|---|---|---|---|
| HDFS_v1 | 1.47 GB | Per-block sequence | Sequence detector evaluation (F1) |
| BGL | 709 MB | Per-line point | Z-Score / IQR / CUSUM evaluation |
| Synthetic | 0 MB | Configurable | Throughput benchmarking |

Both real datasets are from [logpai/loghub](https://github.com/logpai/loghub) hosted on Zenodo.

## License

MIT
