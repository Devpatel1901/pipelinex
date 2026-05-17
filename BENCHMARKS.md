# PipelineX Benchmarks

This document carries the headline numbers for the project. All results
are reproducible from a fresh checkout: the raw command for each
section is included below the result.

## Test environment

- Python 3.12.5 on macOS Darwin 25.3.0 (Apple Silicon).
- 8-core CPU; tests run as a normal user process, no isolation.
- Datasets: HDFS_v1 (1.47 GB, 11.17M lines) and BGL (709 MB, 4.75M
  lines), both fetched via `bash scripts/download_datasets.sh`.

The numbers in this file are deliberately *honest*: not best-of-N runs,
not warmed-up averages — these are typical end-to-end results from one
clean run. A real production deployment with tuned Postgres + a
production CPU would do meaningfully better; an interview demo on a
laptop should not pretend otherwise.

---

## Throughput — synthetic HDFS workload

Records-per-second sustained when streaming 50,000 synthetic HDFS-format
lines through `ParserStage` into `InMemoryLogRepository`. The work is
parser-only (no validation, no enrichment, no detectors) — this measures
the executor's overhead, not full-pipeline throughput.

| Workers | Records | Failed | Elapsed | Records/sec |
|---|---|---|---|---|
| 1 | 50,000 | 0 | 1.05s | 47,665 |
| 2 | 50,000 | 0 | 1.10s | 45,411 |
| 4 | 50,000 | 0 | 1.07s | 46,662 |
| 8 | 50,000 | 0 | 1.08s | 46,125 |

The flat scaling tells a clear story: at this work-per-record (a single
regex), the asyncio event loop itself is the bottleneck, not stage
serialization. The plan's headline goal was greater than or equal to
20K rec/s at 8 workers; we're at ~46K, comfortably above target.

```bash
python scripts/benchmark_throughput.py --workers-sweep --total 50000
```

## Latency

Per-record p50/p95/p99 measured by stamping `t_ingest` on each record
at producer time and computing `(now - t_ingest)` just before
persistence. This is *ingest -> pre-batch* latency, not
ingest-to-durable.

| Workers | Samples | p50 | p95 | p99 |
|---|---|---|---|---|
| 4 | 50,000 | 7 us | 8 us | 8 us |

Sub-10 us p99 is the asyncio overhead dominating: the queue put,
context switch, dispatch, and parser call are all in-process Python.

```bash
python scripts/benchmark_throughput.py --latency --total 50000
```

## Backpressure verification

The plan promises "zero data loss under backpressure" — the bounded
queue backpressures the producer instead of leaking memory. Verified by
injecting a `SlowRepository` (50ms per `save_batch`) on a queue size of
100, with a sidecar coroutine sampling `queue.qsize()`:

| Records | Queue size | Max observed depth | Saturation | Records lost |
|---|---|---|---|---|
| 5,000 | 100 | 100 / 100 | yes | 0 |

The producer's wall time dilated to match the consumer's pace. No
records were lost. The records-conservation property is also pinned by
a Hypothesis test that exercises arbitrary worker/queue/batch counts.

```bash
python scripts/benchmark_throughput.py --backpressure --total 5000
```

---

<!-- BEGIN: hdfs-sequence -->
## HDFS_v1 — Sequence-anomaly detection (KL-divergence)

| detector | blocks_scored | TP | FP | FN | precision | recall | F1 |
|---|---|---|---|---|---|---|---|
| sequence_kl | 575061 | 12211 | 17842 | 4627 | 0.4063 | 0.7252 | 0.5208 |

_threshold = 0.3 (nats), n = 2, α = 0.5_
<!-- END: hdfs-sequence -->



### How to read these numbers

- **Universe:** 575,061 labeled blocks from `anomaly_label.csv`.
- **Ground truth anomalies:** 16,838 (~2.93%) — matches the canonical
  HDFS_v1 stats.
- **Method:** for each block, build the multiset of 2-grams over its
  event-id sequence; flag the block if at least one 2-gram never
  appeared in any *labeled-normal* block. Trained on the full normal
  set (no held-out split because we score against the full corpus).

### Why precision is 1.0 and recall is 0.28

Pure novel-2-gram detection can only fire on bigrams that *never*
appear in any normal block. By construction every fire is correct ->
precision = 1. But ~71% of true anomalies use only 2-grams that also
appear in some normal block (just in different proportions, lengths,
or positions). To raise recall we'd need a richer scoring metric:
2-gram frequency divergence (KL or chi-square), 3-grams, or a fully
Bayesian model. The plan documents the n-gram baseline as deliberate
v1 scope; v2 work would migrate to one of those richer methods.

```bash
python scripts/train_sequence_model.py            # ~3 min on 11.17M lines
python scripts/evaluate_hdfs_sequence.py          # ~3 min
```

---

<!-- BEGIN: bgl-point -->
## BGL — Anomaly detection (per-line label + per-window ensemble)

### Per-line label detector (trust-the-source)

| detector | TP | FP | FN | precision | recall | F1 |
|---|---|---|---|---|---|---|
| label (per-line) | 348460 | 0 | 0 | 1.0000 | 1.0000 | 1.0000 |

### Per-window ensemble

| detector | TP | FP | FN | precision | recall | F1 |
|---|---|---|---|---|---|---|
| window_features | 1793 | 2794 | 45 | 0.3909 | 0.9755 | 0.5581 |
| z_score | 157 | 811 | 1681 | 0.1622 | 0.0854 | 0.1119 |
| iqr | 510 | 2758 | 1328 | 0.1561 | 0.2775 | 0.1998 |
| cusum | 43 | 1208 | 1795 | 0.0344 | 0.0234 | 0.0278 |

_window = 60 seconds; metric = events per window_
<!-- END: bgl-point -->


### How to read these numbers

- **Method:** parse 4.75M BGL lines, bin by 1-minute window, label
  each window anomalous if it contains at least one alert line (~6.6%
  of windows are anomalous). Feed the per-window event-rate stream to
  each detector in time order.
- **Universe:** 27,666 windows.

### What these numbers actually tell us

Point-detector F1 on this signal is intentionally low — that's the
honest finding. BGL alerts don't reliably correlate with rate spikes;
they're individual anomalous lines mixed into otherwise-normal
traffic. This is exactly the result that justifies pairing BGL with
point detectors and HDFS with a sequence detector — different anomaly
models need different detection methods. The interview narrative
writes itself.

If we wanted higher BGL F1 we'd score *features* of each window
(alert density, message-length variance, novelty of message templates)
instead of pure event count. That work falls outside v1 scope.

```bash
python scripts/evaluate_bgl_point.py
```

---

## Test suite

| Suite | Count | Time |
|---|---|---|
| `tests/unit/` | 188 | ~12s |
| `tests/property/` | 6 | ~3s |
| `tests/integration/` | 19 (API + Postgres) | API runs always; Postgres skipped without `POSTGRES_DSN` |

```bash
pytest tests/                              # unit + property + API
POSTGRES_DSN=... pytest tests/integration  # against docker-compose
```

Coverage on production code (excluding the Postgres repo, which is
only exercised when the integration suite runs): **~87%**.

## Reproducing everything

```bash
git clone <this-repo>
cd pipelinex
python3.12 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
bash scripts/download_datasets.sh
pytest tests/
python scripts/benchmark_throughput.py --all
python scripts/train_sequence_model.py
python scripts/evaluate_hdfs_sequence.py
python scripts/evaluate_bgl_point.py
```

End-to-end on a laptop: ~15 min including dataset download.
