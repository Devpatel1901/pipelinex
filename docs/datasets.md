# Datasets

PipelineX uses two real-world labeled datasets and a synthetic generator.
None of the heavy data is committed to the repo — `bash
scripts/download_datasets.sh` fetches it from Zenodo on demand.

## HDFS_v1

- **Source:** https://zenodo.org/records/8196385/files/HDFS_v1.zip
- **License:** CC BY 4.0 (loghub) / source: Wei Xu et al. (Berkeley, 2009).
- **Size:** 1.47 GB compressed, 11.17M log lines.
- **Anomaly model:** per-block-trace. Each line carries a `blk_<id>`
  reference; `anomaly_label.csv` labels each block id Normal vs Anomaly.
- **Stats:** 575,061 labeled blocks, 16,838 (~2.93%) anomalous.
- **Used for:** sequence-detector training and evaluation
  (`scripts/train_sequence_model.py`, `scripts/evaluate_hdfs_sequence.py`).

The anomaly classes documented by the original researchers include
"writes failed", "node decommissioned", "redundant blocks", etc. We
treat the labels as a black-box ground truth — a panel can ask "what
classes of HDFS anomaly are these?" and we point at the source paper.

## BGL

- **Source:** https://zenodo.org/records/8196385/files/BGL.zip
- **License:** CC BY 4.0 (loghub) / source: Lawrence Livermore Nat. Lab.
- **Size:** 709 MB compressed, 4.75M log lines from a Blue Gene/L
  supercomputer.
- **Anomaly model:** per-line. Each line's first whitespace-delimited
  field is the label: `-` for normal, otherwise an alert category code
  (`KERNDTLB`, `APPSEV`, `KERNSTOR`, `APPCHILD`, …).
- **Stats:** ~7% of lines are alerts.
- **Used for:** point-detector evaluation
  (`scripts/evaluate_bgl_point.py`); we bin lines into 1-minute windows
  and label each window anomalous if it contains at least one alert
  line.

## Synthetic

- **Source:** generated locally by `scripts/generate_synthetic_logs.py`.
- **Format:** HDFS by default (so the same parser code path is
  exercised), with `--format=bgl` available.
- **Used for:** throughput benchmarks (`scripts/benchmark_throughput.py`).
- **Knobs:** `--rate`, `--total`, `--anomaly-rate`, `--blocks`,
  `--seed`. Reproducible.

## Why two real datasets, not one

These two datasets test **opposite anomaly models** with the same
pipeline architecture:

- HDFS exercises sessionization + sequence detection.
- BGL exercises point detection over a numeric stream.

A single dataset wouldn't prove the architecture handles both. Two
datasets force the abstractions to actually be plug-points. Adding a
third dataset (e.g., Hadoop, OpenStack) is a follow-on exercise that
should require zero core code change — the project's extensibility
claim made literal.

## Why not NAB

The original PipelineX plan used the Numenta Anomaly Benchmark, a
univariate time-series corpus. NAB is excellent at what it is, but it
is not log data, and PipelineX is pitched as a log-analytics engine.
The framing tension was unproductive, so we dropped NAB and adopted
HDFS_v1 + BGL — both genuine log corpora with published F1 baselines.

## Storage layout (after `download_datasets.sh`)

```
data/
├── HDFS/
│   ├── HDFS.log                                   1.5 GB (gitignored)
│   ├── README.md
│   └── preprocessed/
│       ├── anomaly_label.csv                     (gitignored)
│       ├── HDFS.log_templates.csv
│       ├── Event_occurrence_matrix.csv
│       ├── Event_traces.csv
│       └── HDFS.npz
├── HDFS_v1_sample/
│   ├── HDFS_2k.log                               2,000 lines (committed)
│   ├── HDFS_2k.log_structured.csv                committed
│   └── HDFS_2k.log_templates.csv                 committed
├── BGL/
│   ├── BGL.log                                   709 MB (gitignored)
│   └── README.md
└── samples/
    ├── BGL_sample_100.log                        100 lines (committed)
    └── HDFS_sample_100.log                       100 lines (committed)
```

The 2k and 100-line samples are kept in-tree so unit and oracle tests
work in CI without downloading the full corpora.
