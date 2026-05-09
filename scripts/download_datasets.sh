#!/usr/bin/env bash
# Download HDFS_v1 + BGL datasets from Zenodo.
# Both are loghub datasets used for anomaly-detection benchmarks.
#
# Usage:  bash scripts/download_datasets.sh
# Output: data/HDFS/HDFS.log, data/HDFS/anomaly_label.csv, data/BGL/BGL.log

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_DIR="$ROOT/data"
HDFS_DIR="$DATA_DIR/HDFS"
BGL_DIR="$DATA_DIR/BGL"

HDFS_URL="https://zenodo.org/records/8196385/files/HDFS_v1.zip"
BGL_URL="https://zenodo.org/records/8196385/files/BGL.zip"

# ----- helpers -----
say() { printf "\033[1;34m[download]\033[0m %s\n" "$*"; }
warn() { printf "\033[1;33m[warn]\033[0m %s\n" "$*"; }
die() { printf "\033[1;31m[error]\033[0m %s\n" "$*" >&2; exit 1; }

require() {
    command -v "$1" >/dev/null 2>&1 || die "missing required tool: $1"
}

require curl
require unzip

mkdir -p "$HDFS_DIR" "$BGL_DIR"

# ----- HDFS_v1 -----
HDFS_ZIP="$HDFS_DIR/HDFS_v1.zip"
if [[ -f "$HDFS_DIR/HDFS.log" && -f "$HDFS_DIR/anomaly_label.csv" ]]; then
    say "HDFS_v1 already present, skipping download"
else
    if [[ ! -f "$HDFS_ZIP" ]]; then
        say "downloading HDFS_v1.zip (~1.47 GB)"
        curl -L --fail --progress-bar -o "$HDFS_ZIP" "$HDFS_URL"
    else
        say "HDFS_v1.zip present, skipping download"
    fi

    say "extracting HDFS_v1.zip"
    unzip -o -q "$HDFS_ZIP" -d "$HDFS_DIR"

    # The zip extracts to HDFS_v1/ subfolder; flatten the layout.
    if [[ -d "$HDFS_DIR/HDFS_v1" ]]; then
        find "$HDFS_DIR/HDFS_v1" -type f -exec mv -f {} "$HDFS_DIR/" \;
        rm -rf "$HDFS_DIR/HDFS_v1"
    fi
fi

# ----- BGL -----
BGL_ZIP="$BGL_DIR/BGL.zip"
if [[ -f "$BGL_DIR/BGL.log" ]]; then
    say "BGL already present, skipping download"
else
    if [[ ! -f "$BGL_ZIP" ]]; then
        say "downloading BGL.zip (~709 MB)"
        curl -L --fail --progress-bar -o "$BGL_ZIP" "$BGL_URL"
    else
        say "BGL.zip present, skipping download"
    fi

    say "extracting BGL.zip"
    unzip -o -q "$BGL_ZIP" -d "$BGL_DIR"

    if [[ -d "$BGL_DIR/BGL" ]]; then
        find "$BGL_DIR/BGL" -type f -exec mv -f {} "$BGL_DIR/" \;
        rm -rf "$BGL_DIR/BGL"
    fi
fi

# ----- HDFS 2k reference sample (used by oracle test) -----
SAMPLE_DIR="$DATA_DIR/HDFS_v1_sample"
mkdir -p "$SAMPLE_DIR"
LOGHUB_RAW="https://raw.githubusercontent.com/logpai/loghub/master/HDFS"
for f in HDFS_2k.log HDFS_2k.log_structured.csv HDFS_2k.log_templates.csv; do
    if [[ ! -f "$SAMPLE_DIR/$f" ]]; then
        say "fetching reference sample $f"
        curl -sL --fail -o "$SAMPLE_DIR/$f" "$LOGHUB_RAW/$f"
    fi
done

# ----- summary -----
say "datasets ready"
ls -lh "$HDFS_DIR" "$BGL_DIR" | grep -v '^total'

# ----- create commit-safe samples -----
say "creating 100-line samples for tests"
if [[ -f "$HDFS_DIR/HDFS.log" ]]; then
    head -100 "$HDFS_DIR/HDFS.log" > "$DATA_DIR/samples/HDFS_sample_100.log"
fi
if [[ -f "$BGL_DIR/BGL.log" ]]; then
    head -100 "$BGL_DIR/BGL.log" > "$DATA_DIR/samples/BGL_sample_100.log"
fi

say "done"
