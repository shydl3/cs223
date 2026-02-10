#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$ROOT_DIR/build/cs223_txn"
OUT_DIR="$ROOT_DIR/build/exp"
mkdir -p "$OUT_DIR"

if [[ ! -x "$BIN" ]]; then
  echo "Binary not found: $BIN"
  echo "Run: cmake -S $ROOT_DIR -B $ROOT_DIR/build && cmake --build $ROOT_DIR/build -j"
  exit 1
fi

DURATION="${DURATION:-5}"
SEED="${SEED:-12345}"
MAX_RETRIES="${MAX_RETRIES:-16}"
BACKOFF_US="${BACKOFF_US:-100}"

run_sweep() {
  local workload_name="$1"
  local input_file="$2"
  local workload_file="$3"
  local hotset_size="$4"
  local fixed_threads="$5"
  local fixed_phot="$6"

  local csv_threads="$OUT_DIR/${workload_name}_throughput_vs_threads.csv"
  local csv_contention="$OUT_DIR/${workload_name}_throughput_vs_contention.csv"
  rm -f "$csv_threads" "$csv_contention"

  for cc in occ c2pl; do
    for th in 1 2 4 8 16; do
      "$BIN" \
        --input "$input_file" \
        --workload "$workload_file" \
        --workload_name "$workload_name" \
        --storage inmem \
        --cc "$cc" \
        --threads "$th" \
        --duration "$DURATION" \
        --p_hot "$fixed_phot" \
        --hotset_size "$hotset_size" \
        --seed "$SEED" \
        --max_retries "$MAX_RETRIES" \
        --backoff_us "$BACKOFF_US" \
        --csv "$csv_threads"
    done

    for p in 0.1 0.3 0.5 0.7 0.9; do
      "$BIN" \
        --input "$input_file" \
        --workload "$workload_file" \
        --workload_name "$workload_name" \
        --storage inmem \
        --cc "$cc" \
        --threads "$fixed_threads" \
        --duration "$DURATION" \
        --p_hot "$p" \
        --hotset_size "$hotset_size" \
        --seed "$SEED" \
        --max_retries "$MAX_RETRIES" \
        --backoff_us "$BACKOFF_US" \
        --csv "$csv_contention"
    done
  done
}

run_sweep "w1" "$ROOT_DIR/data/workload1/input1.txt" "$ROOT_DIR/data/workload1/workload1.txt" 20 8 0.8
run_sweep "w2" "$ROOT_DIR/data/workload2/input2.txt" "$ROOT_DIR/data/workload2/workload2.txt" 50 8 0.8

echo "Experiment CSV generated under: $OUT_DIR"
