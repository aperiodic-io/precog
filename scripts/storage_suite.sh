#!/usr/bin/env bash
# Storage-engine sweep: every backend x batch size x producer count, REPS repetitions.
#   REPS=2 SECS=8 scripts/storage_suite.sh   -> results/storage.jsonl
set -uo pipefail
cd "$(dirname "$0")/.."
REPS=${REPS:-2}; SECS=${SECS:-8}; OUT=${OUT:-results/storage.jsonl}
BACKENDS=${BACKENDS:-"raw-append raw-prealloc raw-falloc raw-odirect raw-uring raft-engine rocksdb rocksdb-blob fjall fjall-kvsep redb lmdb sqlite"}
for rep in $(seq 1 "$REPS"); do
  for size in 27000 2700; do
    for p in 1 4 16; do
      for b in $BACKENDS; do
        timeout 600 target/release/storage-bench --backend "$b" --producers "$p" --size "$size" \
          --secs "$SECS" --gets 20000 --label "rep$rep" --json "$OUT" 2>&1 | tail -1 | cut -c1-200
      done
    done
  done
done
