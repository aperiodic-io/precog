#!/usr/bin/env bash
# The S2 comparison: every system, both workloads, REPS rounds with the systems alternating
# inside each round. Appends JSON lines to $OUT (default results/s2.jsonl).
set -uo pipefail
here=$(cd "$(dirname "$0")" && pwd); OUT=${OUT:-$here/../../results/s2.jsonl}
SYSTEMS=${SYSTEMS:-"s2lite-mem s2lite-disk s2lite-silo s2lite-silo5 precog1 precog3 gw-s3 gw-none"}
for rep in $(seq "${REPS:-3}"); do
  for s in $SYSTEMS; do
    # A: closed loop, 4 streams x 4 writers, 100 x 256 B per append, one SSE reader per stream
    "$here/run.sh" "$s" --label "A-r$rep" --streams 4 --writers 4 --batch 100 --size 256 --readers 4 >> "$OUT" || echo "A $s failed" >&2
    # B: fixed 10k records/s, 4 streams x 2 writers, 10 x 256 B per append (latency at a set rate)
    "$here/run.sh" "$s" --label "B-r$rep" --streams 4 --writers 2 --batch 10 --size 256 --readers 4 --rate 10000 >> "$OUT" || echo "B $s failed" >&2
  done
done
