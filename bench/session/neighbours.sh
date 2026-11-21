#!/usr/bin/env bash
# The neighbour test (results/session-log.md, "Neighbours"): a small writer alone, beside two
# tenants appending large events back to back, and beside them with an append rate on their
# token. One JSON line per case.
# usage: bench/session/neighbours.sh [seconds=10] [rate_kib=4096]
set -euo pipefail
cd "$(dirname "$0")/../.."
secs="${1:-10}"
rate="${2:-4096}"
cargo build --release -p precog-mesh --example neighbours 2> /dev/null
run() {
  local d
  d=$(mktemp -d)
  target/release/examples/neighbours "$d" "$secs" "$@"
  rm -rf "$d"
}
run 0 0 0
for kib in 1024 4096; do
  run 2 "$kib" 0
  run 2 "$kib" "$rate"
done
