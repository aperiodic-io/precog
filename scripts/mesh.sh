#!/usr/bin/env bash
# Start/stop precog mesh nodes on loopback.
#   scripts/mesh.sh up <nodes> <tcp|iroh> [threads]   e.g. scripts/mesh.sh up 3 tcp 2
#   scripts/mesh.sh kill <node> | restart <node> | stop
# Env: MESH_NO_SYNC=1 disables fdatasync; MESH_RPC_MS peer RPC timeout.
set -euo pipefail
cd "$(dirname "$0")/.."
BIN=target/release/precog-node
ROOT=run/mesh
case "${1:-}" in
up)
  N=${2:-3}; T=${3:-tcp}; TH=${4:-2}
  rm -rf $ROOT
  extra=()
  [ "${MESH_NO_SYNC:-0}" = 1 ] && extra+=(--no-sync)
  $BIN init --nodes "$N" --dir $ROOT --transport "$T" --base-port 7000 --rpc-timeout-ms "${MESH_RPC_MS:-1000}" "${extra[@]}" >/dev/null
  echo "$TH" > $ROOT/threads
  for i in $(seq 0 $((N-1))); do
    RUST_LOG=${RUST_LOG:-warn} nohup $BIN serve --config $ROOT/node-$i.json --threads "$TH" > $ROOT/n$i.log 2>&1 &
    echo $! > $ROOT/n$i.pid
  done
  sleep 1
  ;;
kill)
  kill -9 "$(cat $ROOT/n$2.pid)"
  ;;
restart)
  RUST_LOG=${RUST_LOG:-warn} nohup $BIN serve --config $ROOT/node-$2.json --threads "$(cat $ROOT/threads)" >> $ROOT/n$2.log 2>&1 &
  echo $! > $ROOT/n$2.pid
  ;;
stop)
  pids=()
  for f in $ROOT/*.pid; do [ -f "$f" ] && pids+=("$(cat "$f")"); done
  for p in "${pids[@]}"; do kill -CONT "$p" 2>/dev/null; kill -9 "$p" 2>/dev/null || true; done
  for p in "${pids[@]}"; do while kill -0 "$p" 2>/dev/null; do sleep 0.1; done; done
  rm -f $ROOT/*.pid
  ;;
*) echo "usage: $0 up <n> <tcp|iroh> [threads] | kill <n> | restart <n> | stop"; exit 1 ;;
esac
