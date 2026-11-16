#!/usr/bin/env bash
# Start/stop native Apache Iggy servers on loopback.
#   scripts/iggy.sh single  [shards]     one node, client TCP 127.0.0.1:8090
#   scripts/iggy.sh cluster [shards]     three-node VSR cluster, TCP 8090-8092
#   scripts/iggy.sh kill <node>          SIGKILL one cluster node
#   scripts/iggy.sh stop
# IGGY_BIN defaults to run/bin/iggy-server (extracted from apache/iggy:latest).
set -euo pipefail
cd "$(dirname "$0")/.."
BIN=${IGGY_BIN:-run/bin/iggy-server}
ROOT=run/iggy
SHARDS=${2:-2}

common_env() {
  export IGGY_ROOT_USERNAME=iggy IGGY_ROOT_PASSWORD=iggy
  export IGGY_SHARDING_CPU_ALLOCATION=$SHARDS IGGY_SHARDING_PIN_CORES=false
  export IGGY_HTTP_ENABLED=false IGGY_QUIC_ENABLED=false IGGY_WEBSOCKET_ENABLED=false
  export IGGY_TELEMETRY_ENABLED=false RUST_LOG=${RUST_LOG:-warn}
}

case "${1:-}" in
single)
  common_env
  rm -rf $ROOT && mkdir -p $ROOT
  IGGY_PATH=$PWD/$ROOT/n0 IGGY_TCP_ADDRESS=127.0.0.1:8090 nohup "$BIN" > $ROOT/n0.log 2>&1 &
  echo $! > $ROOT/n0.pid
  ;;
cluster)
  common_env
  rm -rf $ROOT && mkdir -p $ROOT
  export IGGY_CLUSTER_ENABLED=true IGGY_CLUSTER_NAME=bench IGGY_CLUSTER_AUTH_ENABLED=true
  export IGGY_CLUSTER_AUTH_SHARED_SECRET=0123456789abcdef0123456789abcdef0123456789abcdef
  # Fastest failure detection Iggy accepts (minimum 2s, >= 4x commit broadcast).
  export IGGY_CLUSTER_HEARTBEAT_TIMEOUT=${IGGY_HEARTBEAT:-2s} IGGY_CLUSTER_COMMIT_BROADCAST_INTERVAL=500ms
  for i in 0 1 2; do
    export IGGY_CLUSTER_NODES_${i}_NAME=n$i IGGY_CLUSTER_NODES_${i}_IP=127.0.0.1 IGGY_CLUSTER_NODES_${i}_REPLICA_ID=$i
    export IGGY_CLUSTER_NODES_${i}_ADVERTISED_ADDRESS=127.0.0.1
    export IGGY_CLUSTER_NODES_${i}_PORTS_TCP=$((8090+i)) IGGY_CLUSTER_NODES_${i}_PORTS_QUIC=$((8080+i))
    export IGGY_CLUSTER_NODES_${i}_PORTS_HTTP=$((3000+i)) IGGY_CLUSTER_NODES_${i}_PORTS_WEBSOCKET=$((8070+i))
    export IGGY_CLUSTER_NODES_${i}_PORTS_TCP_REPLICA=$((9090+i))
  done
  for i in 0 1 2; do
    IGGY_PATH=$PWD/$ROOT/n$i IGGY_TCP_ADDRESS=127.0.0.1:$((8090+i)) \
      nohup "$BIN" --replica-id $i > $ROOT/n$i.log 2>&1 &
    echo $! > $ROOT/n$i.pid
  done
  ;;
kill)
  kill -9 "$(cat $ROOT/n$2.pid)"
  ;;
stop)
  pids=()
  for f in $ROOT/*.pid; do [ -f "$f" ] && pids+=("$(cat "$f")"); done
  for p in "${pids[@]}"; do kill -CONT "$p" 2>/dev/null; kill -9 "$p" 2>/dev/null || true; done
  for p in "${pids[@]}"; do while kill -0 "$p" 2>/dev/null; do sleep 0.1; done; done
  rm -f $ROOT/*.pid
  ;;
*) echo "usage: $0 single|cluster [shards] | kill <n> | stop"; exit 1 ;;
esac
