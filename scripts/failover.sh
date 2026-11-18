#!/usr/bin/env bash
# Crash/hang one node of a 3-node cluster mid-run and record the ack timeline.
#   scripts/failover.sh <iggy|mesh> <kill|stop> [node]
#   kill = SIGKILL (peers see connection resets immediately)
#   stop = SIGSTOP (hung host / partition: sockets stay open, nothing answers;
#          detection only by timeouts). The node stays stopped until the end.
# Output: results/failover.jsonl (with 100 ms timeline)
set -uo pipefail
cd "$(dirname "$0")/.."
SYS=$1; MODE=$2; NODE=${3:-0}
DUR=${DUR:-20}; WARM=2; AT=${AT:-6}; P=${P:-6}
I3=iggy://iggy:iggy@127.0.0.1:8090,iggy://iggy:iggy@127.0.0.1:8091,iggy://iggy:iggy@127.0.0.1:8092
mkdir -p results
if [ "$SYS" = iggy ]; then
  scripts/iggy.sh cluster 1 >/dev/null; sleep 8; PIDF=run/iggy/n$NODE.pid
  args=(iggy --iggy "$I3" --replication 3 --durability persisted)
else
  scripts/mesh.sh up 3 1 >/dev/null; PIDF=run/mesh/n$NODE.pid
  args=(mesh --copies 2 --rpc-timeout-ms ${MESH_RPC_MS:-1000})
fi
target/release/precog-bench "${args[@]}" --label "failover/$SYS-$MODE-n$NODE" --producers $P \
  --duration $DUR --warmup $WARM --json results/failover.jsonl &
B=$!
sleep $((WARM + AT))
if [ "$MODE" = kill ]; then kill -9 "$(cat $PIDF)"; else kill -STOP "$(cat $PIDF)"; fi
echo "$(date +%T.%N) $MODE node $NODE at t=$((WARM+AT))s"
wait $B
if [ "$MODE" = stop ]; then kill -CONT "$(cat $PIDF)" 2>/dev/null; fi
if [ "$SYS" = mesh ] && [ "$MODE" = kill ]; then
  # Rejoin: restart the node and time anti-entropy catch-up.
  t0=$(date +%s.%N); scripts/mesh.sh restart $NODE; sleep 0.5
  want=$(target/release/precog-node status --cluster run/mesh/cluster.json --node 1 | python3 -c "import json,sys;print(json.load(sys.stdin)['events'])")
  while :; do
    have=$(target/release/precog-node status --cluster run/mesh/cluster.json --node $NODE 2>/dev/null | python3 -c "import json,sys;print(json.load(sys.stdin)['events'])" 2>/dev/null || echo 0)
    [ "$have" -ge "$want" ] && break; sleep 0.2
  done
  echo "rejoin: node $NODE caught up to $want events in $(python3 -c "print(round($(date +%s.%N)-$t0,2))")s (incl. WAL replay)"
fi
scripts/iggy.sh stop >/dev/null 2>&1; scripts/mesh.sh stop >/dev/null 2>&1
