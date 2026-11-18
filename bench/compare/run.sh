#!/usr/bin/env bash
# Precog against the Redpanda dedup fork on one machine, same shape: 3 brokers at 1 CPU / 4000Mi
# each (live-ingestion's raw brokers), 3 copies acknowledged at 2, a 5-minute dedup window, each
# load generator in its own 1-CPU container, live-ingestion's loadgen workload (8 producers x 3
# symbols, ~230-byte JSON trades, per-event dedup ids). Broker CPU comes from the containers'
# cgroup counters (cpuacct.usage) over exactly the measured window.
#
#   bench/compare/run.sh <label> <system: precog|redpanda> <dup-rate> <in-flight> [rate]
# Needs: target/release/{precog-node,examples/loadgen} (cargo build --release -p precog-mesh
# --bin precog-node -p precog-client --example loadgen), the fork image, and the franz-go loadgen
# binary at $FRANZ_LOADGEN. Prints one JSON line.
set -euo pipefail
label=$1 system=$2 dup=$3 inflight=$4 rate=${5:-0}
WARMUP=${WARMUP:-5} DURATION=${DURATION:-30}
NET=precog-cmp; SUBNET=172.31.78.0/24; ip() { echo "172.31.78.$((10 + $1))"; }
IMAGE_RP=${IMAGE_RP:-ghcr.io/dream-faster/redpanda:71f80ccdf79917925ec1e3fb4e958017480a65fc}
here=$(cd "$(dirname "$0")" && pwd); root=$(cd "$here/../.." && pwd)
work=$(mktemp -d)

down() { for i in 0 1 2; do docker rm -f "cmp-$i" >/dev/null 2>&1 || true; done; }
cpu_ns() { local t=0; for i in 0 1 2; do id=$(docker inspect -f '{{.Id}}' "cmp-$i"); t=$((t + $(cat /sys/fs/cgroup/cpuacct/docker/$id/cpuacct.usage))); done; echo $t; }
trap 'down; rm -rf "$work"' EXIT
docker network inspect "$NET" >/dev/null 2>&1 || docker network create --subnet "$SUBNET" "$NET" >/dev/null
down

if [ "$system" = precog ]; then
  cp "$root/target/release/precog-node" "$root/target/release/examples/loadgen" "$work/"
  printf 'FROM debian:trixie-slim\nCOPY precog-node loadgen /usr/local/bin/\n' > "$work/Dockerfile"
  docker build -q -t precog-bench "$work" >/dev/null
  python3 - "$work/cluster.json" <<'PY'
import json, sys
peers = [{"id": i, "domain": f"d{i}", "addr": f"tcp://172.31.78.{10+i}:7000"} for i in range(3)]
json.dump({"id": 0, "domain": "", "data_dir": "/var/lib/precog", "token": "in-cluster",
           "listen_tcp": "0.0.0.0:7000", "peers": peers, "target_copies": 3,
           "segment_size": 268435456, "dedup_window_ms": 300000,
           "retention_bytes": 1252698794, "retention_interval_ms": 1000,
           "repair_interval_ms": int(__import__("os").environ.get("REPAIR_MS", "1000"))}, open(sys.argv[1], "w"))
PY
  for i in 0 1 2; do
    docker run -d --name "cmp-$i" --hostname "cmp-$i" --network "$NET" --ip "$(ip $i)" --cpus 1 --memory 4000m \
      -v "$work/cluster.json:/etc/precog/cluster.json:ro" --entrypoint precog-node precog-bench \
      serve --config /etc/precog/cluster.json --member-from-hostname >/dev/null
  done
  sleep 3
  nodes="tcp://$(ip 0):7000,tcp://$(ip 1):7000,tcp://$(ip 2):7000"
  gen=(docker run --rm --network "$NET" --cpus 1 --entrypoint loadgen precog-bench --nodes "$nodes"
       --duration "$DURATION" --warmup "$WARMUP" --producers 8 --symbols 3 --dup-rate "$dup" --in-flight "$inflight"
       --rate "$rate" --batch "${BATCH:-50}" --label "$label")
else
  seeds="$(ip 0):33145,$(ip 1):33145,$(ip 2):33145"
  for i in 0 1 2; do
    docker run -d --name "cmp-$i" --network "$NET" --ip "$(ip $i)" --cpus 1 --memory 4000m --user root "$IMAGE_RP" \
      redpanda start --smp=1 --memory=3200M --reserve-memory=0M --overprovisioned --node-id="$i" --check=false \
      --kafka-addr="internal://0.0.0.0:9092" --advertise-kafka-addr="internal://$(ip $i):9092" \
      --rpc-addr="$(ip $i):33145" --advertise-rpc-addr="$(ip $i):33145" --seeds="$seeds" >/dev/null
  done
  RPK="docker exec cmp-0 /opt/redpanda/bin/rpk"
  for _ in $(seq 1 90); do $RPK cluster health 2>/dev/null | grep -q "Healthy:.*true" && break; sleep 2; done
  $RPK cluster config set auto_create_topics_enabled false >/dev/null
  $RPK cluster config set write_caching_default true >/dev/null  # as live-ingestion runs it: acks before fsync
  $RPK topic create raw.bench.trades -p 3 -r 3 -c retention.bytes=1252698794 -c cleanup.policy=delete \
    -c redpanda.dedup.window.ms=300000 -c redpanda.dedup.key.header=dedup-id >/dev/null
  cp "${FRANZ_LOADGEN:?}" "$work/loadgen"
  [ "$rate" = 0 ] || { echo "the franz-go loadgen has no --rate" >&2; exit 1; }
  gen=(docker run --rm --network "$NET" --cpus 1 -v "$work:/w" alpine:3.20 /w/loadgen
       -brokers="$(ip 0):9092,$(ip 1):9092,$(ip 2):9092" -topic=raw.bench.trades -label="$label"
       -duration="${DURATION}s" -warmup="${WARMUP}s" -producers=8 -symbols-per-producer=3 -dup-rate="$dup"
       -in-flight="$inflight" -out=/w/out.json)
fi

"${gen[@]}" > "$work/gen.out" 2>"$work/gen.err" &
pid=$!
sleep "$WARMUP"; c0=$(cpu_ns); sleep "$DURATION"; c1=$(cpu_ns)
wait $pid
out=$([ "$system" = precog ] && tail -1 "$work/gen.out" || cat "$work/out.json")
python3 - "$c0" "$c1" "$DURATION" "$system" <<PY
import json, sys
d = json.loads('''$out''')
c0, c1, secs, system = int(sys.argv[1]), int(sys.argv[2]), float(sys.argv[3]), sys.argv[4]
rps = d["throughput_records_per_sec"]
d.update(system=system, broker_cpu_cores=(c1 - c0) / 1e9 / secs,
         broker_cpu_us_per_record=(c1 - c0) / 1e3 / (rps * secs) if rps else None)
print(json.dumps(d))
PY
