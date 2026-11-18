#!/usr/bin/env bash
# A 3-broker Redpanda cluster the size of live-ingestion's redpanda-raw-internal brokers (1 CPU,
# 4000Mi each, RF 3), for comparing Precog against the dedup fork on one machine. Same shape as
# live-ingestion/benchmarks/redpanda-dedup/cluster.sh, but with static IPs: on some hosts
# Redpanda's resolver (c-ares) gets "No data" from Docker's DNS and the brokers never meet.
#
#   IMAGE=ghcr.io/dream-faster/redpanda:<sha> ./cluster.sh up
#   ./cluster.sh topic raw.bench.trades 300000   # topic with a 5-minute dedup window
#   ./cluster.sh down
set -euo pipefail
NET=precog-rp-bench
SUBNET=172.31.77.0/24
ip() { echo "172.31.77.$((10 + $1))"; }
RPK="docker exec rp-bench-0 /opt/redpanda/bin/rpk"

up() {
  docker network inspect "$NET" >/dev/null 2>&1 || docker network create --subnet "$SUBNET" "$NET" >/dev/null
  local seeds="$(ip 0):33145,$(ip 1):33145,$(ip 2):33145"
  for i in 0 1 2; do
    docker rm -f "rp-bench-$i" >/dev/null 2>&1 || true
    docker run -d --name "rp-bench-$i" --network "$NET" --ip "$(ip $i)" --cpus 1 --memory 4000m --user root \
      "${IMAGE:?set IMAGE}" redpanda start --smp=1 --memory=3200M --reserve-memory=0M --overprovisioned \
      --node-id="$i" --check=false --kafka-addr="internal://0.0.0.0:9092" \
      --advertise-kafka-addr="internal://$(ip $i):9092" --rpc-addr="$(ip $i):33145" \
      --advertise-rpc-addr="$(ip $i):33145" --seeds="$seeds" >/dev/null
  done
  for _ in $(seq 1 90); do
    $RPK cluster health 2>/dev/null | grep -q "Healthy:.*true" && { echo "healthy"; return; }
    sleep 2
  done
  echo "cluster never became healthy" >&2; exit 1
}

# raw-internal's cluster and topic config (live-ingestion configure.sh and internal/pipeline/topics.go)
topic() {
  local name="$1" window="${2:-}"
  $RPK cluster config set auto_create_topics_enabled false >/dev/null
  $RPK cluster config set log_compression_type producer >/dev/null
  $RPK cluster config set write_caching_default true >/dev/null
  $RPK topic delete "$name" >/dev/null 2>&1 || true
  local cfg=(-c retention.bytes=1252698794 -c cleanup.policy=delete -c compression.type=producer)
  [ -n "$window" ] && cfg+=(-c redpanda.dedup.window.ms="$window" -c redpanda.dedup.key.header=dedup-id)
  $RPK topic create "$name" -p 3 -r 3 "${cfg[@]}"
}

down() {
  for i in 0 1 2; do docker rm -f "rp-bench-$i" >/dev/null 2>&1 || true; done
  docker network rm "$NET" >/dev/null 2>&1 || true
}

brokers() { echo "$(ip 0):9092,$(ip 1):9092,$(ip 2):9092"; }

"$@"
