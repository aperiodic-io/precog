#!/usr/bin/env bash
# Million-stream footprint: create COUNT streams with one 64-byte record each, then report the
# server's RSS, its disk, and how long a restart takes until the server answers again (and, for
# Precog, until a read of the last stream returns its record).
#
#   bench/s2/streams.sh <s2lite-disk|precog1-native|precog1-gw> [COUNT=1000000]
set -euo pipefail
system=$1; COUNT=${2:-1000000}
here=$(cd "$(dirname "$0")" && pwd); root=$(cd "$here/../.." && pwd)
lab=$root/crates/s2-lab/target/release; data=${DATA:-/home/user/ext/run}; work=$(mktemp -d)
ip() { echo "172.31.79.$1"; }
down() { docker rm -f -v s2b-lite s2b-gw s2b-n0 >/dev/null 2>&1 || true; }
trap 'down; rm -rf "$work"' EXIT
down; docker run --rm -v "$data:/d" alpine:3.20 sh -c 'rm -rf /d/*' >/dev/null; mkdir -p "$data/lite" "$data/n0"
cg() { docker inspect -f '{{.Id}}' "$1"; }
rss() { awk '/^total_rss /{print $2}' /sys/fs/cgroup/memory/docker/$(cg $1)/memory.stat; }
wait_http() { for _ in $(seq 600); do curl -sf "$1" -o /dev/null && return; sleep 0.1; done; }
node() {
  python3 - "$work/cluster.json" <<'PY'
import json, sys
json.dump({"id": 0, "domain": "d0", "data_dir": "/var/lib/precog", "token": "in-cluster", "listen_tcp": "0.0.0.0:7000",
           "peers": [], "target_copies": 1, "segment_size": 268435456, "dedup_window_ms": None}, open(sys.argv[1], "w"))
PY
  docker run -d --name s2b-n0 --hostname node-0 --network s2net --ip "$(ip 20)" --user root \
    -v "$work/cluster.json:/etc/precog/cluster.json:ro" -v "$data/n0:/var/lib/precog" --entrypoint precog-node precog-s2 \
    serve --config /etc/precog/cluster.json >/dev/null
  for _ in $(seq 600); do (echo > /dev/tcp/$(ip 20)/7000) 2>/dev/null && return; sleep 0.1; done
}
case $system in
  s2lite-disk)
    start() { docker run -d --name s2b-lite --network s2net --ip "$(ip 10)" --user root -v "$data/lite:/data" \
      ghcr.io/s2-streamstore/s2:latest lite --port 8080 --local-root /data >/dev/null; wait_http "http://$(ip 10):8080/health"; }
    start; c=s2b-lite
    out=$("$lab/streams" --endpoint "http://$(ip 10):8080" --count "$COUNT" --label "$system")
    sleep 3; r=$(rss $c); docker stop -t 30 $c >/dev/null; docker rm $c >/dev/null
    t0=$(date +%s.%N); start
    curl -sf "http://$(ip 10):8080/v1/streams/m$(printf %08d $((COUNT - 1)))/records?seq_num=0" -H 's2-basin: many-basin' -o /dev/null
    t1=$(date +%s.%N); r2=$(rss $c) ;;
  precog1-native|precog1-gw)
    node; c=s2b-n0
    if [ "$system" = precog1-gw ]; then
      docker run -d --name s2b-gw --network s2net --ip "$(ip 10)" --entrypoint gateway precog-s2 --listen 0.0.0.0:8080 \
        --sink mesh --nodes "tcp://$(ip 20):7000" --copies 1 >/dev/null; wait_http "http://$(ip 10):8080/health"
      out=$("$lab/streams" --endpoint "http://$(ip 10):8080" --count "$COUNT" --label "$system")
      sleep 3; gw=$(rss s2b-gw)
    else
      out=$("$lab/streams" --nodes "tcp://$(ip 20):7000" --count "$COUNT" --label "$system")
      gw=0
    fi
    sleep 3; r=$(( $(rss $c) + gw )); docker stop -t 30 $c >/dev/null; docker rm $c >/dev/null
    t0=$(date +%s.%N); node; t1=$(date +%s.%N); r2=$(rss $c) ;;
esac
disk=$(docker run --rm -v "$data:/d" alpine:3.20 du -sb /d | cut -f1)
python3 -c "import json,sys; d=json.loads(sys.argv[1]); d.update(system='$system', rss_mib=round($r/2**20,1), rss_after_restart_mib=round($r2/2**20,1), restart_s=round($t1-$t0,2), disk_mib=round($disk/2**20,1), bytes_per_stream_rss=round($r/$COUNT)); print(json.dumps(d))" "$out"
