#!/usr/bin/env bash
# One run of the S2 REST workload (crates/s2-lab s2bench) against one system, on one machine.
# Every server runs in a container on network s2net; the driver runs on the host. Server CPU is
# the containers' cgroup time over the measured window (cgroup v1 cpuacct), memory their peak
# RSS (memory.stat total_rss is sampled each second), disk what their data directories hold.
#
#   bench/s2/run.sh <system> [s2bench args...]
# systems: s2lite-mem s2lite-disk s2lite-silo s2lite-silo5 (5 ms SlateDB flush) precog1 precog3 gw-s3 gw-none
# Needs: target/release/precog-node, crates/s2-lab/target/release/{gateway,s2bench}, images
# ghcr.io/s2-streamstore/s2 and docker.io/pgsty/silo. Prints one JSON line.
set -euo pipefail
system=$1; shift
WARMUP=${WARMUP:-5} DURATION=${DURATION:-20}
here=$(cd "$(dirname "$0")" && pwd); root=$(cd "$here/../.." && pwd)
lab=$root/crates/s2-lab/target/release
IMG_S2=${IMG_S2:-ghcr.io/s2-streamstore/s2:latest} IMG_SILO=${IMG_SILO:-docker.io/pgsty/silo:latest}
NET=s2net; ip() { echo "172.31.79.$1"; }
data=${DATA:-/home/user/ext/run}; work=$(mktemp -d)
ctrs=(); silo=0
down() { docker rm -f -v s2b-silo s2b-lite s2b-gw s2b-n0 s2b-n1 s2b-n2 >/dev/null 2>&1 || true; }
trap '[ -n "${KEEP:-}" ] || down; rm -rf "$work"' EXIT
docker network inspect $NET >/dev/null 2>&1 || docker network create --subnet 172.31.79.0/24 $NET >/dev/null
down; sudo_rm() { docker run --rm -v "$data:/d" alpine:3.20 sh -c 'rm -rf /d/*' >/dev/null; }
mkdir -p "$data"; sudo_rm

start_silo() {
  mkdir -p "$data/silo"
  docker run -d --name s2b-silo --network $NET --ip "$(ip 5)" -e MINIO_ROOT_USER=silo -e MINIO_ROOT_PASSWORD=silosilo123 \
    -v "$data/silo:/data" "$IMG_SILO" server /data >/dev/null
  for _ in $(seq 60); do curl -sf "http://$(ip 5):9000/minio/health/ready" && break; sleep 0.5; done
  python3 - "$(ip 5)" <<'PY'
import boto3, sys
s = boto3.client('s3', endpoint_url=f'http://{sys.argv[1]}:9000', aws_access_key_id='silo',
                 aws_secret_access_key='silosilo123', region_name='us-east-1')
for b in ['s2lite', 'precogwal']: s.create_bucket(Bucket=b)
PY
  silo=1
}
precog_image() {
  cp "$root/target/release/precog-node" "$lab/gateway" "$work/"
  printf 'FROM debian:trixie-slim\nCOPY precog-node gateway /usr/local/bin/\n' > "$work/Dockerfile"
  docker build -q -t precog-s2 "$work" >/dev/null
}
start_nodes() { # <n>
  local n=$1; python3 - "$work/cluster.json" "$n" <<'PY'
import json, sys
n = int(sys.argv[2])
peers = [{"id": i, "domain": f"d{i}", "addr": f"tcp://172.31.79.{20+i}:7000"} for i in range(n)]
json.dump({"id": 0, "domain": "", "data_dir": "/var/lib/precog", "token": "in-cluster", "listen_tcp": "0.0.0.0:7000",
           "peers": peers, "target_copies": n, "segment_size": 268435456, "dedup_window_ms": None,
           "retention_interval_ms": 1000, "flush_delay_ms": 0}, open(sys.argv[1], "w"))
PY
  for i in $(seq 0 $((n - 1))); do
    mkdir -p "$data/n$i"
    docker run -d --name "s2b-n$i" --hostname "node-$i" --network $NET --ip "$(ip $((20 + i)))" --user root \
      -v "$work/cluster.json:/etc/precog/cluster.json:ro" -v "$data/n$i:/var/lib/precog" --entrypoint precog-node precog-s2 \
      serve --config /etc/precog/cluster.json --member-from-hostname >/dev/null
    ctrs+=("s2b-n$i")
  done
  sleep 2
}
start_gw() { # gateway args...
  docker run -d --name s2b-gw --network $NET --ip "$(ip 10)" -e AWS_ACCESS_KEY_ID=silo -e AWS_SECRET_ACCESS_KEY=silosilo123 \
    --entrypoint gateway precog-s2 --listen 0.0.0.0:8080 "$@" >/dev/null
  ctrs+=(s2b-gw)
}
start_lite() { # s2 lite args...
  docker run -d --name s2b-lite --network $NET --ip "$(ip 10)" --user root -e AWS_ACCESS_KEY_ID=silo \
    -e AWS_SECRET_ACCESS_KEY=silosilo123 -e AWS_ENDPOINT_URL_S3="http://$(ip 5):9000" -e AWS_REGION=us-east-1 ${SL8_FLUSH_INTERVAL:+-e SL8_FLUSH_INTERVAL=$SL8_FLUSH_INTERVAL} \
    -v "$data/lite:/data" "$IMG_S2" lite --port 8080 "$@" >/dev/null
  ctrs+=(s2b-lite)
}

case $system in
  s2lite-mem) start_lite ;;
  s2lite-disk) mkdir -p "$data/lite"; start_lite --local-root /data ;;
  s2lite-silo) start_silo; start_lite --bucket s2lite --path bench ;;
  s2lite-silo5) start_silo; SL8_FLUSH_INTERVAL=5ms start_lite --bucket s2lite --path bench ;;
  precog1) precog_image; start_nodes 1; start_gw --sink mesh --nodes "tcp://$(ip 20):7000" ;;
  precog3) precog_image; start_nodes 3; start_gw --sink mesh --nodes "tcp://$(ip 20):7000,tcp://$(ip 21):7000,tcp://$(ip 22):7000" ;;
  gw-s3) precog_image; start_silo; start_gw --sink s3 --s3-endpoint "http://$(ip 5):9000" --bucket precogwal --flush-ms "${FLUSH_MS:-0}" ;;
  gw-none) precog_image; start_gw --sink none ;;
  *) echo "unknown system $system" >&2; exit 1 ;;
esac
ep="http://$(ip 10):8080"
for _ in $(seq 60); do curl -sf "$ep/health" -o /dev/null && break; sleep 0.5; done

cg() { docker inspect -f '{{.Id}}' "$1"; }
cpu_ns() { local t=0; for c in "$@"; do t=$((t + $(cat /sys/fs/cgroup/cpuacct/docker/$(cg $c)/cpuacct.usage))); done; echo $t; }
rss() { local t=0; for c in "$@"; do t=$((t + $(awk '/^total_rss /{print $2}' /sys/fs/cgroup/memory/docker/$(cg $c)/memory.stat))); done; echo $t; }
peak=0
"$lab/s2bench" --endpoint "$ep" --duration "$DURATION" --warmup "$WARMUP" "$@" > "$work/out.json" &
pid=$!
sleep "$WARMUP"; sleep 0.5
c0=$(cpu_ns "${ctrs[@]}"); s0=0; [ $silo = 1 ] && s0=$(cpu_ns s2b-silo)
for _ in $(seq "$DURATION"); do r=$(rss "${ctrs[@]}"); [ "$r" -gt "$peak" ] && peak=$r; sleep 1; done
c1=$(cpu_ns "${ctrs[@]}"); s1=0; [ $silo = 1 ] && s1=$(cpu_ns s2b-silo)
wait $pid
disk=$(docker run --rm -v "$data:/d" alpine:3.20 du -sb /d | cut -f1)
python3 - "$c0" "$c1" "$s0" "$s1" "$DURATION" "$system" "$peak" "$disk" "$work/out.json" <<'PY'
import json, sys
c0, c1, s0, s1, secs, system, peak, disk, f = sys.argv[1:]
d = json.load(open(f))
recs = d["records_per_s"] * float(secs)
d.update(system=system, server_cpu_cores=round((int(c1) - int(c0)) / 1e9 / float(secs), 2),
         server_cpu_us_per_record=round((int(c1) - int(c0)) / 1e3 / max(recs, 1), 2),
         objstore_cpu_us_per_record=round((int(s1) - int(s0)) / 1e3 / max(recs, 1), 2),
         server_peak_rss_mib=round(int(peak) / 2**20, 1), disk_mib=round(int(disk) / 2**20, 1))
print(json.dumps(d))
PY
